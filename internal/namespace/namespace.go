/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |---- |     | |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2026 Microsoft Corporation. All rights reserved.
   Author : <blobfusedev@microsoft.com>

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE
*/

// Package namespace pins object identity across rename and delete operations so
// that open handles and cache entries follow the object instead of the path
// string they were opened with.
//
// Problem it solves: while a process holds a file open, another process can
// rename or delete the path. A cache that only tracks paths will then (a) write
// dirty handle data back to the old name, recreating a deleted object or
// overwriting the rename target, and (b) let a stale cache purge remove the
// cache files of an object that was recreated under the same name.
//
// The Registry offers three guarantees:
//
//   - Identity: each registered object has an immutable ObjectID. Handles are
//     attached to that ID. Rename commits rebind the ID to the new name and
//     update every attached handle atomically; delete commits mark the ID
//     deleted. Flush/release resolve the ID and therefore always write to the
//     object's current name (or nowhere, if the object was deleted).
//   - Naming generations: every name carries a monotonically increasing
//     generation bumped by create, rename and delete. Cache cleanup tagged
//     with the generation captured when the operation was prepared becomes a
//     no-op once a newer generation occupies the name, so stale rename/delete
//     work can never remove or tombstone a recreated object.
//   - Two-phase transactions: PrepareRename/PrepareDelete only snapshot state.
//     Remote mutation happens in between. Commit applies the local mapping,
//     Rollback leaves it untouched, and Decide* classify an uncertain remote
//     outcome from fresh src/dst probes so the caller can commit, finish a
//     partial move, roll back, or keep write-back fenced until identity is
//     re-verified.
//
// The registry is in-memory. It is rebuilt empty on every mount; open handles
// and pending operations do not survive a service restart, and local caches
// are revalidated from storage afterwards, so no stale operation can outlive a
// restart.
package namespace

import (
	"sync"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
)

// State of an object identity as seen by the local namespace.
type State uint8

const (
	// StateAlive: the object has a current name and write-back is allowed.
	StateAlive State = iota
	// StateDeleted: the object's name was removed (or overwritten by a
	// different object). Dirty data must never be written back.
	StateDeleted
	// StateAmbiguous: the remote rename/delete result could not be
	// determined. Write-back must re-probe the name and verify the pinned
	// identity before touching storage.
	StateAmbiguous
)

// TxnKind identifies the kind of prepared transaction.
type TxnKind uint8

const (
	TxnRename TxnKind = iota + 1
	TxnDelete
)

// Identity is the best-effort remote fingerprint of an object, captured before
// a rename/delete and used to recognise the same object again after an
// uncertain response. ETag is authoritative; Mtime/Size are the fallback for
// backends that did not return an ETag.
type Identity struct {
	ETag  string
	Mtime time.Time
	Size  int64
}

// SameIdentity reports whether two fingerprints describe the same object
// version. ETags win when both sides have one; otherwise identical modification
// time and size are accepted.
func SameIdentity(a, b Identity) bool {
	if a.ETag != "" && b.ETag != "" {
		return a.ETag == b.ETag
	}
	return !a.Mtime.IsZero() && !b.Mtime.IsZero() && a.Mtime.Equal(b.Mtime) && a.Size == b.Size
}

// IsZero reports whether no identity fingerprint was captured at all. This is
// common for files that were created locally and never flushed, or backends
// that do not return ETags.
func (id Identity) IsZero() bool {
	return id.ETag == "" && id.Mtime.IsZero() && id.Size == 0
}

// sameOrUnknown accepts a match when the pinned identity is known and equal,
// and also when it was never captured (best effort: nothing proves a
// difference).
func sameOrUnknown(current, pinned Identity) bool {
	if pinned.IsZero() {
		return true
	}
	return SameIdentity(current, pinned)
}

// differentProven reports a confirmed identity mismatch; an unknown pinned
// identity is never a proven mismatch.
func differentProven(current, pinned Identity) bool {
	if pinned.IsZero() {
		return false
	}
	return !SameIdentity(current, pinned)
}

// Txn is a prepared rename/delete transaction. It only holds snapshots: until
// Commit is called the published name/object mappings are unchanged, so every
// handle and cache entry still has a valid mapping and Rollback costs nothing.
type Txn struct {
	Kind TxnKind
	Src  string
	Dst  string // empty for delete

	// Generations captured at prepare time. Commit only updates a name record
	// whose generation still matches, which fences a name that was recreated
	// while the remote call was in flight (the per-path locks in the cache
	// components normally prevent this, the check is the second line of
	// defence).
	SrcGen uint64
	DstGen uint64

	// Object IDs bound to src/dst at prepare time (zero when there were no
	// registered open handles). SrcID is the object being moved/removed;
	// DstID is the object being overwritten by a rename, if any.
	SrcID handlemap.ObjectID
	DstID handlemap.ObjectID

	// Pinned identity of the source object and precondition identity of the
	// destination object, as known before the remote call.
	SrcIdentity Identity
	DstIdentity Identity
	SrcPresent  bool
	DstPresent  bool
}

// CommitPlan describes the local mapping changes a rename commit performed.
// The cache component uses the generations to tag cache cleanup and the handle
// lists to migrate local files and locks.
type CommitPlan struct {
	SrcGen uint64 // generation left on the source name (tombstone)
	DstGen uint64 // generation left on the destination name

	// A conflict means the name changed generation between prepare and commit.
	// The name record was left untouched; cache state for that name must be
	// invalidated rather than renamed/tombstoned.
	SrcConflict bool
	DstConflict bool

	// MovedHandles were open on the source object and now resolve at Dst.
	// ReplacedHandles were open on the destination object that the rename
	// overwrote; their object is deleted as far as they are concerned and
	// their write-back is suppressed (matching POSIX rename-over-file
	// semantics for pre-existing destination descriptors).
	MovedHandles    []*handlemap.Handle
	ReplacedHandles []*handlemap.Handle

	SrcID       handlemap.ObjectID
	ReplacedID  handlemap.ObjectID
	MovedCount  uint32
	OrphanCount uint32
}

// Binding is the resolved state of an object ID.
type Binding struct {
	ID    handlemap.ObjectID
	Path  string
	Gen   uint64
	State State
	Self  Identity
}

// WriteBackAllowed reports whether dirty data of a handle on this binding may
// be uploaded. Ambiguous bindings require an identity-verifying probe first.
func (b Binding) WriteBackAllowed() bool { return b.State == StateAlive }

type nameRec struct {
	id      handlemap.ObjectID // object currently bound to the name (0 if none)
	gen     uint64
	present bool // false = negative record: the name was confirmed absent
}

type objRec struct {
	id      handlemap.ObjectID
	path    string
	gen     uint64
	state   State
	self    Identity
	handles map[*handlemap.Handle]struct{}
}

// Registry keeps the name -> object bindings and naming generations for one
// mount. A cache component owns one instance; its zero value is not ready, use
// NewRegistry.
type Registry struct {
	mu    sync.Mutex
	next  handlemap.ObjectID
	names map[string]*nameRec
	objs  map[handlemap.ObjectID]*objRec
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		names: make(map[string]*nameRec),
		objs:  make(map[handlemap.ObjectID]*objRec),
	}
}

func (r *Registry) nameLocked(path string) *nameRec {
	rec := r.names[path]
	if rec == nil {
		rec = &nameRec{gen: 0}
		r.names[path] = rec
	}
	return rec
}

// Generation returns the current naming generation of a path (0 if the name was
// never seen). Cache cleanup compares the generation captured at prepare time
// against this value.
func (r *Registry) Generation(path string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec := r.names[path]; rec != nil {
		return rec.gen
	}
	return 0
}

// CheckGen reports whether a name is still at the generation captured by an
// earlier prepare. A zero captured generation means the path was never
// tracked, in which case cleanup keeps its legacy behaviour (true).
func (r *Registry) CheckGen(path string, gen uint64) bool {
	if gen == 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.names[path]
	return rec != nil && rec.gen == gen
}

// Present reports whether the name is currently known to hold a registered
// object.
func (r *Registry) Present(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.names[path]
	return rec != nil && rec.present
}

// HasBinding reports whether at least one tracked handle is currently bound to
// the object living at name. Cache layers use it to decide whether a rename or
// delete has open handles to carry along (identity-safe path) or can take the
// cheap legacy path.
func (r *Registry) HasBinding(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.names[path]
	if rec == nil || rec.id == 0 {
		return false
	}
	o := r.objs[rec.id]
	return o != nil && len(o.handles) > 0
}

// TouchCreate records that a brand new object now occupies path: the generation
// is bumped and any object previously bound to the name loses it (the name was
// overwritten, e.g. delete followed by recreate). The new object binds itself
// through Bind. Returns the new generation.
func (r *Registry) TouchCreate(path string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec := r.nameLocked(path)
	rec.gen++
	rec.present = true
	if rec.id != 0 {
		if old := r.objs[rec.id]; old != nil {
			// The name was recycled onto a new object. Detach the previous
			// object so its handles cannot write to the new occupant.
			old.state = StateDeleted
			old.path = path
		}
		rec.id = 0
	}
	return rec.gen
}

// Bind attaches a handle to the object currently registered at path. Handles
// opened repeatedly on the same name share one ObjectID for as long as the
// binding lives. identity is the remote fingerprint captured at open time
// (may be zero for newly created local-only files). Returns the bound ID and
// the current name generation.
func (r *Registry) Bind(h *handlemap.Handle, path string, identity Identity) (handlemap.ObjectID, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec := r.nameLocked(path)
	if !rec.present {
		// Re-opened after a delete recorded in this registry: revive with a
		// fresh generation so older tombstones/cleanup cannot shadow it.
		rec.gen++
		rec.present = true
	}
	if rec.id == 0 {
		// No object bound yet (including the brand-new TouchCreate case, which
		// already advanced the generation). Allocate the object under the
		// current generation.
		r.next++
		id := r.next
		o := &objRec{
			id:      id,
			path:    path,
			gen:     rec.gen,
			state:   StateAlive,
			self:    identity,
			handles: make(map[*handlemap.Handle]struct{}),
		}
		r.objs[id] = o
		rec.id = id
		o.handles[h] = struct{}{}
		h.Lock()
		h.BindIdentity(id, rec.gen)
		h.Unlock()
		return id, rec.gen
	}

	o := r.objs[rec.id]
	if o == nil {
		// Defensive: name record references a freed object. Recreate it.
		r.next++
		id := r.next
		o = &objRec{id: id, path: path, gen: rec.gen, state: StateAlive, self: identity,
			handles: make(map[*handlemap.Handle]struct{})}
		r.objs[id] = o
		rec.id = id
	}
	if identity.ETag != "" {
		o.self = identity
	}
	o.handles[h] = struct{}{}
	h.Lock()
	h.BindIdentity(o.id, rec.gen)
	h.Unlock()
	return o.id, rec.gen
}

// Detach removes a handle from its object. It must be called when the handle is
// released, after all write-back decisions for it are done.
func (r *Registry) Detach(h *handlemap.Handle) {
	id, _ := h.Identity()
	if id == handlemap.InvalidObjectID {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	o := r.objs[id]
	if o == nil {
		return
	}
	delete(o.handles, h)
	if len(o.handles) == 0 {
		delete(r.objs, id)
		if rec := r.names[o.path]; rec != nil && rec.id == id {
			rec.id = 0 // file may stay cached, but no open identity is bound
		}
	}
}

// PrepareRename snapshots the source and destination state for a rename. The
// returned Txn is committed or rolled back once the remote result is known.
// srcIdentity/dstIdentity carry the attributes the caller gathered before the
// rename (ETag best effort).
func (r *Registry) PrepareRename(src, dst string, srcIdentity, dstIdentity Identity, srcPresent, dstPresent bool) *Txn {
	r.mu.Lock()
	defer r.mu.Unlock()

	srcRec := r.nameLocked(src)
	dstRec := r.nameLocked(dst)

	t := &Txn{
		Kind:        TxnRename,
		Src:         src,
		Dst:         dst,
		SrcGen:      srcRec.gen,
		DstGen:      dstRec.gen,
		SrcID:       srcRec.id,
		DstID:       dstRec.id,
		SrcIdentity: srcIdentity,
		DstIdentity: dstIdentity,
		SrcPresent:  srcPresent,
		DstPresent:  dstPresent,
	}
	if srcRec.id != 0 {
		if o := r.objs[srcRec.id]; o != nil && srcIdentity.ETag == "" {
			t.SrcIdentity = o.self
		}
	}
	return t
}

// PrepareDelete snapshots the source state for a delete.
func (r *Registry) PrepareDelete(src string, identity Identity, present bool) *Txn {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec := r.nameLocked(src)
	t := &Txn{
		Kind:        TxnDelete,
		Src:         src,
		SrcGen:      rec.gen,
		SrcID:       rec.id,
		SrcIdentity: identity,
		SrcPresent:  present,
	}
	if rec.id != 0 {
		if o := r.objs[rec.id]; o != nil && identity.ETag == "" {
			t.SrcIdentity = o.self
		}
	}
	return t
}

// CommitRename applies a successful rename to the local mapping:
//   - the source object ID becomes alive at Dst with a new destination
//     generation and all its handles are repathed (never two writable names);
//   - an object previously bound to Dst becomes deleted for its open handles;
//   - the source name becomes a negative record with a new generation.
//
// Name records whose generation moved between prepare and commit are left
// alone (reported as conflicts) so a concurrently recreated name is neither
// stolen nor tombstoned.
func (r *Registry) CommitRename(t *Txn) CommitPlan {
	return r.commitRename(t, true)
}

// CommitRenameLockedHandles is CommitRename for callers that already hold the
// write locks of every handle attached to the transaction's source and
// destination objects (used to keep flush/upload paths blocked across the whole
// remote rename). It must not be mixed with unlocked commits for the same
// handles.
func (r *Registry) CommitRenameLockedHandles(t *Txn) CommitPlan {
	return r.commitRename(t, false)
}

func (r *Registry) commitRename(t *Txn, lockHandles bool) CommitPlan {
	r.mu.Lock()
	defer r.mu.Unlock()

	srcRec := r.nameLocked(t.Src)
	dstRec := r.nameLocked(t.Dst)

	srcConflict := srcRec.gen != t.SrcGen
	dstConflict := dstRec.gen != t.DstGen

	newSrcGen := t.SrcGen + 1
	newDstGen := t.DstGen + 1

	plan := CommitPlan{
		SrcGen:      newSrcGen,
		DstGen:      newDstGen,
		SrcID:       t.SrcID,
		ReplacedID:  t.DstID,
		SrcConflict: srcConflict,
		DstConflict: dstConflict,
	}

	// Move the source object identity to the destination name.
	if t.SrcID != 0 {
		if o := r.objs[t.SrcID]; o != nil {
			o.path = t.Dst
			o.gen = newDstGen
			o.state = StateAlive
			for h := range o.handles {
				if lockHandles {
					h.Lock()
					h.Rebind(t.Dst, newDstGen)
					h.Unlock()
				} else {
					h.Rebind(t.Dst, newDstGen)
				}
				plan.MovedHandles = append(plan.MovedHandles, h)
			}
			plan.MovedCount = uint32(len(plan.MovedHandles))
		}
	}

	// The object overwritten at the destination dies for name purposes; its
	// open handles keep working on their local data but must not write back.
	if t.DstID != 0 && t.DstID != t.SrcID {
		if o := r.objs[t.DstID]; o != nil {
			o.state = StateDeleted
			o.gen = newDstGen
			for h := range o.handles {
				plan.ReplacedHandles = append(plan.ReplacedHandles, h)
			}
			plan.OrphanCount = uint32(len(plan.ReplacedHandles))
		}
	}

	if !srcConflict {
		srcRec.gen = newSrcGen
		srcRec.present = false
		srcRec.id = 0
	}
	if !dstConflict {
		dstRec.gen = newDstGen
		dstRec.present = true
		dstRec.id = t.SrcID // zero when there were no open handles
	}

	return plan
}

// CommitDelete applies a successful delete: the object ID is marked deleted so
// its handles stop writing back, and the name becomes a negative record with a
// new generation. If the name advanced generations between prepare and commit
// (a new object reoccupied it), the name record is untouched and only the old
// object is neutralised. Returns the generation left on the name.
func (r *Registry) CommitDelete(t *Txn) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec := r.nameLocked(t.Src)
	conflict := rec.gen != t.SrcGen
	newGen := t.SrcGen + 1

	if t.SrcID != 0 {
		if o := r.objs[t.SrcID]; o != nil {
			o.state = StateDeleted
			o.gen = newGen
		}
	}

	if !conflict {
		rec.gen = newGen
		rec.present = false
		rec.id = 0
		return newGen, false
	}
	return rec.gen, true
}

// MarkAmbiguous puts an object in the identity-verification-required state
// after an rename/delete whose remote outcome could not be determined.
func (r *Registry) MarkAmbiguous(id handlemap.ObjectID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if o := r.objs[id]; o != nil && o.state == StateAlive {
		o.state = StateAmbiguous
	}
}

// HandlesOf returns a snapshot of the handles currently attached to an object
// ID. Cache layers use it to serialize in-flight flushes against rename/delete
// commits.
func (r *Registry) HandlesOf(id handlemap.ObjectID) []*handlemap.Handle {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.objs[id]
	if o == nil {
		return nil
	}
	out := make([]*handlemap.Handle, 0, len(o.handles))
	for h := range o.handles {
		out = append(out, h)
	}
	return out
}

// MarkDeletedID forces an object into the deleted state, used when a write-back
// time probe proves the pinned object no longer exists at the name or a
// different object occupies it.
func (r *Registry) MarkDeletedID(id handlemap.ObjectID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if o := r.objs[id]; o != nil {
		o.state = StateDeleted
	}
}

// MarkAlive clears an ambiguous/deleted state after a probe proved the object
// still exists under path with the pinned identity.
func (r *Registry) MarkAlive(id handlemap.ObjectID, path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if o := r.objs[id]; o != nil {
		o.state = StateAlive
		if path != "" {
			o.path = path
		}
	}
}

// NoteExternalPresent records that a different object now occupies path
// (discovered while reconciling an uncertain delete/rename). The name advances
// generation and is positive without stealing the binding.
func (r *Registry) NoteExternalPresent(path string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.nameLocked(path)
	rec.gen++
	rec.present = true
	rec.id = 0
	return rec.gen
}

// Resolve returns the current binding of an object ID. ok is false when the ID
// was never registered (callers then fall back to path-based behaviour).
func (r *Registry) Resolve(id handlemap.ObjectID) (Binding, bool) {
	if id == handlemap.InvalidObjectID {
		return Binding{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.objs[id]
	if o == nil {
		return Binding{}, false
	}
	return Binding{ID: o.id, Path: o.path, Gen: o.gen, State: o.state, Self: o.self}, true
}

// Rollback releases a prepared transaction without changing any published
// mapping. Prepare never mutates mappings, so this exists for API symmetry and
// to mark the transaction done in caller logs/assertions.
func (r *Registry) Rollback(_ *Txn) {}

// ---------------------------------------------------------------------------
// Outcome classification for uncertain remote responses
// ---------------------------------------------------------------------------

// Probe is the freshly fetched state of the rename source/destination used to
// classify an uncertain remote rename.
type Probe struct {
	SrcExists bool
	DstExists bool
	Src       Identity
	Dst       Identity
}

// RenameVerdict classifies the real state behind an uncertain rename response.
type RenameVerdict int

const (
	// RenameInconclusive: remote truth could not be established; keep the
	// object ambiguous and invalidate caches.
	RenameInconclusive RenameVerdict = iota
	// RenameCommitted: source gone, destination present. Apply the commit.
	RenameCommitted
	// RenameComplete: destination present and the source still holds the
	// pinned object (copy succeeded, source delete did not). Finish by
	// deleting the pinned source, then commit.
	RenameComplete
	// RenameSrcRecreated: destination present, but the source now holds a
	// different object. Commit the rename without deleting the recreated
	// source.
	RenameSrcRecreated
	// RenameRolledBack: the pinned source object is still present and the
	// destination is absent; the rename did not take.
	RenameRolledBack
)

// DecideRename classifies an uncertain rename outcome from fresh probes.
func DecideRename(t *Txn, p Probe) RenameVerdict {
	switch {
	case p.DstExists && !p.SrcExists:
		return RenameCommitted
	case p.DstExists && p.SrcExists:
		switch {
		case sameOrUnknown(p.Src, t.SrcIdentity) && !t.SrcIdentity.IsZero():
			// Pinned object is still at the source and a copy landed at the
			// destination: finish the move by deleting the source.
			return RenameComplete
		case differentProven(p.Src, t.SrcIdentity):
			// Source was recreated by someone else; destination holds the
			// renamed object. Commit without touching the new source.
			return RenameSrcRecreated
		default:
			// Both exist but the pinned identity is unknown: a source delete
			// here could remove either the pinned object or a recreated one.
			// Do not guess.
			return RenameInconclusive
		}
	case !p.DstExists && p.SrcExists:
		if sameOrUnknown(p.Src, t.SrcIdentity) {
			return RenameRolledBack
		}
		// Source holds a proven different object and destination is gone:
		// nothing safe can be inferred.
		return RenameInconclusive
	default:
		// Both absent (or probes failed): cannot claim either outcome.
		return RenameInconclusive
	}
}

// DeleteVerdict classifies the real state behind an uncertain delete response.
type DeleteVerdict int

const (
	// DeleteInconclusive: remote truth could not be established.
	DeleteInconclusive DeleteVerdict = iota
	// DeleteCommitted: the name is absent.
	DeleteCommitted
	// DeleteRolledBack: the pinned object is still present, the delete did
	// not take.
	DeleteRolledBack
	// DeleteRecreated: the name now holds a different object. The old object
	// is gone (commit its deletion) but the name must stay positive with a
	// fresh generation.
	DeleteRecreated
)

// DecideDelete classifies an uncertain delete outcome from a fresh probe.
// present=false means the probe confirmed absence; if exists is true the probe
// returned the current object's identity.
func DecideDelete(t *Txn, exists bool, current Identity) DeleteVerdict {
	if !exists {
		return DeleteCommitted
	}
	if sameOrUnknown(current, t.SrcIdentity) {
		// Same object (or identity never captured): the delete did not take.
		return DeleteRolledBack
	}
	// Proven different object at the name: the old object was deleted and a
	// new one recreated it.
	return DeleteRecreated
}
