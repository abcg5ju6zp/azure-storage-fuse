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

package file_cache

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
	"github.com/Azure/azure-storage-fuse/v2/internal/namespace"
)

// identityOfAttr extracts the best-effort remote identity fingerprint from an
// ObjAttr. nil is accepted and yields the zero identity.
func identityOfAttr(attr *internal.ObjAttr) namespace.Identity {
	id := namespace.Identity{}
	if attr != nil {
		id.ETag = attr.ETag
		id.Mtime = attr.Mtime
		id.Size = attr.Size
	}
	return id
}

// isDefinitiveRemoteError reports whether a remote rename/delete error
// describes an unambiguous terminal outcome. Network/timeout/5xx style errors
// are not definitive: the request may still have been applied server side, so
// the caller must reconcile against actual remote state.
func isDefinitiveRemoteError(err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, syscall.ENOENT),
		errors.Is(err, syscall.EEXIST),
		errors.Is(err, syscall.EACCES),
		errors.Is(err, syscall.EPERM),
		errors.Is(err, syscall.ENOTDIR),
		errors.Is(err, syscall.EISDIR),
		errors.Is(err, syscall.ENOTEMPTY),
		errors.Is(err, syscall.ENAMETOOLONG),
		errors.Is(err, syscall.EINVAL),
		errors.Is(err, syscall.ENOSYS):
		return true
	default:
		return false
	}
}

// probeAttr fetches the actual current attributes of a name from the remote
// bypassing the attribute cache. Returns (attr, exists, err). A confirmed
// absence is reported as (nil, false, nil); transport errors propagate.
func (fc *FileCache) probeAttr(name string) (*internal.ObjAttr, bool, error) {
	attr, err := fc.NextComponent().GetAttr(internal.GetAttrOptions{Name: name, BypassCache: true})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return attr, true, nil
}

// lockTwoNames locks the per-path locks of two names in a stable order so that
// concurrent renames (e.g. a->b racing with b->a) cannot deadlock. The two
// returned unlock functions must be invoked LIFO by the caller via defers.
func (fc *FileCache) lockTwoNames(a, b string) (func(), func()) {
	names := []string{a, b}
	if a != b {
		sort.Strings(names)
	}
	l0 := fc.fileLocks.Get(names[0])
	l0.Lock()
	if a == b || names[0] == names[1] {
		return func() {}, func() { l0.Unlock() }
	}
	l1 := fc.fileLocks.Get(names[1])
	l1.Lock()
	return func() { l1.Unlock() }, func() { l0.Unlock() }
}

// localPathExists reports whether a non-directory entry is present in the
// local cache.
func localPathExists(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && !info.IsDir()
}

// removeLocalIfGen deletes the local cache file and purges it from the policy
// only while the name still sits at gen. A newer generation means another
// object has taken the name since the operation was prepared; its cache state
// must not be touched.
func (fc *FileCache) removeLocalIfGen(localPath, name string, gen uint64) {
	if gen != 0 && !fc.ns.CheckGen(name, gen) {
		log.Debug("FileCache::removeLocalIfGen : skip removal of %s, name moved to a newer generation", name)
		return
	}
	if err := deleteFile(localPath); err != nil && !os.IsNotExist(err) {
		log.Err("FileCache::removeLocalIfGen : failed to remove %s [%s]", localPath, err.Error())
	}
	fc.policy.CachePurge(localPath)
}

// ---------------------------------------------------------------------------
// Legacy fast paths (no local cache state, no open handles)
// ---------------------------------------------------------------------------

// renameFileLegacyLocked keeps the original rename behaviour for the common
// case where neither name is cached nor open. Both path locks are held by the
// caller.
func (fc *FileCache) renameFileLegacyLocked(options internal.RenameFileOptions, localSrcPath, localDstPath string) error {
	err := fc.NextComponent().RenameFile(options)
	err = fc.validateStorageError(options.Src, err, "RenameFile", false)
	if err != nil {
		log.Err("FileCache::RenameFile : %s failed to rename file [%s]", options.Src, err.Error())
		return err
	}

	// in case of git clone multiple rename requests come for which destination files already exists in system
	// if we do not perform rename operation locally and those destination files are cached then next time they are read
	// we will be serving the wrong content (as we did not rename locally, we still be having older destination files with
	// stale content). We either need to remove dest file as well from cache or just run rename to replace the content.
	err = os.Rename(localSrcPath, localDstPath)
	if err != nil && !os.IsNotExist(err) {
		log.Err("FileCache::RenameFile : %s failed to rename local file %s [%s]", localSrcPath, localDstPath, err.Error())
	}

	if err != nil {
		// If there was a problem in local rename then delete the destination file
		// it might happen that dest file was already there and local rename failed
		// so deleting local dest file ensures next open of that will get the updated file from container
		err = deleteFile(localDstPath)
		if err != nil && !os.IsNotExist(err) {
			log.Err("FileCache::RenameFile : failed to delete local file %s [%s]", localDstPath, err.Error())
		}

		fc.policy.CachePurge(localDstPath)
	}

	err = deleteFile(localSrcPath)
	if err != nil && !os.IsNotExist(err) {
		log.Err("FileCache::RenameFile : failed to delete local file %s [%s]", localSrcPath, err.Error())
	}

	fc.policy.CachePurge(localSrcPath)

	if fc.cacheTimeout == 0 {
		// Destination file needs to be deleted immediately
		fc.policy.CachePurge(localDstPath)
	} else {
		// Add destination file to cache, it will be removed on timeout
		fc.policy.CacheValid(localDstPath)
	}

	return nil
}

// deleteFileLegacyLocked keeps the original delete behaviour for the case
// where the file is neither cached nor open. The path lock is held by the
// caller.
func (fc *FileCache) deleteFileLegacyLocked(options internal.DeleteFileOptions, localPath string) error {
	err := fc.NextComponent().DeleteFile(options)
	err = fc.validateStorageError(options.Name, err, "DeleteFile", false)
	if err != nil {
		log.Err("FileCache::DeleteFile : error  %s [%s]", options.Name, err.Error())
		return err
	}

	err = deleteFile(localPath)
	if err != nil && !os.IsNotExist(err) {
		log.Err("FileCache::DeleteFile : failed to delete local file %s [%s]", localPath, err.Error())
	}

	fc.policy.CachePurge(localPath)
	return nil
}

// ---------------------------------------------------------------------------
// Rename
// ---------------------------------------------------------------------------

// renameFileLocked is the identity-safe rename path, used when at least one
// side is cached locally or has open handles. It pins the source object
// identity and destination precondition, performs the remote rename, and only
// then commits the local name mapping; a failed/uncertain remote result rolls
// back or is reconciled against actual remote state. Both path locks must be
// held by the caller.
func (fc *FileCache) renameFileLocked(options internal.RenameFileOptions) error {
	localSrcPath := filepath.Join(fc.tmpPath, options.Src)
	localDstPath := filepath.Join(fc.tmpPath, options.Dst)

	txn := fc.ns.PrepareRename(options.Src, options.Dst,
		identityOfAttr(options.SrcAttr), identityOfAttr(options.DstAttr),
		options.SrcAttr != nil, options.DstAttr != nil)

	rawErr := fc.NextComponent().RenameFile(options)
	err := fc.validateStorageError(options.Src, rawErr, "RenameFile", false)

	if err != nil {
		if isDefinitiveRemoteError(rawErr) {
			// Definite terminal outcome (incl. the client-side EIO synthesized
			// for a local-only file missing remotely): nothing was committed,
			// local mapping stays intact, error code is preserved.
			log.Err("FileCache::RenameFile : %s failed to rename file [%s]", options.Src, err.Error())
			fc.ns.Rollback(txn)
			return err
		}

		// The response is uncertain: reconcile against actual remote state.
		switch fc.reconcileRename(txn, err) {
		case namespace.RenameCommitted:
			log.Info("FileCache::RenameFile : rename %s -> %s confirmed by state probe after uncertain response", options.Src, options.Dst)
		case namespace.RenameSrcRecreated:
			// Source name now holds a freshly created different object. Make
			// sure the commit does not tombstone it.
			log.Info("FileCache::RenameFile : %s recreated with a different object during %s -> %s", options.Src, options.Src, options.Dst)
			fc.ns.NoteExternalPresent(options.Src)
		case namespace.RenameComplete:
			// Copy landed at destination but the source delete did not. Finish
			// the move, but only while the source is still the pinned object.
			log.Warn("FileCache::RenameFile : finishing partial rename %s -> %s by deleting source", options.Src, options.Dst)
			delErr := fc.NextComponent().DeleteFile(internal.DeleteFileOptions{Name: options.Src})
			if delErr != nil && !errors.Is(delErr, syscall.ENOENT) {
				log.Err("FileCache::RenameFile : failed to finish partial rename, source delete error [%s]", delErr.Error())
				// Do not delete local cache state (it may hold dirty data of
				// open handles); fence write-back and report the failure.
				fc.ns.MarkAmbiguous(txn.SrcID)
				return err
			}
		case namespace.RenameRolledBack:
			log.Warn("FileCache::RenameFile : rename %s -> %s did not take, returning original error", options.Src, options.Dst)
			return err
		default: // RenameInconclusive
			log.Err("FileCache::RenameFile : unable to determine outcome of %s -> %s [%s], fencing write-back", options.Src, options.Dst, err.Error())
			// Local cache files (possibly dirty, held by open descriptors) are
			// preserved; handles are fenced until identity is re-verified.
			fc.ns.MarkAmbiguous(txn.SrcID)
			return err
		}
	}

	plan := fc.ns.CommitRename(txn)

	// Remote state is authoritative from here on. Mirror the rename in the
	// local cache; failures complete invalidation instead of leaving stale
	// content behind.
	fc.applyRenameLocal(options, localSrcPath, localDstPath, plan)
	return nil
}

// applyRenameLocal mirrors a committed rename in the local cache and migrates
// the open-handle counts. Called with both path locks held.
func (fc *FileCache) applyRenameLocal(options internal.RenameFileOptions,
	localSrcPath, localDstPath string, plan namespace.CommitPlan) {

	// Cross-directory rename: make sure the destination's local directory
	// exists so the inode move does not fail just because the subtree was
	// never cached locally.
	if mkErr := os.MkdirAll(filepath.Dir(localDstPath), 0755|0700); mkErr != nil {
		log.Err("FileCache::RenameFile : failed to create local destination directory %s [%s]", filepath.Dir(localDstPath), mkErr.Error())
	}

	renameErr := os.Rename(localSrcPath, localDstPath)
	switch {
	case renameErr == nil:
		// Local inode moved; open descriptors stay valid and now follow the
		// destination through the registry.
	case os.IsNotExist(renameErr):
		// Source was not cached. A stale cached destination would serve the
		// old (overwritten) content on next open, so invalidate it.
		fc.removeLocalIfGen(localDstPath, options.Dst, plan.DstGen)
	default:
		// Local mirror failed (IO error). Finish by invalidation so the next
		// open fetches the renamed object from storage. Handles continue on
		// their live descriptor and upload through the destination identity.
		log.Err("FileCache::RenameFile : local rename %s -> %s failed [%s], invalidating cache", localSrcPath, localDstPath, renameErr.Error())
		fc.removeLocalIfGen(localSrcPath, options.Src, plan.SrcGen)
		fc.removeLocalIfGen(localDstPath, options.Dst, plan.DstGen)
	}

	// Any source cache file left behind (e.g. mirror failed) is removed under
	// the source's post-commit generation; a recreated source is preserved
	// (generation mismatch).
	fc.removeLocalIfGen(localSrcPath, options.Src, plan.SrcGen)

	if fc.cacheTimeout == 0 {
		fc.policy.CachePurge(localDstPath)
	} else if localPathExists(localDstPath) {
		fc.policy.CacheValid(localDstPath)
	}

	// Migrate open handle counts source -> destination. Handles of the
	// overwritten destination object were already counted at the destination;
	// source handles are repathed to it by the registry commit. Skip on
	// same-name renames, which are no-ops handled by the caller.
	if options.Src != options.Dst {
		fc.fileLocks.TransferHandleCount(options.Src, options.Dst)
	}

	log.Debug("FileCache::RenameFile : committed %s -> %s (moved handles %d, orphaned %d, srcGen %d, dstGen %d, srcConflict %t, dstConflict %t)",
		options.Src, options.Dst, plan.MovedCount, plan.OrphanCount, plan.SrcGen, plan.DstGen, plan.SrcConflict, plan.DstConflict)
}

// reconcileRename probes actual source/destination state after an uncertain
// remote rename response and classifies the outcome against the pinned source
// identity.
func (fc *FileCache) reconcileRename(txn *namespace.Txn, _ error) namespace.RenameVerdict {
	srcAttr, srcExists, srcErr := fc.probeAttr(txn.Src)
	dstAttr, dstExists, dstErr := fc.probeAttr(txn.Dst)
	if srcErr != nil || dstErr != nil {
		log.Warn("FileCache::reconcileRename : state probe failed for %s -> %s [%v/%v]",
			txn.Src, txn.Dst, srcErr, dstErr)
		return namespace.RenameInconclusive
	}
	return namespace.DecideRename(txn, namespace.Probe{
		SrcExists: srcExists,
		DstExists: dstExists,
		Src:       identityOfAttr(srcAttr),
		Dst:       identityOfAttr(dstAttr),
	})
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

// deleteFileLocked is the identity-safe delete path used when the file is
// cached or has open handles. Open handles are bound to the deleted object
// identity so their late flush/release cannot recreate it. The path lock must
// be held by the caller.
func (fc *FileCache) deleteFileLocked(options internal.DeleteFileOptions) error {
	localPath := filepath.Join(fc.tmpPath, options.Name)

	txn := fc.ns.PrepareDelete(options.Name, namespace.Identity{}, true)

	rawErr := fc.NextComponent().DeleteFile(options)
	err := fc.validateStorageError(options.Name, rawErr, "DeleteFile", false)

	if err != nil {
		if isDefinitiveRemoteError(rawErr) {
			// Definite terminal outcome (incl. the client-side EIO for a
			// local-only file missing remotely): local mapping and cache stay
			// intact, error code is preserved.
			log.Err("FileCache::DeleteFile : error %s [%s]", options.Name, err.Error())
			fc.ns.Rollback(txn)
			return err
		}

		switch fc.reconcileDelete(txn, err, options.Name) {
		case namespace.DeleteCommitted:
			log.Info("FileCache::DeleteFile : delete of %s confirmed by state probe after uncertain response", options.Name)
		case namespace.DeleteRecreated:
			log.Warn("FileCache::DeleteFile : %s holds a new object; old open handles will not write back", options.Name)
			fc.ns.NoteExternalPresent(options.Name)
		case namespace.DeleteRolledBack:
			log.Warn("FileCache::DeleteFile : delete of %s did not take, returning original error", options.Name)
			return err
		default:
			log.Err("FileCache::DeleteFile : unable to determine outcome of delete %s [%s], fencing write-back", options.Name, err.Error())
			fc.ns.MarkAmbiguous(txn.SrcID)
			return err
		}
	}

	gen, conflict := fc.ns.CommitDelete(txn)
	// Remove the local inode under the committed generation. On Linux this
	// succeeds while descriptors are open; descriptors keep operating on the
	// unlinked inode and their write-back is suppressed by the Deleted binding.
	// A conflict means a different object was found at the name during
	// reconciliation (external recreate). The path lock is held for the whole
	// operation, so no local create can exist concurrently: what is on disk is
	// stale data of the deleted object and must be invalidated so the next open
	// refetches the new object. Generation-fenced callers outside this
	// transaction (release teardown, async directory invalidation) use their
	// older captured generation and are refused instead.
	if conflict {
		log.Debug("FileCache::DeleteFile : %s holds a recreated object, invalidating stale local cache", options.Name)
	}
	fc.removeLocalIfGen(localPath, options.Name, gen)
	return nil
}

// reconcileDelete probes actual state after an uncertain delete response and
// classifies it against the pinned source identity.
func (fc *FileCache) reconcileDelete(txn *namespace.Txn, _ error, name string) namespace.DeleteVerdict {
	attr, exists, probeErr := fc.probeAttr(name)
	if probeErr != nil {
		log.Warn("FileCache::reconcileDelete : state probe failed for %s [%s]", name, probeErr.Error())
		return namespace.DeleteInconclusive
	}
	return namespace.DecideDelete(txn, exists, identityOfAttr(attr))
}

// ---------------------------------------------------------------------------
// Handle write-back gating
// ---------------------------------------------------------------------------

// resolveWriteBackTarget decides where (and whether) a handle's dirty data may
// be uploaded. Returns the effective storage path (empty means write-back must
// be skipped) and updates the handle path to the object's current name.
//
//   - Alive bindings write to the current name.
//   - Deleted bindings never write back (unlink / overwrite semantics).
//   - Ambiguous bindings re-probe with identity verification: same object may
//     proceed, a missing object is treated as deleted, a different object as a
//     hard error so another object is never overwritten.
func (fc *FileCache) resolveWriteBackTarget(handle *handlemap.Handle) string {
	id, _ := handle.Identity()
	if id == handlemap.InvalidObjectID {
		return handle.Path // untracked handle: legacy behaviour
	}

	binding, ok := fc.ns.Resolve(id)
	if !ok {
		return handle.Path
	}

	switch binding.State {
	case namespace.StateDeleted:
		log.Info("FileCache::FlushFile : suppressing write-back for deleted object (handle %d, last path %s)", handle.ID, binding.Path)
		handle.Flags.Clear(handlemap.HandleFlagDirty)
		return ""
	case namespace.StateAlive:
		fc.repathHandle(handle, binding.Path, binding.Gen)
		return binding.Path
	case namespace.StateAmbiguous:
		attr, exists, err := fc.probeAttr(binding.Path)
		switch {
		case err != nil:
			log.Err("FileCache::FlushFile : cannot verify object identity at %s [%s]", binding.Path, err.Error())
			return "" // keep ambiguous; dirty bit retained, retry on next flush
		case !exists:
			log.Warn("FileCache::FlushFile : object vanished at %s, suppressing write-back", binding.Path)
			fc.ns.MarkDeletedID(id)
			handle.Flags.Clear(handlemap.HandleFlagDirty)
			return ""
		case namespace.SameIdentity(identityOfAttr(attr), binding.Self):
			log.Info("FileCache::FlushFile : object identity verified at %s after uncertain rename/delete", binding.Path)
			fc.ns.MarkAlive(id, binding.Path)
			fc.repathHandle(handle, binding.Path, binding.Gen)
			return binding.Path
		default:
			log.Err("FileCache::FlushFile : object at %s is a different object than handle %d was opened on; refusing write-back", binding.Path, handle.ID)
			fc.ns.MarkDeletedID(id)
			return ""
		}
	}
	return handle.Path
}

// repathHandle updates a handle's cached path under its lock.
func (fc *FileCache) repathHandle(handle *handlemap.Handle, path string, gen uint64) {
	handle.Lock()
	defer handle.Unlock()
	if handle.Path != path {
		log.Debug("FileCache : handle %d follows rename %s -> %s", handle.ID, handle.Path, path)
		handle.Rebind(path, gen)
	}
}

// releaseBinding returns the resolved binding information used while releasing
// a handle: the effective path and the generation that must still own the name
// for local cache teardown to be safe. Zero generation disables the check.
func (fc *FileCache) releaseBinding(handle *handlemap.Handle) (path string, gen uint64, deleted bool) {
	id, _ := handle.Identity()
	if id == handlemap.InvalidObjectID {
		return handle.Path, 0, false
	}
	if binding, ok := fc.ns.Resolve(id); ok {
		fc.repathHandle(handle, binding.Path, binding.Gen)
		return binding.Path, binding.Gen, binding.State == namespace.StateDeleted
	}
	return handle.Path, 0, false
}

// ---------------------------------------------------------------------------
// Directory invalidation with generation guards
// ---------------------------------------------------------------------------

// invalidateDirectoryGuarded recursively invalidates a directory in the local
// cache but skips every name whose generation advanced while the invalidation
// was pending (e.g. file recreated right after a directory delete/rename).
// Without handles this is best effort; the per-file locks taken by the cache
// policy provide the second layer of protection.
func (fc *FileCache) invalidateDirectoryGuarded(name string) {
	log.Trace("FileCache::invalidateDirectoryGuarded : %s", name)

	localPath := filepath.Join(fc.tmpPath, name)
	info, err := os.Stat(localPath)
	if os.IsNotExist(err) {
		return
	} else if err != nil {
		log.Debug("FileCache::invalidateDirectoryGuarded : %s stat err [%s]", name, err.Error())
		return
	}
	if !info.IsDir() {
		return
	}

	// Snapshot the generation of every child before the remote-triggered
	// invalidation runs, so a child recreated in between is preserved.
	guards := make(map[string]uint64)
	_ = filepath.WalkDir(localPath, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil || d == nil || d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(fc.tmpPath, path)
		if relErr == nil {
			guards[path] = fc.ns.Generation(rel)
		}
		return nil
	})

	_ = filepath.WalkDir(localPath, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil || d == nil {
			return nil
		}
		if d.IsDir() {
			_ = deleteFile(path)
			return nil
		}
		rel, relErr := filepath.Rel(fc.tmpPath, path)
		if relErr == nil {
			if gen, seen := guards[path]; seen && !fc.ns.CheckGen(rel, gen) {
				log.Debug("FileCache::invalidateDirectoryGuarded : skip recreated child %s", rel)
				return nil
			}
		}
		fc.policy.CachePurge(path)
		return nil
	})

	_ = deleteFile(localPath)
}

// Reconcile directory rename/delete uncertain outcomes. Returns true when the
// remote operation actually took (local invalidation should proceed).
func (fc *FileCache) probeDirectoryOutcome(src, dst string, isRename bool) bool {
	_, srcExists, srcErr := fc.probeAttr(src)
	if srcErr != nil {
		return false
	}
	if !isRename {
		// Directory delete: the marker blob is optional, but a confirmed
		// absence is sufficient to treat the delete as applied.
		return !srcExists
	}

	_, dstExists, dstErr := fc.probeAttr(dst)
	if dstErr != nil {
		return false
	}
	// Directory rename: destination present (or source gone) treats the rename
	// as applied; a possibly leftover source subtree is fixed by forcing
	// refetch through the guarded invalidation.
	return dstExists || !srcExists
}
