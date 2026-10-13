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

package block_cache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
	"github.com/Azure/azure-storage-fuse/v2/internal/namespace"
)

func bcIdentityOfAttr(attr *internal.ObjAttr) namespace.Identity {
	id := namespace.Identity{}
	if attr != nil {
		id.ETag = attr.ETag
		id.Mtime = attr.Mtime
		id.Size = attr.Size
	}
	return id
}

// bcDefinitiveError mirrors the definitive/uncertain classification used by
// file_cache: errno results are unambiguous, transport/timeout/5xx errors may
// still have been applied remotely and require a state probe.
func bcDefinitiveError(err error) bool {
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

// lockTwoNames locks two per-path locks in a stable order to avoid deadlocks
// between concurrent renames.
func (bc *BlockCache) lockTwoNames(a, b string) (func(), func()) {
	names := []string{a, b}
	if a != b {
		sort.Strings(names)
	}
	l0 := bc.fileLocks.Get(names[0])
	l0.Lock()
	if a == b {
		return func() {}, func() { l0.Unlock() }
	}
	l1 := bc.fileLocks.Get(names[1])
	l1.Lock()
	return func() { l1.Unlock() }, func() { l0.Unlock() }
}

func (bc *BlockCache) probeAttr(name string) (*internal.ObjAttr, bool, error) {
	attr, err := bc.NextComponent().GetAttr(internal.GetAttrOptions{Name: name, BypassCache: true})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return attr, true, nil
}

// lockAttachedHandles locks every handle currently attached to the given
// object IDs in a stable order and returns an unlock function. Block-cache
// flush holds the handle lock for its whole commit sequence; locking the
// handles before the remote rename/delete and keeping them locked until the
// local commit guarantees no upload can race the name change (and that the
// registry commit may rebind the handles without re-locking).
func (bc *BlockCache) lockAttachedHandles(ids ...handlemap.ObjectID) func() {
	seen := make(map[*handlemap.Handle]struct{})
	hs := make([]*handlemap.Handle, 0)
	for _, id := range ids {
		if id == handlemap.InvalidObjectID {
			continue
		}
		for _, h := range bc.ns.HandlesOf(id) {
			if _, ok := seen[h]; !ok {
				seen[h] = struct{}{}
				hs = append(hs, h)
			}
		}
	}
	// Stable order across callers so two transactions that touch overlapping
	// handles cannot lock them in opposite orders.
	sort.Slice(hs, func(i, j int) bool {
		if hs[i].ID != hs[j].ID {
			return hs[i].ID < hs[j].ID
		}
		return fmt.Sprintf("%p", hs[i]) < fmt.Sprintf("%p", hs[j])
	})
	for _, h := range hs {
		h.Lock()
	}
	return func() {
		for _, h := range hs {
			h.Unlock()
		}
	}
}

// diskBlockPrefix returns the on-disk prefix of all block files belonging to a
// name. Disk blocks are stored flat as "<name>::<block-id>".
func (bc *BlockCache) diskBlockPrefix(name string) string {
	if bc.tmpPath == "" {
		return ""
	}
	return filepath.Join(bc.tmpPath, name)
}

// diskFilesForName lists on-disk block files belonging to a name.
func (bc *BlockCache) diskFilesForName(name string) []string {
	prefix := bc.diskBlockPrefix(name)
	if prefix == "" {
		return nil
	}
	files, err := filepath.Glob(prefix + "*")
	if err != nil {
		return nil
	}
	return files
}

// hasDiskBlocks reports whether any on-disk block file exists for a name.
func (bc *BlockCache) hasDiskBlocks(name string) bool {
	return len(bc.diskFilesForName(name)) > 0
}

// removeDiskBlocksGuarded removes disk block files and node-map entries of a
// name while the name still sits at gen. A newer generation (object recreated
// under the same name) is left untouched.
func (bc *BlockCache) removeDiskBlocksGuarded(name string, gen uint64) {
	if gen != 0 && !bc.ns.CheckGen(name, gen) {
		log.Debug("BlockCache::removeDiskBlocksGuarded : skip %s, name moved to newer generation", name)
		return
	}
	for _, f := range bc.diskFilesForName(name) {
		_ = os.Remove(f)
		bc.fileNodeMap.Delete(filepath.Base(f))
	}
}

// resolveWriteBackTarget is the block-cache write-back identity gate, called
// while the caller holds the handle lock. It returns the effective storage
// name; empty means dirty blocks must not be committed (the object was
// deleted/overwritten, or its current identity could not be verified).
func (bc *BlockCache) resolveWriteBackTarget(handle *handlemap.Handle) string {
	id, _ := handle.Identity()
	if id == handlemap.InvalidObjectID {
		return handle.Path
	}
	binding, ok := bc.ns.Resolve(id)
	if !ok {
		return handle.Path
	}

	switch binding.State {
	case namespace.StateAlive:
		// Caller holds the handle lock; rebind directly.
		if handle.Path != binding.Path {
			log.Debug("BlockCache : handle %d follows rename %s -> %s", handle.ID, handle.Path, binding.Path)
			handle.Rebind(binding.Path, binding.Gen)
		}
		return binding.Path
	case namespace.StateDeleted:
		log.Info("BlockCache::FlushFile : suppressing block commit for deleted object (handle %d, last path %s)", handle.ID, binding.Path)
		handle.Flags.Clear(handlemap.HandleFlagDirty)
		return ""
	case namespace.StateAmbiguous:
		attr, exists, err := bc.probeAttr(binding.Path)
		switch {
		case err != nil:
			log.Err("BlockCache::FlushFile : cannot verify object identity at %s [%s]", binding.Path, err.Error())
			return ""
		case !exists:
			log.Warn("BlockCache::FlushFile : object vanished at %s, suppressing block commit", binding.Path)
			bc.ns.MarkDeletedID(id)
			handle.Flags.Clear(handlemap.HandleFlagDirty)
			return ""
		case namespace.SameIdentity(bcIdentityOfAttr(attr), binding.Self):
			log.Info("BlockCache::FlushFile : object identity verified at %s", binding.Path)
			bc.ns.MarkAlive(id, binding.Path)
			handle.Rebind(binding.Path, binding.Gen)
			return binding.Path
		default:
			log.Err("BlockCache::FlushFile : object at %s differs from handle %d identity; refusing block commit", binding.Path, handle.ID)
			bc.ns.MarkDeletedID(id)
			return ""
		}
	}
	return handle.Path
}

// applyRenameDisk mirrors a committed rename in the disk block cache and the
// node map, and migrates open handle counts. Called with both path locks held.
func (bc *BlockCache) applyRenameDisk(options internal.RenameFileOptions, plan namespace.CommitPlan) {
	srcPrefix := bc.diskBlockPrefix(options.Src)
	dstPrefix := bc.diskBlockPrefix(options.Dst)

	// Cross-directory rename: ensure the destination's on-disk directory
	// exists before moving block files.
	if dstPrefix != "" {
		_ = os.MkdirAll(filepath.Dir(dstPrefix), 0755)
	}

	if srcPrefix != "" && dstPrefix != "" {
		renamed := make(map[string]struct{})
		files, err := filepath.Glob(srcPrefix + "*")
		if err == nil {
			for _, f := range files {
				newName := strings.Replace(f, srcPrefix, dstPrefix, 1)
				if rerr := os.Rename(f, newName); rerr != nil {
					// Local mirror failed: drop both old and new node entries
					// and remove the stale file so the next open refetches.
					log.Err("BlockCache::RenameFile : failed to rename disk block %s [%s]", f, rerr.Error())
					_ = os.Remove(newName)
					bc.fileNodeMap.Delete(filepath.Base(newName))
					bc.fileNodeMap.Delete(filepath.Base(f))
					continue
				}
				renamed[filepath.Base(newName)] = struct{}{}
				// Move the node-map entry to the new key.
				if node, present := bc.fileNodeMap.LoadAndDelete(filepath.Base(f)); present {
					bc.fileNodeMap.Store(filepath.Base(newName), node)
				}
			}
		}

		// Destination blocks that belonged to the overwritten object are stale
		// now; remove leftovers the source rename did not produce. Blocks just
		// moved from the source are preserved.
		for _, f := range bc.diskFilesForName(options.Dst) {
			base := filepath.Base(f)
			if _, keep := renamed[base]; keep {
				continue
			}
			_ = os.Remove(f)
			bc.fileNodeMap.Delete(base)
		}
	}

	if options.Src != options.Dst {
		bc.fileLocks.TransferHandleCount(options.Src, options.Dst)
	}

	log.Debug("BlockCache::RenameFile : committed %s -> %s (moved handles %d, orphaned %d, srcGen %d, dstGen %d, srcConflict %t, dstConflict %t)",
		options.Src, options.Dst, plan.MovedCount, plan.OrphanCount, plan.SrcGen, plan.DstGen, plan.SrcConflict, plan.DstConflict)
}

// reconcileRename probes actual state after an uncertain remote rename.
func (bc *BlockCache) reconcileRename(txn *namespace.Txn) namespace.RenameVerdict {
	srcAttr, srcExists, srcErr := bc.probeAttr(txn.Src)
	dstAttr, dstExists, dstErr := bc.probeAttr(txn.Dst)
	if srcErr != nil || dstErr != nil {
		return namespace.RenameInconclusive
	}
	return namespace.DecideRename(txn, namespace.Probe{
		SrcExists: srcExists,
		DstExists: dstExists,
		Src:       bcIdentityOfAttr(srcAttr),
		Dst:       bcIdentityOfAttr(dstAttr),
	})
}

// reconcileDelete probes actual state after an uncertain remote delete.
func (bc *BlockCache) reconcileDelete(txn *namespace.Txn) namespace.DeleteVerdict {
	attr, exists, err := bc.probeAttr(txn.Src)
	if err != nil {
		return namespace.DeleteInconclusive
	}
	return namespace.DecideDelete(txn, exists, bcIdentityOfAttr(attr))
}

// probeDirectoryOutcome reconciles an uncertain directory rename/delete by
// probing actual marker state, bypassing caches.
func (bc *BlockCache) probeDirectoryOutcome(src, dst string, isRename bool) bool {
	_, srcExists, srcErr := bc.probeAttr(src)
	if srcErr != nil {
		return false
	}
	if !isRename {
		return !srcExists
	}
	_, dstExists, dstErr := bc.probeAttr(dst)
	if dstErr != nil {
		return false
	}
	return dstExists || !srcExists
}

// invalidateDirectoryGuarded invalidates all on-disk block files under a
// directory prefix, skipping names whose generation advanced (recreated
// objects).
func (bc *BlockCache) invalidateDirectoryGuarded(name string) {
	if bc.tmpPath == "" {
		return
	}
	prefix := filepath.Join(bc.tmpPath, name)
	files, err := filepath.Glob(prefix + "*")
	if err != nil {
		return
	}
	for _, f := range files {
		base := filepath.Base(f)
		owner := base
		if idx := strings.Index(base, "::"); idx >= 0 {
			owner = base[:idx]
		}
		gen := bc.ns.Generation(owner)
		if gen != 0 && !bc.ns.CheckGen(owner, gen) {
			continue
		}
		_ = os.Remove(f)
		bc.fileNodeMap.Delete(base)
	}
}
