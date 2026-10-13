/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |---- |     | |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2026 Microsoft Corporation. All rights reserved.
*/

package block_cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// waitAsyncCloses bounds the wait for lazy-write background releases (each of
// which performs stage + commit). Returns false when ctx expired first.
func (bc *BlockCache) waitAsyncCloses(ctx context.Context) bool {
	if !bc.lazyWrite {
		return true
	}
	log.Info("BlockCache::waitAsyncCloses : Waiting for async close to complete")

	done := make(chan struct{})
	go func() {
		bc.fileCloseOpt.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-ctx.Done():
		log.Err("BlockCache::waitAsyncCloses : timed out waiting for async closes [%v]", ctx.Err())
		return false
	}
}

// stagedBlocks returns the block index -> blockInfo entries that the service
// holds as staged but uncommitted for this handle. The blockList map is
// otherwise serialized by the per-handle write/flush lock; the shutdown
// drainer does not own that lock, so it acquires it bounded while a possibly
// abandoned commit goroutine still holds it. When the lock cannot be taken no
// entries are returned (the caller conservatively reports in-memory-lost);
// id/size are immutable after insertion and staged/committed are atomic.
func stagedBlocks(handle *handlemap.Handle, maxWait time.Duration) map[int64]*blockInfo {
	out := make(map[int64]*blockInfo)
	if !tryLockHandleBounded(handle, maxWait) {
		log.Err("BlockCache::stagedBlocks : handle %d (%s) still locked by an abandoned commit, "+
			"staged entries cannot be enumerated safely", handle.ID, handle.Path)
		return out
	}
	defer handle.Unlock()

	v, ok := handle.GetValue("blockList")
	if !ok {
		return out
	}
	listMap, ok := v.(map[int64]*blockInfo)
	if !ok {
		return out
	}
	for idx, info := range listMap {
		if info != nil && info.staged.Load() && !info.committed.Load() {
			out[idx] = info
		}
	}
	return out
}

// tryLockHandleBounded attempts TryLock until it succeeds or maxWait elapses.
// Unlike an unbounded Lock(), an abandoned commit holding the handle lock can
// never hang the shutdown sequence.
func tryLockHandleBounded(handle *handlemap.Handle, maxWait time.Duration) bool {
	if handle.TryLock() {
		return true
	}
	if maxWait <= 0 {
		return false
	}
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		if handle.TryLock() {
			return true
		}
	}
	return false
}

// classifyUncommitted maps a still-dirty handle after a failed/timed-out
// commit to recovery records: one record per staged block (recoverable via
// PutBlockList, local disk copy protected when present), or a single
// in-memory-lost record when nothing was staged. lockWait bounds acquisition
// of the handle lock guarding the blockList map.
func (bc *BlockCache) classifyUncommitted(handle *handlemap.Handle, lockWait time.Duration) []internal.UncommittedItem {
	staged := stagedBlocks(handle, lockWait)
	if len(staged) == 0 {
		return []internal.UncommittedItem{{
			Component: bc.Name(),
			Name:      handle.Path,
			Reason:    internal.UncommittedInMemoryLost,
			Size:      handle.Size,
		}}
	}

	indexes := make([]int64, 0, len(staged))
	for idx := range staged {
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })

	items := make([]internal.UncommittedItem, 0, len(indexes))
	for _, idx := range indexes {
		info := staged[idx]
		item := internal.UncommittedItem{
			Component: bc.Name(),
			Name:      handle.Path,
			Reason:    internal.UncommittedStageOnly,
			Size:      int64(info.size),
			BlockIDs:  []string{info.id},
			Offset:    idx * int64(bc.blockSize),
		}
		if bc.tmpPath != "" {
			localPath := filepath.Join(bc.tmpPath, fmt.Sprintf("%s::%d", handle.Path, idx))
			if fi, err := os.Stat(localPath); err == nil {
				item.LocalPath = localPath
				item.ModTime = fi.ModTime()
				item.Size = fi.Size()
			}
		}
		items = append(items, item)
	}
	return items
}

// commitBounded runs the existing commit path without blocking past ctx. A
// stuck stage/commit goroutine is abandoned for the dying process and the
// handle is classified from its block map afterwards.
func (bc *BlockCache) commitBounded(ctx context.Context, handle *handlemap.Handle) error {
	done := make(chan error, 1)
	log.Info("BlockCache::commitBounded : starting bounded commit of %s (handle %d)", handle.Path, handle.ID)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("commit panic: %v", r)
			}
		}()
		done <- bc.FlushFile(internal.FlushFileOptions{
			Handle:          handle,
			CloseInProgress: true,
		})
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DrainForShutdown waits out lazy releases and drives remaining dirty handles
// through stage + commit within the bounded context. Everything still dirty
// afterwards is returned as staged-only (recoverable) or in-memory-lost.
func (bc *BlockCache) DrainForShutdown(ctx context.Context) ([]internal.UncommittedItem, error) {
	bc.waitAsyncCloses(ctx)

	items := make([]internal.UncommittedItem, 0)
	dirtyCount := 0

	// Bound the wait for an abandoned commit goroutine to release the handle
	// lock. Prefer the remaining flush deadline; even under a forced (already
	// cancelled) context give the common fast-completion case a short window.
	lockWait := 2 * time.Second
	if d, ok := ctx.Deadline(); ok {
		if left := time.Until(d); left < lockWait {
			lockWait = left
		}
	}
	if ctx.Err() != nil {
		lockWait = 500 * time.Millisecond
	}

	for _, handle := range handlemap.Snapshot() {
		// Reconcile the native direct-IO dirty byte for safety; block-cache
		// writes normally flow through Go, but a cached native handle must
		// never appear clean merely because its flag was not yet promoted.
		if !handle.ReconcileDirty() {
			continue
		}
		dirtyCount++

		if ctx.Err() != nil {
			items = append(items, bc.classifyUncommitted(handle, lockWait)...)
			continue
		}

		err := bc.commitBounded(ctx, handle)
		if err != nil && handle.Dirty() {
			if !(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
				log.Err("BlockCache::DrainForShutdown : commit of %s failed [%v]", handle.Path, err)
			} else {
				log.Err("BlockCache::DrainForShutdown : commit of %s abandoned [%v]", handle.Path, err)
			}
			items = append(items, bc.classifyUncommitted(handle, lockWait)...)
		}
	}

	log.Crit("BlockCache::DrainForShutdown : %d dirty handle(s), %d uncommitted record(s)", dirtyCount, len(items))
	bc.setPendingUncommitted(items)
	bc.applyProtection(items)
	return items, nil
}

// applyProtection makes disk eviction respect staged block files that must
// survive for PutBlockList recovery, including residue from an earlier
// manifest.
func (bc *BlockCache) applyProtection(items []internal.UncommittedItem) {
	set := recovery.NewProtectedSet()
	manifestPath := recovery.ManifestPath(common.DefaultWorkDir, bc.mntPath, compName)
	if m, err := recovery.LoadManifest(manifestPath); err != nil {
		log.Err("BlockCache::applyProtection : unable to read manifest %s [%s]", manifestPath, err.Error())
	} else if m != nil {
		for _, it := range m.Items {
			set.Add(it.LocalPath)
		}
	}
	for _, it := range items {
		set.Add(it.LocalPath)
	}
	bc.pendingMu.Lock()
	bc.protectedSet = set
	bc.pendingMu.Unlock()
}

// isProtectedDiskFile checks the on-disk block file key ("path::blockindex").
func (bc *BlockCache) isProtectedDiskFile(fileKey string) bool {
	if bc.tmpPath == "" {
		return false
	}
	bc.pendingMu.Lock()
	set := bc.protectedSet
	bc.pendingMu.Unlock()
	if set == nil {
		return false
	}
	return set.IsProtected(filepath.Join(bc.tmpPath, fileKey))
}

// CacheRoot returns the disk cache directory (for manifest recording).
func (bc *BlockCache) CacheRoot() string { return bc.tmpPath }

func (bc *BlockCache) setPendingUncommitted(items []internal.UncommittedItem) {
	bc.pendingMu.Lock()
	bc.pendingUncommitted = items
	bc.pendingMu.Unlock()
}

// PendingUncommitted returns records captured by DrainForShutdown.
func (bc *BlockCache) PendingUncommitted() []internal.UncommittedItem {
	bc.pendingMu.Lock()
	defer bc.pendingMu.Unlock()
	return append([]internal.UncommittedItem(nil), bc.pendingUncommitted...)
}

// boundedStopContext bounds legacy blocking waits inside Stop.
func boundedStopContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), shutdown.DefaultComponentStopTimeout+
		shutdown.DefaultOpDrainTimeout)
}
