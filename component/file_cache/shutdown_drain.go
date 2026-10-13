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

package file_cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// waitAsyncCloses waits for lazy-write background close/upload goroutines to
// finish. The wait is bounded by ctx: on timeout/freeze the method returns
// false and the abandoned goroutines are accounted as uncommitted data
// instead of keeping unmount hanging forever.
func (fc *FileCache) waitAsyncCloses(ctx context.Context) bool {
	if !fc.lazyWrite {
		return true
	}
	log.Info("FileCache::waitAsyncCloses : Waiting for async close to complete")

	done := make(chan struct{})
	go func() {
		fc.fileCloseOpt.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-ctx.Done():
		log.Err("FileCache::waitAsyncCloses : timed out waiting for async closes [%v]", ctx.Err())
		return false
	}
}

// uncommittedItem builds the protected record for a still-dirty handle.
func (fc *FileCache) uncommittedItem(handle *handlemap.Handle, reason string) internal.UncommittedItem {
	localPath := filepath.Join(fc.tmpPath, handle.Path)
	item := internal.UncommittedItem{
		Component: fc.Name(),
		Name:      handle.Path,
		LocalPath: localPath,
		Reason:    reason,
	}
	if fi, err := os.Stat(localPath); err == nil {
		item.Size = fi.Size()
		item.ModTime = fi.ModTime()
	}
	return item
}

// flushHandleBounded runs the existing FlushFile upload path but never waits
// past ctx. A blocked upload goroutine is left behind for the dying process;
// its local file is reported uncommitted and preserved.
func flushHandleBounded(ctx context.Context, fc *FileCache, handle *handlemap.Handle) error {
	type res struct{ err error }
	done := make(chan res, 1)
	log.Info("FileCache::flushHandleBounded : starting bounded flush of %s (handle %d)", handle.Path, handle.ID)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- res{err: fmt.Errorf("flush panic: %v", r)}
			}
		}()
		done <- res{err: fc.FlushFile(internal.FlushFileOptions{
			Handle:          handle,
			CloseInProgress: true,
		})}
	}()

	select {
	case r := <-done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DrainForShutdown drives every dirty handle known at the freeze instant
// through the regular upload path and reports whatever could not be confirmed
// committed. It implements internal.ShutdownDrainer.
func (fc *FileCache) DrainForShutdown(ctx context.Context) ([]internal.UncommittedItem, error) {
	fc.waitAsyncCloses(ctx)

	forced := errors.Is(ctx.Err(), context.Canceled)
	items := make([]internal.UncommittedItem, 0)
	dirtyCount := 0

	for _, handle := range handlemap.Snapshot() {
		// Reconcile the native direct-IO dirty byte: cached writes can run
		// entirely in C without ever promoting the Go dirty flag, and an open
		// handle killed mid-write would otherwise look clean here.
		if !handle.ReconcileDirty() {
			continue
		}
		dirtyCount++

		// Best effort: push kernel buffered writes of the open fd to disk so
		// the preserved cache file is complete even if the upload is skipped.
		if f := handle.GetFileObject(); f != nil {
			_ = f.Sync()
		}

		reason := internal.UncommittedFlushTimeout
		if forced || errors.Is(ctx.Err(), context.Canceled) {
			reason = internal.UncommittedForcedSignal
		}

		if ctx.Err() != nil {
			items = append(items, fc.uncommittedItem(handle, reason))
			continue
		}

		err := flushHandleBounded(ctx, fc, handle)
		if err != nil {
			if !handle.Dirty() {
				// Another flusher/concurrent flush committed; nothing to report.
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				log.Err("FileCache::DrainForShutdown : flush of %s abandoned [%v]", handle.Path, err)
				if errors.Is(err, context.Canceled) {
					// The second (forced) signal arrived while this flush was
					// already running, after the forced snapshot above.
					reason = internal.UncommittedForcedSignal
				}
			} else {
				log.Err("FileCache::DrainForShutdown : flush of %s failed [%v]", handle.Path, err)
				if !forced {
					reason = internal.UncommittedUploadFailed
				}
			}
			items = append(items, fc.uncommittedItem(handle, reason))
		}
	}

	log.Crit("FileCache::DrainForShutdown : %d dirty handle(s), %d uncommitted", dirtyCount, len(items))
	fc.setPendingUncommitted(items)

	// Forbid LRU eviction and temp cleanup from touching residue, including a
	// recovery manifest left by an earlier unclean shutdown.
	if protected := fc.buildProtectedSet(items); protected.Len() > 0 && fc.policy != nil {
		fc.policy.SetProtectedCheck(protected.IsProtected)
	}
	return items, nil
}

// buildProtectedSet merges the just-detected uncommitted files with any
// recovery manifest already on disk (e.g. previous crash, or a read-only mount
// that preserved an earlier failure).
func (fc *FileCache) buildProtectedSet(items []internal.UncommittedItem) *recovery.ProtectedSet {
	set := recovery.NewProtectedSet()
	manifestPath := recovery.ManifestPath(common.DefaultWorkDir, fc.mountPath, compName)
	if m, err := recovery.LoadManifest(manifestPath); err != nil {
		log.Err("FileCache::buildProtectedSet : unable to read existing manifest %s [%s]",
			manifestPath, err.Error())
	} else if m != nil {
		for _, it := range m.Items {
			set.Add(it.LocalPath)
		}
	}
	for _, it := range items {
		set.Add(it.LocalPath)
	}
	return set
}

func (fc *FileCache) setPendingUncommitted(items []internal.UncommittedItem) {
	fc.pendingMu.Lock()
	fc.pendingUncommitted = items
	fc.pendingMu.Unlock()
}

// PendingUncommitted returns the uncommitted records captured during
// DrainForShutdown (nil if never drained). Used by cache cleanup to protect
// files and by recovery/manifest wiring.
func (fc *FileCache) PendingUncommitted() []internal.UncommittedItem {
	fc.pendingMu.Lock()
	defer fc.pendingMu.Unlock()
	return append([]internal.UncommittedItem(nil), fc.pendingUncommitted...)
}

// CacheRoot returns the local cache directory (for manifest recording).
func (fc *FileCache) CacheRoot() string { return fc.tmpPath }

// boundedStopContext bounds legacy blocking waits inside Stop when no explicit
// shutdown context is propagated through the fixed Component.Stop signature.
func boundedStopContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), shutdown.DefaultComponentStopTimeout+
		shutdown.DefaultOpDrainTimeout)
}
