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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
)

func (suite *fileCacheTestSuite) nsStoragePath(name string) string {
	return filepath.Join(suite.fake_storage_path, name)
}

func (suite *fileCacheTestSuite) nsWriteFlush(handle *handlemap.Handle, data string) {
	_, err := suite.fileCache.WriteFile(&internal.WriteFileOptions{Handle: handle, Offset: 0, Data: []byte(data)})
	suite.assert.NoError(err)
	err = suite.fileCache.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true})
	suite.assert.NoError(err)
}

// A handle kept open across a rename must flush its later writes to the new
// name; the old name must never be recreated.
func (suite *fileCacheTestSuite) TestRenameFollowedByOpenHandle() {
	defer suite.cleanupTest()

	src, dst := "ns_src1", "ns_dst1"
	handle, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: src, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(handle, "first-data")

	suite.assert.FileExists(suite.nsStoragePath(src))

	err = suite.fileCache.RenameFile(internal.RenameFileOptions{Src: src, Dst: dst})
	suite.assert.NoError(err)

	// The handle follows the object to the destination.
	suite.assert.Equal(dst, handle.Path)
	suite.assert.NoFileExists(suite.nsStoragePath(src))
	suite.assert.FileExists(suite.nsStoragePath(dst))

	// Late write after the rename lands on the destination only.
	suite.nsWriteFlush(handle, "second-data")
	suite.assert.NoFileExists(suite.nsStoragePath(src))
	d, rerr := os.ReadFile(suite.nsStoragePath(dst))
	suite.assert.NoError(rerr)
	suite.assert.Equal([]byte("second-data"), d)

	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: handle}))
	suite.assert.NoFileExists(suite.nsStoragePath(src))
}

// A delete while a dirty handle is open must not be undone by the handle's
// later flush/release.
func (suite *fileCacheTestSuite) TestDeleteWithOpenDirtyHandleDoesNotRecreate() {
	defer suite.cleanupTest()

	name := "ns_del1"
	handle, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: name, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(handle, "data-v1")
	suite.assert.FileExists(suite.nsStoragePath(name))

	// More dirty data, then delete while the handle stays open.
	_, err = suite.fileCache.WriteFile(&internal.WriteFileOptions{Handle: handle, Offset: 0, Data: []byte("data-v2")})
	suite.assert.NoError(err)

	err = suite.fileCache.DeleteFile(internal.DeleteFileOptions{Name: name})
	suite.assert.NoError(err)
	suite.assert.NoFileExists(suite.nsStoragePath(name))

	// Late flush is suppressed for the deleted object.
	err = suite.fileCache.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true})
	suite.assert.NoError(err)
	suite.assert.NoFileExists(suite.nsStoragePath(name))

	// Reads/writes on the descriptor still work locally (unlinked inode).
	_, err = suite.fileCache.WriteFile(&internal.WriteFileOptions{Handle: handle, Offset: 0, Data: []byte("data-v3")})
	suite.assert.NoError(err)
	err = suite.fileCache.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true})
	suite.assert.NoError(err)
	suite.assert.NoFileExists(suite.nsStoragePath(name))

	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: handle}))
	suite.assert.NoFileExists(suite.nsStoragePath(name))
}

// Rename over an open destination: the source handle writes to the destination
// while the replaced destination handle can no longer write anywhere; two
// writable names are never exposed.
func (suite *fileCacheTestSuite) TestRenameOverOpenDestinationSingleWritableName() {
	defer suite.cleanupTest()

	src, dst := "ns_src2", "ns_dst2"

	srcHandle, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: src, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(srcHandle, "src-content")

	dstHandle, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: dst, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(dstHandle, "dst-content")

	err = suite.fileCache.RenameFile(internal.RenameFileOptions{Src: src, Dst: dst})
	suite.assert.NoError(err)
	suite.assert.NoFileExists(suite.nsStoragePath(src))

	// Both descriptors carry the destination path now, but only the moved
	// source object is writable remotely.
	suite.assert.Equal(dst, srcHandle.Path)
	suite.assert.Equal(dst, dstHandle.Path)

	// Writes from the old destination handle must not reach storage.
	_, err = suite.fileCache.WriteFile(&internal.WriteFileOptions{Handle: dstHandle, Offset: 0, Data: []byte("XXXX")})
	suite.assert.NoError(err)
	err = suite.fileCache.FlushFile(internal.FlushFileOptions{Handle: dstHandle, CloseInProgress: true})
	suite.assert.NoError(err)
	d, rerr := os.ReadFile(suite.nsStoragePath(dst))
	suite.assert.NoError(rerr)
	suite.assert.Equal([]byte("src-content"), d)

	// Writes from the moved source handle land on the destination.
	suite.nsWriteFlush(srcHandle, "new-content")
	d, rerr = os.ReadFile(suite.nsStoragePath(dst))
	suite.assert.NoError(rerr)
	suite.assert.Equal([]byte("new-content"), d)
	suite.assert.NoFileExists(suite.nsStoragePath(src))

	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: srcHandle}))
	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: dstHandle}))
	suite.assert.NoFileExists(suite.nsStoragePath(src))
}

// Delete followed by recreate: releasing the old open handle must not delete
// or overwrite the freshly created object, thanks to the generation fence.
func (suite *fileCacheTestSuite) TestDeleteRecreateOldReleasePreservesNewObject() {
	defer suite.cleanupTest()

	name := "ns_recreate1"
	oldHandle, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: name, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(oldHandle, "old-data")

	err = suite.fileCache.DeleteFile(internal.DeleteFileOptions{Name: name})
	suite.assert.NoError(err)
	suite.assert.NoFileExists(suite.nsStoragePath(name))

	// Recreate the name with a new object and a new handle.
	newHandle, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: name, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(newHandle, "new-data")

	// Old handle goes away after the new object exists.
	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: oldHandle}))
	d, rerr := os.ReadFile(suite.nsStoragePath(name))
	suite.assert.NoError(rerr)
	suite.assert.Equal([]byte("new-data"), d, "old handle release must not touch the recreated object")

	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: newHandle}))
	d, rerr = os.ReadFile(suite.nsStoragePath(name))
	suite.assert.NoError(rerr)
	suite.assert.Equal([]byte("new-data"), d)
}

// Definitive error codes for uncached names are preserved by the fast path.
func (suite *fileCacheTestSuite) TestFastPathErrorCodesPreserved() {
	defer suite.cleanupTest()

	err := suite.fileCache.RenameFile(internal.RenameFileOptions{Src: "ns_missing_src", Dst: "ns_missing_dst"})
	suite.assert.Error(err)
	suite.assert.True(os.IsNotExist(err), "expected ENOENT, got %v", err)

	err = suite.fileCache.DeleteFile(internal.DeleteFileOptions{Name: "ns_missing_del"})
	suite.assert.Error(err)
	suite.assert.True(os.IsNotExist(err), "expected ENOENT, got %v", err)
}

// ---------------------------------------------------------------------------
// Uncertain remote response reconciliation with a fault-injecting backend
// ---------------------------------------------------------------------------

// flakyComponent wraps a backend and can make the next rename/delete return an
// uncertain error after optionally applying the real operation.
type flakyComponent struct {
	internal.Component
	mu sync.Mutex

	renameApplied bool
	renameErr     error
	renameCalls   int32

	deleteApplied bool
	deleteErr     error
	deleteCalls   int32
}

func (f *flakyComponent) RenameFile(options internal.RenameFileOptions) error {
	atomic.AddInt32(&f.renameCalls, 1)
	f.mu.Lock()
	apply, injected := f.renameApplied, f.renameErr
	f.renameApplied, f.renameErr = false, nil
	f.mu.Unlock()

	var baseErr error
	if apply {
		baseErr = f.Component.RenameFile(options)
	}
	if injected != nil {
		return injected
	}
	return baseErr
}

func (f *flakyComponent) DeleteFile(options internal.DeleteFileOptions) error {
	atomic.AddInt32(&f.deleteCalls, 1)
	f.mu.Lock()
	apply, injected := f.deleteApplied, f.deleteErr
	f.deleteApplied, f.deleteErr = false, nil
	f.mu.Unlock()

	var baseErr error
	if apply {
		baseErr = f.Component.DeleteFile(options)
	}
	if injected != nil {
		return injected
	}
	return baseErr
}

// newFileCacheWithFlakyBackend builds an independent loopback-backed file
// cache wrapped by the flaky component.
func (suite *fileCacheTestSuite) newFileCacheWithFlakyBackend() (*FileCache, *flakyComponent, string, func()) {
	randStr := randomString(8)
	cachePath := filepath.Join(home_dir, "file_cache"+randStr)
	storagePath := filepath.Join(home_dir, "fake_storage"+randStr)
	suite.assert.NoError(os.MkdirAll(cachePath, 0777))
	suite.assert.NoError(os.MkdirAll(storagePath, 0777))

	cfg := fmt.Sprintf("file_cache:\n  path: %s\n  offload-io: true\n  timeout-sec: 0\n\nloopbackfs:\n  path: %s", cachePath, storagePath)
	prev := suite.configString
	suite.assert.NoError(config.ReadConfigFromReader(strings.NewReader(cfg)))

	loop := newLoopbackFS()
	flaky := &flakyComponent{Component: loop}

	comp := NewFileCacheComponent()
	comp.SetNextComponent(flaky)
	suite.assert.NoError(comp.Configure(true))
	suite.assert.NoError(config.ReadConfigFromReader(strings.NewReader(prev)))

	fc := comp.(*FileCache)
	suite.assert.NoError(loop.Start(context.Background()))
	suite.assert.NoError(fc.Start(context.Background()))

	cleanup := func() {
		_ = fc.Stop()
		_ = loop.Stop()
		_ = os.RemoveAll(cachePath)
		_ = os.RemoveAll(storagePath)
	}
	return fc, flaky, storagePath, cleanup
}

func (f *flakyComponent) armRename(applied bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renameApplied = applied
	f.renameErr = err
}

func (f *flakyComponent) armDelete(applied bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteApplied = applied
	f.deleteErr = err
}

// Rename that actually happened remotely despite an uncertain response:
// reconciliation probes src/dst, commits the mapping and the handle follows.
func (suite *fileCacheTestSuite) TestUncertainRenameActuallyCommitted() {
	fc, flaky, storagePath, cleanup := suite.newFileCacheWithFlakyBackend()
	defer cleanup()

	src, dst := "u_src1", "u_dst1"
	handle, err := fc.CreateFile(internal.CreateFileOptions{Name: src, Mode: 0666})
	suite.assert.NoError(err)
	_, err = fc.WriteFile(&internal.WriteFileOptions{Handle: handle, Offset: 0, Data: []byte("data")})
	suite.assert.NoError(err)
	suite.assert.NoError(fc.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true}))

	// Backend performs the rename but the response looks like a timeout.
	flaky.armRename(true, context.DeadlineExceeded)
	err = fc.RenameFile(internal.RenameFileOptions{Src: src, Dst: dst})
	suite.assert.NoError(err, "rename should be confirmed by state probe")
	suite.assert.Equal(dst, handle.Path)
	suite.assert.NoFileExists(filepath.Join(storagePath, src))
	suite.assert.FileExists(filepath.Join(storagePath, dst))

	suite.assert.NoError(fc.ReleaseFile(internal.ReleaseFileOptions{Handle: handle}))
}

// Rename that never reached the backend: reconciliation rolls the local state
// back, returns the original error, and later writes still target the source.
func (suite *fileCacheTestSuite) TestUncertainRenameRolledBack() {
	fc, flaky, storagePath, cleanup := suite.newFileCacheWithFlakyBackend()
	defer cleanup()

	src, dst := "u_src2", "u_dst2"
	handle, err := fc.CreateFile(internal.CreateFileOptions{Name: src, Mode: 0666})
	suite.assert.NoError(err)
	suite.assert.NoError(fc.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true}))

	flaky.armRename(false, context.DeadlineExceeded)
	err = fc.RenameFile(internal.RenameFileOptions{Src: src, Dst: dst})
	suite.assert.ErrorIs(err, context.DeadlineExceeded)
	suite.assert.Equal(src, handle.Path)
	suite.assert.FileExists(filepath.Join(storagePath, src))
	suite.assert.NoFileExists(filepath.Join(storagePath, dst))

	// Handle remains fully writable at the source.
	_, err = fc.WriteFile(&internal.WriteFileOptions{Handle: handle, Offset: 0, Data: []byte("still-src")})
	suite.assert.NoError(err)
	suite.assert.NoError(fc.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true}))
	suite.assert.FileExists(filepath.Join(storagePath, src))
	suite.assert.NoFileExists(filepath.Join(storagePath, dst))
	suite.assert.NoError(fc.ReleaseFile(internal.ReleaseFileOptions{Handle: handle}))
}

// Delete that actually happened despite an uncertain response is confirmed by
// probing absence; the open handle is fenced.
func (suite *fileCacheTestSuite) TestUncertainDeleteActuallyCommitted() {
	fc, flaky, storagePath, cleanup := suite.newFileCacheWithFlakyBackend()
	defer cleanup()

	name := "u_del1"
	handle, err := fc.CreateFile(internal.CreateFileOptions{Name: name, Mode: 0666})
	suite.assert.NoError(err)
	_, err = fc.WriteFile(&internal.WriteFileOptions{Handle: handle, Offset: 0, Data: []byte("data")})
	suite.assert.NoError(err)
	suite.assert.NoError(fc.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true}))

	flaky.armDelete(true, context.DeadlineExceeded)
	suite.assert.NoError(fc.DeleteFile(internal.DeleteFileOptions{Name: name}))
	suite.assert.NoFileExists(filepath.Join(storagePath, name))

	suite.assert.NoError(fc.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true}))
	suite.assert.NoFileExists(filepath.Join(storagePath, name))
	suite.assert.NoError(fc.ReleaseFile(internal.ReleaseFileOptions{Handle: handle}))
}

// Delete that never reached the backend returns its error and leaves both the
// object and the handle usable.
func (suite *fileCacheTestSuite) TestUncertainDeleteRolledBack() {
	fc, flaky, storagePath, cleanup := suite.newFileCacheWithFlakyBackend()
	defer cleanup()

	name := "u_del2"
	handle, err := fc.CreateFile(internal.CreateFileOptions{Name: name, Mode: 0666})
	suite.assert.NoError(err)
	_, err = fc.WriteFile(&internal.WriteFileOptions{Handle: handle, Offset: 0, Data: []byte("data")})
	suite.assert.NoError(err)
	suite.assert.NoError(fc.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true}))

	flaky.armDelete(false, context.DeadlineExceeded)
	err = fc.DeleteFile(internal.DeleteFileOptions{Name: name})
	suite.assert.ErrorIs(err, context.DeadlineExceeded)
	suite.assert.FileExists(filepath.Join(storagePath, name))

	_, err = fc.WriteFile(&internal.WriteFileOptions{Handle: handle, Offset: 0, Data: []byte("still")})
	suite.assert.NoError(err)
	suite.assert.NoError(fc.FlushFile(internal.FlushFileOptions{Handle: handle, CloseInProgress: true}))
	suite.assert.FileExists(filepath.Join(storagePath, name))
	suite.assert.NoError(fc.ReleaseFile(internal.ReleaseFileOptions{Handle: handle}))
}

// Concurrent renames in opposite directions with open handles must not
// deadlock (stable lock ordering), and every write-back must land on an
// existing name.
func (suite *fileCacheTestSuite) TestConcurrentOppositeRenamesDoNotDeadlock() {
	defer suite.cleanupTest()

	a, b := "ns_a", "ns_b"
	ha, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: a, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(ha, "a-data")
	hb, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: b, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(hb, "b-data")

	const rounds = 50
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if i%2 == 0 {
				if e := suite.fileCache.RenameFile(internal.RenameFileOptions{Src: a, Dst: b}); e != nil {
					errCh <- e
					return
				}
			} else {
				if e := suite.fileCache.RenameFile(internal.RenameFileOptions{Src: b, Dst: a}); e != nil {
					errCh <- e
					return
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if i%2 == 1 {
				if e := suite.fileCache.RenameFile(internal.RenameFileOptions{Src: a, Dst: b}); e != nil {
					errCh <- e
					return
				}
			} else {
				if e := suite.fileCache.RenameFile(internal.RenameFileOptions{Src: b, Dst: a}); e != nil {
					errCh <- e
					return
				}
			}
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		suite.T().Fatal("opposite renames deadlocked")
	}
	close(errCh)
	for e := range errCh {
		// Cross-round interleaving can make a swap race with the other
		// goroutine's swap (src already moved): a benign application-level
		// ENOENT. The point of this test is stable lock ordering, i.e. no
		// deadlock.
		if !os.IsNotExist(e) {
			suite.assert.Failf("rename error", "%v", e)
		}
	}

	// Both handles resolve to whichever name currently holds their object;
	// flush must succeed on the resolved name.
	for _, h := range []**handlemap.Handle{&ha, &hb} {
		suite.assert.NotEmpty((*h).Path)
		suite.assert.NoError(suite.fileCache.FlushFile(internal.FlushFileOptions{Handle: *h, CloseInProgress: true}))
	}
	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: ha}))
	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: hb}))
}

// Rename swap racing with dirty flushes: every write-back must serialize
// against the rename commit and land on the object's current name. At the end
// of the x->y->x ping-pong the moved handle owns x and exactly one name exists.
func (suite *fileCacheTestSuite) TestConcurrentRenameSwapAndFlush() {
	defer suite.cleanupTest()

	x, y := "ns_swapx", "ns_swapy"
	hX, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: x, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(hX, "xxxxxxxx") // 8 bytes
	hY, err := suite.fileCache.CreateFile(internal.CreateFileOptions{Name: y, Mode: 0666})
	suite.assert.NoError(err)
	suite.nsWriteFlush(hY, "yyyyyyyy")

	const iters = 100
	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	wg.Add(2)

	// Renamer: x->y then y->x per iteration.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if e := suite.fileCache.RenameFile(internal.RenameFileOptions{Src: x, Dst: y}); e != nil && !os.IsNotExist(e) {
				errCh <- e
				return
			}
			if e := suite.fileCache.RenameFile(internal.RenameFileOptions{Src: y, Dst: x}); e != nil && !os.IsNotExist(e) {
				errCh <- e
				return
			}
		}
	}()

	// Flusher: keep writing fixed-length markers to the live handle; the
	// orphaned handle must never error or write back.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			data := []byte(fmt.Sprintf("%08d", i))
			if _, e := suite.fileCache.WriteFile(&internal.WriteFileOptions{Handle: hX, Offset: 0, Data: data}); e != nil {
				errCh <- e
				return
			}
			if e := suite.fileCache.FlushFile(internal.FlushFileOptions{Handle: hX, CloseInProgress: true}); e != nil {
				errCh <- e
				return
			}
			// hY becomes the replaced object after the first x->y; its
			// write-back is suppressed but flush returns nil.
			if _, e := suite.fileCache.WriteFile(&internal.WriteFileOptions{Handle: hY, Offset: 0, Data: []byte("zzzzzzzz")}); e != nil {
				errCh <- e
				return
			}
			if e := suite.fileCache.FlushFile(internal.FlushFileOptions{Handle: hY, CloseInProgress: true}); e != nil {
				errCh <- e
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		suite.T().Fatal("rename/flush workload deadlocked")
	}
	close(errCh)
	for e := range errCh {
		suite.assert.Failf("rename/flush error", "%v", e)
	}

	suite.assert.Equal(x, hX.Path)
	// Exactly one name remains, and it carries one of the flushed markers.
	_, xErr := os.Stat(suite.nsStoragePath(x))
	_, yErr := os.Stat(suite.nsStoragePath(y))
	suite.assert.NoError(xErr, "x must exist after the ping-pong")
	suite.assert.True(os.IsNotExist(yErr), "y must not exist after the ping-pong")
	d, rerr := os.ReadFile(suite.nsStoragePath(x))
	suite.assert.NoError(rerr)
	suite.assert.Len(d, 8)

	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: hX}))
	suite.assert.NoError(suite.fileCache.ReleaseFile(internal.ReleaseFileOptions{Handle: hY}))
}
