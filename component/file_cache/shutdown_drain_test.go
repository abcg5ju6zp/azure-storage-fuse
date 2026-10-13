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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func silentLoggerForDrainTest(t *testing.T) {
	t.Helper()
	_ = log.SetDefaultLogger("silent", common.LogConfig{Level: common.ELogLevel.LOG_DEBUG()})

	// Global handlemap isolation: earlier tests may leave dirty handles.
	saved := map[*handlemap.Handle]bool{}
	for _, h := range handlemap.Snapshot() {
		if h.Dirty() {
			saved[h] = true
			h.Flags.Clear(handlemap.HandleFlagDirty)
		}
	}
	t.Cleanup(func() {
		for h := range saved {
			h.Flags.Set(handlemap.HandleFlagDirty)
		}
	})
}

func newDrainTestCache(t *testing.T, mockCtrl *gomock.Controller, lazy bool) (*FileCache, *internal.MockComponent, string, func()) {
	t.Helper()
	cachePath := t.TempDir()
	cfg := fmt.Sprintf("file_cache:\n  path: %s\n  offload-io: true\n  timeout-sec: 0\nlazy-write: %t",
		cachePath, lazy)
	if err := config.ReadConfigFromReader(strings.NewReader(cfg)); err != nil {
		t.Fatal(err)
	}

	mockComponent := internal.NewMockComponent(mockCtrl)
	fcComp := NewFileCacheComponent()
	fcComp.SetNextComponent(mockComponent)
	if err := fcComp.Configure(true); err != nil {
		t.Fatal(err)
	}
	mockComponent.EXPECT().Start(gomock.Any()).Return(nil).AnyTimes()
	mockComponent.EXPECT().Stop().Return(nil).AnyTimes()
	if err := mockComponent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fcComp.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	fc := fcComp.(*FileCache)

	cleanup := func() {
		_ = fc.Stop()
		_ = mockComponent.Stop()
		_ = os.RemoveAll(cachePath)
	}
	return fc, mockComponent, cachePath, cleanup
}

func dirtyHandleWithFile(t *testing.T, cachePath, name, content string) (*handlemap.Handle, string) {
	t.Helper()
	localPath := filepath.Join(cachePath, name)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(localPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	h := handlemap.NewHandle(name)
	h.SetFileObject(f)
	h.Flags.Set(handlemap.HandleFlagDirty)
	id := handlemap.Add(h)
	t.Cleanup(func() { handlemap.Delete(id) })
	return h, localPath
}

func TestDrainForShutdownFlushTimeoutPreservesFile(t *testing.T) {
	silentLoggerForDrainTest(t)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	fc, mock, cachePath, cleanup := newDrainTestCache(t, mockCtrl, false)
	defer cleanup()

	h, localPath := dirtyHandleWithFile(t, cachePath, "dir/uncommitted.txt", "pending bytes")
	fc.fileLocks.Get(h.Path).Inc() // simulate open handle count

	mock.EXPECT().CopyFromFile(gomock.Any()).DoAndReturn(func(internal.CopyFromFileOptions) error {
		time.Sleep(400 * time.Millisecond)
		return nil
	}).AnyTimes()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	items, err := fc.DrainForShutdown(ctx)
	elapsed := time.Since(start)
	assert.NoError(t, err)
	assert.Len(t, items, 1)
	assert.True(t, elapsed < 300*time.Millisecond, "drain blocked past deadline: %v", elapsed)

	assert.Equal(t, internal.UncommittedFlushTimeout, items[0].Reason)
	assert.Equal(t, h.Path, items[0].Name)
	assert.Equal(t, localPath, items[0].LocalPath)
	assert.True(t, h.Dirty(), "dirty flag must remain set on timeout")

	if fi, statErr := os.Stat(localPath); assert.NoError(t, statErr) {
		assert.Equal(t, int64(len("pending bytes")), fi.Size())
	}

	// Stop must not delete the preserved file and must surface the condition.
	stopErr := fc.Stop()
	assert.Error(t, stopErr)
	_, stopStatErr := os.Stat(localPath)
	assert.NoError(t, stopStatErr, "uncommitted cache file deleted by Stop")
}

func TestDrainForShutdownSuccess(t *testing.T) {
	silentLoggerForDrainTest(t)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	fc, mock, cachePath, cleanup := newDrainTestCache(t, mockCtrl, false)
	defer cleanup()

	h, _ := dirtyHandleWithFile(t, cachePath, "clean.txt", "committed bytes")
	fc.fileLocks.Get(h.Path).Inc()

	mock.EXPECT().CopyFromFile(gomock.Any()).Return(nil).Times(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	items, err := fc.DrainForShutdown(ctx)
	assert.NoError(t, err)
	assert.Empty(t, items)
	assert.False(t, h.Dirty())
}

func TestDrainForShutdownForcedSkipsUpload(t *testing.T) {
	silentLoggerForDrainTest(t)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	fc, _, cachePath, cleanup := newDrainTestCache(t, mockCtrl, false)
	defer cleanup()

	h, _ := dirtyHandleWithFile(t, cachePath, "forced.txt", "x")
	fc.fileLocks.Get(h.Path).Inc()

	// No CopyFromFile expectation: forced drain must never reach the next
	// component.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	items, err := fc.DrainForShutdown(ctx)
	assert.NoError(t, err)
	if assert.Len(t, items, 1) {
		assert.Equal(t, internal.UncommittedForcedSignal, items[0].Reason)
	}
}

func TestDrainForShutdownUploadFailureReported(t *testing.T) {
	silentLoggerForDrainTest(t)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	fc, mock, cachePath, cleanup := newDrainTestCache(t, mockCtrl, false)
	defer cleanup()

	h, localPath := dirtyHandleWithFile(t, cachePath, "failed.txt", "lost bytes")
	fc.fileLocks.Get(h.Path).Inc()

	mock.EXPECT().CopyFromFile(gomock.Any()).Return(fmt.Errorf("storage unavailable")).Times(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	items, err := fc.DrainForShutdown(ctx)
	assert.NoError(t, err)
	if assert.Len(t, items, 1) {
		assert.Equal(t, internal.UncommittedUploadFailed, items[0].Reason)
		assert.Equal(t, localPath, items[0].LocalPath)
	}
	assert.True(t, h.Dirty())
}

func TestWaitAsyncClosesBounded(t *testing.T) {
	silentLoggerForDrainTest(t)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	fc, _, _, cleanup := newDrainTestCache(t, mockCtrl, true)
	defer cleanup()

	fc.fileCloseOpt.Add(1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		fc.fileCloseOpt.Done()
	}()
	ok := fc.waitAsyncCloses(context.Background())
	assert.True(t, ok)

	fc.fileCloseOpt.Add(1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		fc.fileCloseOpt.Done()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	ok = fc.waitAsyncCloses(ctx)
	assert.False(t, ok, "waitAsyncCloses must return false on timeout")
}

// TestDrainForShutdownReconcilesNativeDirtyByte covers the direct-IO window:
// cached native pwrite marks only the C file_handle_t byte while the Go handle
// flag is still clear (promotion normally happens at flush/release). Shutdown
// drain must consult the registered native check, promote the flag and upload
// the file instead of reporting a false "clean".
func TestDrainForShutdownReconcilesNativeDirtyByte(t *testing.T) {
	silentLoggerForDrainTest(t)
	handlemap.ResetGateForTest()
	t.Cleanup(handlemap.ResetGateForTest)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	fc, mock, cachePath, cleanup := newDrainTestCache(t, mockCtrl, false)
	defer cleanup()

	// Direct-IO dirty handle: Go flag deliberately left clear.
	dirtyPath := filepath.Join(cachePath, "native-dirty.bin")
	if err := os.MkdirAll(filepath.Dir(dirtyPath), 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(dirtyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("native written bytes"); err != nil {
		t.Fatal(err)
	}
	hDirty := handlemap.NewHandle("native-dirty.bin")
	hDirty.SetFileObject(f)
	const fakeNativeFH = uintptr(0xabcdef123456)
	hDirty.SetNativeFH(fakeNativeFH)
	idDirty := handlemap.Add(hDirty)
	t.Cleanup(func() { handlemap.Delete(idDirty) })
	fc.fileLocks.Get(hDirty.Path).Inc()

	// Clean handle with a native FH: must be skipped entirely.
	hClean := handlemap.NewHandle("native-clean.bin")
	hClean.SetNativeFH(0x999)
	idClean := handlemap.Add(hClean)
	t.Cleanup(func() { handlemap.Delete(idClean) })

	consulted := 0
	handlemap.RegisterNativeDirtyCheck(func(fh uintptr) bool {
		consulted++
		return fh == fakeNativeFH
	})

	promotedDuringUpload := false
	mock.EXPECT().CopyFromFile(gomock.Any()).DoAndReturn(func(opts internal.CopyFromFileOptions) error {
		assert.Equal(t, hDirty.Path, opts.Name)
		promotedDuringUpload = hDirty.Dirty()
		return nil
	}).Times(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	items, err := fc.DrainForShutdown(ctx)
	assert.NoError(t, err)
	assert.Empty(t, items, "reconciled direct-IO flush must complete, not be reported uncommitted")
	assert.True(t, promotedDuringUpload, "Go dirty flag must be promoted before FlushFile upload")
	assert.False(t, hDirty.Dirty(), "successful flush clears the promoted flag")
	assert.Greater(t, consulted, 0, "native dirty check was never consulted")
}
