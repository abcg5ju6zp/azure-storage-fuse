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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"

	"github.com/stretchr/testify/assert"
)

func newTestBlockCache(t *testing.T) *BlockCache {
	t.Helper()
	_ = log.SetDefaultLogger("silent", common.LogConfig{Level: common.ELogLevel.LOG_DEBUG()})
	bc := &BlockCache{}
	bc.SetName(compName)
	return bc
}

// testBlockInfo builds a blockInfo with the atomic state flags used by the
// shutdown classification paths.
func testBlockInfo(id string, size uint64, isStaged, isCommitted bool) *blockInfo {
	info := &blockInfo{id: id, size: size}
	info.staged.Store(isStaged)
	info.committed.Store(isCommitted)
	return info
}

func TestClassifyUncommittedStagedOnly(t *testing.T) {
	bc := newTestBlockCache(t)
	bc.tmpPath = t.TempDir()

	h := handlemap.NewHandle("blob.bin")
	h.Size = 4096
	listMap := map[int64]*blockInfo{
		2: testBlockInfo("block-id-2", 100, true, false),
		1: testBlockInfo("block-id-1", 200, false, false),
		0: testBlockInfo("block-id-0", 50, true, true),
	}
	h.SetValue("blockList", listMap)

	// Local copy exists for the recoverable block.
	localFile := filepath.Join(bc.tmpPath, "blob.bin::2")
	assert.NoError(t, os.MkdirAll(filepath.Dir(localFile), 0755))
	assert.NoError(t, os.WriteFile(localFile, []byte("data"), 0644))

	items := bc.classifyUncommitted(h, time.Second)
	if assert.Len(t, items, 1) {
		assert.Equal(t, internal.UncommittedStageOnly, items[0].Reason)
		assert.Equal(t, "blob.bin", items[0].Name)
		assert.Equal(t, []string{"block-id-2"}, items[0].BlockIDs)
		assert.Equal(t, localFile, items[0].LocalPath)
		assert.Equal(t, int64(4), items[0].Size)
	}
}

func TestClassifyUncommittedInMemoryLost(t *testing.T) {
	bc := newTestBlockCache(t)
	h := handlemap.NewHandle("fresh.bin")
	h.Size = 10
	h.SetValue("blockList", map[int64]*blockInfo{
		0: testBlockInfo("never-staged", 0, false, false),
	})

	items := bc.classifyUncommitted(h, time.Second)
	if assert.Len(t, items, 1) {
		assert.Equal(t, internal.UncommittedInMemoryLost, items[0].Reason)
		assert.Empty(t, items[0].LocalPath)
		assert.Empty(t, items[0].BlockIDs)
	}
}

// isolateDirtyHandles clears the dirty flag on any handles already in the
// global handle map (left by earlier tests) and restores them on cleanup, so
// DrainForShutdown enumerates only this test's handles.
func isolateDirtyHandles(t *testing.T) {
	t.Helper()
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

func TestDrainForShutdownForcedClassifies(t *testing.T) {
	bc := newTestBlockCache(t)
	bc.tmpPath = t.TempDir()
	isolateDirtyHandles(t)

	staged := handlemap.NewHandle("a.bin")
	staged.Flags.Set(handlemap.HandleFlagDirty)
	staged.SetValue("blockList", map[int64]*blockInfo{
		0: testBlockInfo("sid", 5, true, false),
	})
	handlemap.Add(staged)

	memOnly := handlemap.NewHandle("b.bin")
	memOnly.Flags.Set(handlemap.HandleFlagDirty)
	handlemap.Add(memOnly)

	clean := handlemap.NewHandle("c.bin")
	clean.Flags.Clear(handlemap.HandleFlagDirty)
	handlemap.Add(clean)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	items, err := bc.DrainForShutdown(ctx)
	assert.NoError(t, err)

	reasons := map[string]int{}
	for _, it := range items {
		reasons[it.Reason]++
	}
	assert.Equal(t, 1, reasons[internal.UncommittedStageOnly])
	assert.Equal(t, 1, reasons[internal.UncommittedInMemoryLost])
	assert.Len(t, bc.PendingUncommitted(), 2)
}

func TestBlockWaitAsyncClosesBounded(t *testing.T) {
	bc := newTestBlockCache(t)
	bc.lazyWrite = true

	bc.fileCloseOpt.Add(1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		bc.fileCloseOpt.Done()
	}()
	assert.True(t, bc.waitAsyncCloses(context.Background()))

	bc.fileCloseOpt.Add(1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		bc.fileCloseOpt.Done()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	assert.False(t, bc.waitAsyncCloses(ctx))
}

func TestStagedBlocksFilter(t *testing.T) {
	h := handlemap.NewHandle("x")
	assert.Empty(t, stagedBlocks(h, time.Second))

	h.SetValue("blockList", map[int64]*blockInfo{
		1: testBlockInfo("staged-uncommitted", 0, true, false),
		2: testBlockInfo("staged-committed", 0, true, true),
		3: testBlockInfo("not-staged", 0, false, false),
	})
	got := stagedBlocks(h, time.Second)
	if assert.Len(t, got, 1) {
		assert.Equal(t, "staged-uncommitted", got[1].id)
	}
}
