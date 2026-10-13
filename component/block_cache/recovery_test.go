/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |----  _____| |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2026 Microsoft Corporation. All rights reserved.
*/

package block_cache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func newRecoveryBC(t *testing.T, mockCtrl *gomock.Controller, tmpPath, policy string) (*BlockCache, *internal.MockComponent) {
	t.Helper()
	_ = log.SetDefaultLogger("silent", common.LogConfig{})
	common.DefaultWorkDir = "$HOME/.blobfuse2"
	cfg := fmt.Sprintf("block_cache:\n  path: %s\nstop:\n  recovery-policy: %s", tmpPath, policy)
	assert.NoError(t, config.ReadConfigFromReader(strings.NewReader(cfg)))

	mock := internal.NewMockComponent(mockCtrl)
	bc := &BlockCache{tmpPath: tmpPath, mntPath: "/mnt/x", blockSize: 4096}
	bc.SetName(compName)
	bc.SetNextComponent(mock)
	return bc, mock
}

func TestRecoverBlockBlobSuccess(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	tmp := t.TempDir()
	bc, mock := newRecoveryBC(t, mockCtrl, tmp, "recover")

	localBlock := filepath.Join(tmp, "b.bin::0")
	assert.NoError(t, os.WriteFile(localBlock, []byte("xxxx"), 0o644))

	mountPath := "/mnt/x"
	manifestPath := recovery.ManifestPath(common.DefaultWorkDir, mountPath, "block_cache")
	_, err := recovery.MergeItems(manifestPath, mountPath, tmp, "block_cache", "tid", []internal.UncommittedItem{
		{Component: "block_cache", Name: "b.bin", LocalPath: localBlock, Reason: internal.UncommittedStageOnly, BlockIDs: []string{"id0"}, Offset: 0},
		{Component: "block_cache", Name: "b.bin", Reason: internal.UncommittedStageOnly, BlockIDs: []string{"id1"}, Offset: 4096},
	})
	assert.NoError(t, err)

	mock.EXPECT().CommitData(gomock.Any()).DoAndReturn(func(opt internal.CommitDataOptions) error {
		assert.Equal(t, "b.bin", opt.Name)
		assert.Equal(t, []string{"id0", "id1"}, opt.List)
		assert.Equal(t, uint64(4096), opt.BlockSize)
		return nil
	}).Times(1)

	assert.NoError(t, bc.recoverOnStartup())

	_, statErr := os.Stat(localBlock)
	assert.True(t, os.IsNotExist(statErr))
	m, err := recovery.LoadManifest(manifestPath)
	assert.NoError(t, err)
	assert.Nil(t, m)
}

func TestRecoverBlockBlobFailurePreserved(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	tmp := t.TempDir()
	bc, mock := newRecoveryBC(t, mockCtrl, tmp, "recover")

	manifestPath := recovery.ManifestPath(common.DefaultWorkDir, bc.mntPath, "block_cache")
	_, err := recovery.MergeItems(manifestPath, bc.mntPath, tmp, "block_cache", "tid", []internal.UncommittedItem{
		{Component: "block_cache", Name: "c.bin", Reason: internal.UncommittedStageOnly, BlockIDs: []string{"id0"}, Offset: 0},
	})
	assert.NoError(t, err)

	mock.EXPECT().CommitData(gomock.Any()).Return(fmt.Errorf("InvalidBlockList")).Times(1)

	err = bc.recoverOnStartup()
	assert.Error(t, err)
	m, err := recovery.LoadManifest(manifestPath)
	assert.NoError(t, err)
	if assert.NotNil(t, m) {
		assert.Len(t, m.Items, 1)
	}
}

func TestRecoverInMemoryLostDropped(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	tmp := t.TempDir()
	bc, _ := newRecoveryBC(t, mockCtrl, tmp, "recover")

	manifestPath := recovery.ManifestPath(common.DefaultWorkDir, bc.mntPath, "block_cache")
	_, err := recovery.MergeItems(manifestPath, bc.mntPath, tmp, "block_cache", "tid", []internal.UncommittedItem{
		{Component: "block_cache", Name: "d.bin", Reason: internal.UncommittedInMemoryLost},
	})
	assert.NoError(t, err)

	// No CommitData expected: in-memory data cannot be recovered.
	assert.NoError(t, bc.recoverOnStartup())
	m, err := recovery.LoadManifest(manifestPath)
	assert.NoError(t, err)
	assert.Nil(t, m)
}
