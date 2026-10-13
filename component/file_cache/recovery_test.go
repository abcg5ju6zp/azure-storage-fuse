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

package file_cache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func newRecoveryFC(t *testing.T, mockCtrl *gomock.Controller, cachePath, policy string) (*FileCache, *internal.MockComponent) {
	t.Helper()
	common.DefaultWorkDir = "$HOME/.blobfuse2"
	cfg := fmt.Sprintf("file_cache:\n  path: %s\n  timeout-sec: 0\n  allow-non-empty-temp: true\nstop:\n  recovery-policy: %s",
		cachePath, policy)
	assert.NoError(t, config.ReadConfigFromReader(strings.NewReader(cfg)))

	mock := internal.NewMockComponent(mockCtrl)
	comp := NewFileCacheComponent()
	comp.SetNextComponent(mock)
	assert.NoError(t, comp.Configure(true))
	fc := comp.(*FileCache)
	return fc, mock
}

func writeFileCacheManifest(t *testing.T, mountPath, cachePath, reason string) string {
	t.Helper()
	common.DefaultWorkDir = "$HOME/.blobfuse2"
	manifestPath := recovery.ManifestPath(common.DefaultWorkDir, mountPath, "file_cache")
	_, err := recovery.MergeItems(manifestPath, mountPath, cachePath, "file_cache", "testid",
		[]internal.UncommittedItem{{
			Component: "file_cache",
			Name:      "a.txt",
			LocalPath: filepath.Join(cachePath, "a.txt"),
			Size:      4,
			Reason:    reason,
		}})
	assert.NoError(t, err)
	return manifestPath
}

func TestRecoverOnStartupUploadSuccess(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	cachePath := t.TempDir()
	localFile := filepath.Join(cachePath, "a.txt")
	assert.NoError(t, os.WriteFile(localFile, []byte("data"), 0o644))

	mountPath := "/mnt/recover-ok"
	manifestPath := writeFileCacheManifest(t, mountPath, cachePath, internal.UncommittedFlushTimeout)

	fc, mock := newRecoveryFC(t, mockCtrl, cachePath, "recover")
	fc.mountPath = mountPath
	mock.EXPECT().CopyFromFile(gomock.Any()).Return(nil).Times(1)

	assert.NoError(t, fc.recoverOnStartup())

	// Local copy deleted after confirmed upload; manifest removed.
	_, err := os.Stat(localFile)
	assert.True(t, os.IsNotExist(err))
	m, err := recovery.LoadManifest(manifestPath)
	assert.NoError(t, err)
	assert.Nil(t, m)
}

func TestRecoverOnStartupFailureRecoverPolicy(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	cachePath := t.TempDir()
	localFile := filepath.Join(cachePath, "a.txt")
	assert.NoError(t, os.WriteFile(localFile, []byte("data"), 0o644))

	mountPath := "/mnt/recover-fail"
	manifestPath := writeFileCacheManifest(t, mountPath, cachePath, internal.UncommittedUploadFailed)

	fc, mock := newRecoveryFC(t, mockCtrl, cachePath, "recover")
	fc.mountPath = mountPath
	mock.EXPECT().CopyFromFile(gomock.Any()).Return(fmt.Errorf("storage down")).Times(1)

	err := fc.recoverOnStartup()
	assert.Error(t, err)

	// File and manifest retained for the next mount.
	_, statErr := os.Stat(localFile)
	assert.NoError(t, statErr)
	m, err := recovery.LoadManifest(manifestPath)
	assert.NoError(t, err)
	if assert.NotNil(t, m) {
		assert.Len(t, m.Items, 1)
	}
}

func TestRecoverOnStartupFailureReadonlyPolicy(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	cachePath := t.TempDir()
	localFile := filepath.Join(cachePath, "a.txt")
	assert.NoError(t, os.WriteFile(localFile, []byte("data"), 0o644))

	mountPath := "/mnt/recover-readonly"
	manifestPath := writeFileCacheManifest(t, mountPath, cachePath, internal.UncommittedUploadFailed)

	fc, mock := newRecoveryFC(t, mockCtrl, cachePath, "readonly")
	fc.mountPath = mountPath
	mock.EXPECT().CopyFromFile(gomock.Any()).Return(fmt.Errorf("storage down")).Times(1)

	// Read-only policy does not fail the mount; residue preserved.
	assert.NoError(t, fc.recoverOnStartup())
	_, statErr := os.Stat(localFile)
	assert.NoError(t, statErr)
	m, err := recovery.LoadManifest(manifestPath)
	assert.NoError(t, err)
	assert.NotNil(t, m)
}

func TestRecoverOnStartupMissingLocalFile(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	cachePath := t.TempDir()
	mountPath := "/mnt/recover-missing"
	manifestPath := writeFileCacheManifest(t, mountPath, cachePath, internal.UncommittedFlushTimeout)

	fc, _ := newRecoveryFC(t, mockCtrl, cachePath, "recover")
	fc.mountPath = mountPath
	// No CopyFromFile expectation: missing file must not reach the next component.

	err := fc.recoverOnStartup()
	assert.Error(t, err)
	m, err := recovery.LoadManifest(manifestPath)
	assert.NoError(t, err)
	assert.NotNil(t, m)
}

func TestRecoverOnStartupNoManifest(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	fc, _ := newRecoveryFC(t, mockCtrl, t.TempDir(), "recover")
	fc.mountPath = "/mnt/no-manifest"
	assert.NoError(t, fc.recoverOnStartup())
}
