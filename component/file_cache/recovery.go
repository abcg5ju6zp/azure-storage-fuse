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
	"fmt"
	"os"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// recoverOnStartup re-uploads cache files preserved by an earlier unclean
// shutdown. Recovered files are deleted after success; failed items are kept
// in the manifest. With recovery-policy=recover any failure aborts the mount;
// with readonly the mount proceeds read-only and the residue survives.
func (fc *FileCache) recoverOnStartup() error {
	manifestPath := recovery.ManifestPath(common.DefaultWorkDir, fc.mountPath, compName)
	manifest, err := recovery.LoadManifest(manifestPath)
	if err != nil {
		// Corrupt manifest: fail conservatively, never wipe blindly.
		return fmt.Errorf("file-cache recovery manifest unreadable: %w", err)
	}
	if manifest == nil {
		return nil
	}

	cfg, cfgErr := shutdown.LoadConfig()
	if cfgErr != nil {
		cfg = shutdown.DefaultConfig()
	}

	log.Crit("FileCache::recoverOnStartup : recovering %d record(s) from %s",
		len(manifest.Items), manifestPath)

	recovered, failed := 0, 0
	remaining := make([]internal.UncommittedItem, 0, len(manifest.Items))
	for _, it := range manifest.Items {
		if fc.recoverOne(it) {
			recovered++
			continue
		}
		failed++
		remaining = append(remaining, it)
	}

	if failed > 0 {
		log.Crit("FileCache::recoverOnStartup : %d recovered, %d failed; residue preserved",
			recovered, failed)
		manifest.Items = remaining
		manifest.Identity = ""
		if err := recovery.SaveManifest(manifestPath, manifest); err != nil {
			log.Err("FileCache::recoverOnStartup : failed to update manifest [%v]", err)
		}
		if cfg.RecoveryPolicy == shutdown.RecoveryRecover {
			return fmt.Errorf("file-cache recovery failed for %d file(s), mount aborted", failed)
		}
		log.Crit("FileCache::recoverOnStartup : recovery-policy=readonly, mounting read-only with residue")
		return nil
	}

	log.Crit("FileCache::recoverOnStartup : all %d record(s) recovered, removing manifest", recovered)
	return recovery.DeleteManifest(manifestPath)
}

// recoverOne uploads one preserved file and removes its local copy on success.
func (fc *FileCache) recoverOne(it internal.UncommittedItem) bool {
	if it.LocalPath == "" {
		log.Crit("FileCache::recoverOne : %s has no local cache path, cannot recover", it.Name)
		return false
	}
	f, err := os.Open(it.LocalPath)
	if err != nil {
		log.Err("FileCache::recoverOne : cannot open %s [%v]", it.LocalPath, err)
		return false
	}

	uploadErr := fc.NextComponent().CopyFromFile(internal.CopyFromFileOptions{
		Name: it.Name,
		File: f,
	})
	closeErr := f.Close()
	if uploadErr != nil {
		log.Err("FileCache::recoverOne : re-upload of %s failed [%v]", it.Name, uploadErr)
		return false
	}
	if closeErr != nil {
		log.Warn("FileCache::recoverOne : close after upload of %s reported [%v]", it.Name, closeErr)
	}

	if err := os.Remove(it.LocalPath); err != nil && !os.IsNotExist(err) {
		log.Warn("FileCache::recoverOne : uploaded file left on disk %s [%v]", it.LocalPath, err)
	}
	return true
}
