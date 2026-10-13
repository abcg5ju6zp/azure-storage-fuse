/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |----  _____| |-----| |-----  ------
   |     |  |      |     | |     | |     | \   /| |       | |       |
   | ____|  |_____ | ____| | ____| |     |__\_/ |  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2026 Microsoft Corporation. All rights reserved.
*/

package block_cache

import (
	"fmt"
	"os"
	"sort"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// recoverOnStartup commits staged-but-uncommitted blocks recorded by an
// earlier unclean shutdown via PutBlockList. In-memory-only losses are
// reported but cannot be recovered. Residue survives failed commits.
func (bc *BlockCache) recoverOnStartup() error {
	manifestPath := recovery.ManifestPath(common.DefaultWorkDir, bc.mntPath, compName)
	manifest, err := recovery.LoadManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("block-cache recovery manifest unreadable: %w", err)
	}
	if manifest == nil {
		return nil
	}

	cfg, cfgErr := shutdown.LoadConfig()
	if cfgErr != nil {
		cfg = shutdown.DefaultConfig()
	}

	log.Crit("BlockCache::recoverOnStartup : recovering %d record(s) from %s",
		len(manifest.Items), manifestPath)

	byName := make(map[string][]internal.UncommittedItem)
	var lost int
	for _, it := range manifest.Items {
		if it.Reason == internal.UncommittedInMemoryLost {
			lost++
			log.Crit("BlockCache::recoverOnStartup : %s had memory-only dirty blocks, data lost",
				it.Name)
			continue
		}
		byName[it.Name] = append(byName[it.Name], it)
	}
	if lost > 0 {
		log.Crit("BlockCache::recoverOnStartup : %d record(s) unrecoverable (in-memory-lost)", lost)
	}

	failed := 0
	remaining := make([]internal.UncommittedItem, 0)
	for name, group := range byName {
		if ok, its := bc.recoverBlob(name, group); ok {
			for _, it := range its {
				if it.LocalPath != "" {
					if err := os.Remove(it.LocalPath); err != nil && !os.IsNotExist(err) {
						log.Warn("BlockCache::recoverOnStartup : block copy left on disk %s [%v]",
							it.LocalPath, err)
					}
				}
			}
		} else {
			failed++
			remaining = append(remaining, its...)
		}
	}

	recoverableBlobs := len(byName)
	if failed > 0 {
		log.Crit("BlockCache::recoverOnStartup : %d blob(s) recovered, %d failed; residue preserved",
			recoverableBlobs-failed, failed)
		manifest.Items = remaining
		manifest.Identity = ""
		if err := recovery.SaveManifest(manifestPath, manifest); err != nil {
			log.Err("BlockCache::recoverOnStartup : failed to update manifest [%v]", err)
		}
		if cfg.RecoveryPolicy == shutdown.RecoveryRecover {
			return fmt.Errorf("block-cache recovery failed for %d blob(s), mount aborted", failed)
		}
		log.Crit("BlockCache::recoverOnStartup : recovery-policy=readonly, mounting read-only with residue")
		return nil
	}

	log.Crit("BlockCache::recoverOnStartup : all %d blob(s) recovered (%d lost record(s) reported), removing manifest",
		recoverableBlobs, lost)
	return recovery.DeleteManifest(manifestPath)
}

// recoverBlob commits one blob's staged blocks in offset order.
func (bc *BlockCache) recoverBlob(name string, group []internal.UncommittedItem) (bool, []internal.UncommittedItem) {
	sort.Slice(group, func(i, j int) bool { return group[i].Offset < group[j].Offset })

	blockIDs := make([]string, 0, len(group))
	for _, it := range group {
		blockIDs = append(blockIDs, it.BlockIDs...)
	}

	err := bc.CommitData(internal.CommitDataOptions{
		Name:      name,
		List:      blockIDs,
		BlockSize: bc.blockSize,
	})
	if err != nil {
		log.Err("BlockCache::recoverBlob : PutBlockList failed for %s [%v]", name, err)
		return false, group
	}
	return true, group
}
