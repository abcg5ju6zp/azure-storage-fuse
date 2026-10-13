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

package cmd

import (
	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// manifestComponents lists cache components that can leave recovery residue.
var manifestComponents = []string{"file_cache", "block_cache", "xload"}

// loadRecoveryManifests returns the manifests found for this mount across the
// cache components. Parse failures are logged and treated conservatively.
func loadRecoveryManifests(mountPath string) []*recovery.Manifest {
	out := make([]*recovery.Manifest, 0)
	for _, component := range manifestComponents {
		path := recovery.ManifestPath(common.DefaultWorkDir, mountPath, component)
		m, err := recovery.LoadManifest(path)
		if err != nil {
			log.Err("Mount::loadRecoveryManifests : %s unreadable [%v]", path, err)
			continue
		}
		if m != nil {
			out = append(out, m)
		}
	}
	return out
}

// applyRecoveryReadonly forces read-only mode when recovery-policy=readonly
// and at least one uncommitted-data manifest exists. Must run before the
// pipeline is configured, because libfuse resolves read-only at Configure.
func applyRecoveryReadonly(components []string, mountPath string) {
	cfg, err := shutdown.LoadConfig()
	if err != nil {
		log.Err("Mount::applyRecoveryReadonly : invalid shutdown config [%v]", err)
		return
	}
	if cfg.RecoveryPolicy != shutdown.RecoveryReadonly {
		return
	}

	have := false
	for _, m := range loadRecoveryManifests(mountPath) {
		if contains(components, m.Component) && len(m.Items) > 0 {
			have = true
			break
		}
	}
	if !have {
		return
	}

	log.Crit("Mount::applyRecoveryReadonly : recovery residue present; mounting %s read-only", mountPath)
	// libfuse resolves read-only at Configure (before pipeline Start), so this
	// must happen before NewPipeline.
	config.Set("read-only", "true")
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// protectedSetFromManifests builds a ProtectedSet over all recovery manifests
// belonging to cache components in this pipeline.
func protectedSetFromManifests(components []string, mountPath string) *recovery.ProtectedSet {
	set := recovery.NewProtectedSet()
	for _, m := range loadRecoveryManifests(mountPath) {
		if !contains(components, m.Component) {
			continue
		}
		for _, it := range m.Items {
			set.Add(it.LocalPath)
		}
	}
	return set
}
