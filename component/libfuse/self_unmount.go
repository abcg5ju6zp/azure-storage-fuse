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

package libfuse

import (
	"os/exec"

	"github.com/Azure/azure-storage-fuse/v2/common/log"
)

// selfUnmount detaches our own mountpoint through fusermount, which tears down
// the kernel FUSE connection and makes the blocked fuse_main loop return. It
// is the in-process counterpart of `blobfuse2 unmount` on the signal-driven
// shutdown path. Errors are informational: an external unmount may already
// have detached the session.
func (lf *Libfuse) selfUnmount() {
	if lf.mountPath == "" {
		return
	}

	cmds := [][]string{
		{"fusermount3", "-u"},
		{"fusermount", "-u"},
	}
	for _, args := range cmds {
		path, err := exec.LookPath(args[0])
		if err != nil {
			continue
		}
		cmd := exec.Command(path, append(args[1:], lf.mountPath)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			log.Debug("Libfuse::selfUnmount : %s %s failed [%v]: %s",
				args[0], lf.mountPath, err, string(out))
			continue
		}
		log.Info("Libfuse::selfUnmount : %s detached %s", args[0], lf.mountPath)
		return
	}
}
