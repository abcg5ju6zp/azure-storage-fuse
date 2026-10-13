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
	"syscall"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// watchParentExiter triggers the shutdown coordinator with parent-exit when
// the daemonizer process disappears. Linux uses PR_SET_PDEATHSIG as the
// primary mechanism (see parent_watch_linux.go); a getppid() reparenting poll
// is the portable fallback and also catches PDEATHSIG edge cases.
func watchParentExiter(coord *shutdown.Coordinator) {
	enablePDeathSignal()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if syscall.Getppid() == 1 {
			if _, owner := coord.Begin(shutdown.TriggerParentExit, "parent process exited"); owner {
				log.Crit("Mount::watchParentExiter : parent gone, started parent-exit shutdown")
			}
			return
		}
	}
}
