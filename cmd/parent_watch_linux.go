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

	"github.com/Azure/azure-storage-fuse/v2/common/log"
)

// prSetPDeathSig is PR_SET_PDEATHSIG (linux/prctl.h).
const prSetPDeathSig = 0x1

// enablePDeathSignal asks the kernel to deliver SIGTERM to this process when
// its parent (the daemonizer) dies. The signal is handled by the ordinary
// shutdown signal entry; the getppid poll provides the parent-exit identity.
func enablePDeathSignal() {
	if _, _, errno := syscall.Syscall(syscall.SYS_PRCTL,
		prSetPDeathSig, uintptr(syscall.SIGTERM), 0); errno != 0 {
		log.Warn("Mount::enablePDeathSignal : PR_SET_PDEATHSIG failed [%v], relying on getppid poll", errno)
	}
}
