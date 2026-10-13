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
	"os"
	"syscall"

	daemon "github.com/sevlyar/go-daemon"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// daemonChildSignalHandler is inherited by the daemonized child. USR1 keeps
// the existing hot config reload, USR2 stays log-only, and terminal signals
// are forwarded into the coordinator's signal channel.
func daemonChildSignalHandler(sigCh chan<- os.Signal) daemon.SignalHandlerFunc {
	return func(sig os.Signal) error {
		log.Crit("Mount::sigHandler : Signal %d received", sig)
		switch sig {
		case syscall.SIGUSR1:
			config.OnConfigChange()
		case syscall.SIGUSR2:
			log.Crit("Mount::sigHandler : SIGUSR2 received")
		default:
			select {
			case sigCh <- sig:
			default:
				log.Warn("Mount::sigHandler : signal channel full, dropping %v", sig)
			}
		}
		return nil
	}
}

// forwardSignalsToChild re-sends every terminal signal the daemon parent
// receives to its child, where the child's own handler owns the shutdown
// sequencing. Each forwarded signal increments the child's escalation count
// (first = graceful, second = forced).
func forwardSignalsToChild(in <-chan os.Signal, childPid int) {
	for sig := range in {
		s, ok := sig.(syscall.Signal)
		if !ok {
			continue
		}
		log.Info("Mount::forwardSignalsToChild : forwarding %v to child %d", s, childPid)
		if err := syscall.Kill(childPid, s); err != nil {
			log.Err("Mount::forwardSignalsToChild : failed to signal child %d [%v]", childPid, err)
		}
	}
}

// childWaitResult is the terminal wait4 outcome for the daemon child.
type childWaitResult struct {
	status syscall.WaitStatus
	err    error
}

// waitChildResult blocks until the child actually exits or is killed. Unlike
// the old monitorChild it keeps waiting through job-control stops (Stopped/
// Continued) instead of treating them as termination, and returns the raw
// wait status for exit-code propagation.
func waitChildResult(pid int, out chan<- childWaitResult) {
	var wstatus syscall.WaitStatus
	for {
		_, err := syscall.Wait4(pid, &wstatus, 0, nil)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			out <- childWaitResult{err: err}
			return
		}
		if wstatus.Stopped() || wstatus.Continued() {
			log.Info("Mount::waitChildResult : child %d state change (stopped/continued), keeping wait", pid)
			continue
		}
		out <- childWaitResult{status: wstatus}
		return
	}
}

// exitCode maps the wait outcome to the propagated process exit code. When
// wait4 fails (e.g. ECHILD after a pid reuse/race), the child's stop status
// file is the bounded fallback instead of waiting indefinitely.
func (r childWaitResult) exitCode(mountPath string) int {
	if r.err == nil {
		switch {
		case r.status.Exited():
			return r.status.ExitStatus()
		case r.status.Signaled():
			return 128 + int(r.status.Signal())
		}
	}

	statusPath := recovery.StatusPath(common.DefaultWorkDir, mountPath)
	if status, err := recovery.LoadStatus(statusPath); err == nil && status != nil {
		log.Crit("Mount::exitCode : child wait4 failed [%v], using child-reported code %d from %s",
			r.err, status.ExitCode, statusPath)
		return status.ExitCode
	}

	log.Err("Mount::exitCode : child %v state lost and no status file [%v], exiting %d",
		mountPath, r.err, shutdown.ExitGenericError)
	return shutdown.ExitGenericError
}
