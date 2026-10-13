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

package cmd

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	hmcommon "github.com/Azure/azure-storage-fuse/v2/tools/health-monitor/common"
)

// bfusemonProcess is the externally started health monitor. It implements
// monitorHandle for the shutdown coordinator.
type bfusemonProcess struct {
	cmd  *exec.Cmd
	done chan error
}

// startMonitorProcess launches bfusemon as an external process (relying on
// PATH, as the packaged binary does) and keeps its handle for graceful
// shutdown. Returns nil when monitoring is disabled or launch fails.
func startMonitorProcess(blobfusePid int) monitorHandle {
	if !common.EnableMonitoring {
		return nil
	}

	args := BuildMonitorCliParams(strconv.Itoa(blobfusePid))
	cmd := exec.Command(hmcommon.BfuseMon, args...)
	// Detach stdout/stderr: bfusemon reports via syslog and its output file.
	if err := cmd.Start(); err != nil {
		log.Err("Mount::startMonitorProcess : unable to start %s [%v]", hmcommon.BfuseMon, err)
		common.EnableMonitoring = false
		return nil
	}

	p := &bfusemonProcess{cmd: cmd, done: make(chan error, 1)}
	go func() {
		p.done <- cmd.Wait()
		close(p.done)
	}()
	log.Info("Mount::startMonitorProcess : started %s pid %d for blobfuse2 pid %d",
		hmcommon.BfuseMon, cmd.Process.Pid, blobfusePid)
	return p
}

// Shutdown implements monitorHandle. A forced shutdown skips straight to
// SIGKILL; otherwise SIGTERM lets bfusemon flush monitor_<pid>.json and the
// coordinator deadline bounds the wait.
func (p *bfusemonProcess) Shutdown(ctx context.Context, force bool) error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	pid := p.cmd.Process.Pid

	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	if err := p.cmd.Process.Signal(sig); err != nil && err != syscall.ESRCH {
		return fmt.Errorf("signal %v bfusemon pid %d: %w", sig, pid, err)
	}
	if force {
		log.Info("Mount::monitorShutdown : SIGKILL sent to bfusemon pid %d", pid)
		return nil
	}

	select {
	case <-p.done:
		log.Info("Mount::monitorShutdown : bfusemon pid %d exited gracefully", pid)
		return nil
	case <-ctx.Done():
		log.Warn("Mount::monitorShutdown : bfusemon pid %d did not exit in time, SIGKILL", pid)
		if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			return err
		}
		select {
		case <-p.done:
		case <-time.After(time.Second):
		}
		return nil
	}
}
