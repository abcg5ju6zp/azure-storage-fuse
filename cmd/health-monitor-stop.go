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
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common/log"

	"github.com/spf13/cobra"
)

// gracefulMonitorTimeout bounds the wait between SIGTERM (which lets bfusemon
// flush its JSON output) and the SIGKILL fallback.
var gracefulMonitorTimeout = 5 * time.Second

// monitorPidPattern extracts the --pid=<n> argument with word boundaries so
// "--pid=12" cannot match a "--pid=123" argument or a pid embedded in a path.
func monitorPidPattern(pid string) *regexp.Regexp {
	return regexp.MustCompile(`(?:^|\s)--pid=` + regexp.QuoteMeta(pid) + `(?:\s|$)`)
}

// getPid executes ps for all bfusemon processes and returns the pids whose
// command line contains a word-boundary --pid=<pid> argument.
func getMonitorPids(targetPid string) ([]int, error) {
	var out bytes.Buffer
	var errb bytes.Buffer
	cmd := exec.Command("ps", "-e", "-o", "pid=,args=")
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("pidof failed: %v", err)
	}

	pattern := monitorPidPattern(targetPid)
	pids := make([]int, 0)
	for _, line := range strings.Split(out.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, "bfusemon") {
			continue
		}
		if !pattern.MatchString(" " + line) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			log.Err("health monitor stop : invalid pid %q in ps line: %s", fields[0], line)
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

// stopMonitorPid asks one bfusemon to exit gracefully and escalates to
// SIGKILL after gracefulMonitorTimeout. A process that already exited is not
// an error.
func stopMonitorPid(pid int) error {
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		if err == syscall.ESRCH {
			return nil // already gone
		}
		return err
	}

	deadline := time.Now().Add(gracefulMonitorTimeout)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err != nil {
			if err == syscall.ESRCH {
				return nil
			}
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Warn("health monitor stop : bfusemon pid %d did not exit in %v, sending SIGKILL",
		pid, gracefulMonitorTimeout)
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}

var healthMonStopCmd = &cobra.Command{
	Use:        "health-monitor stop --pid=<blobfuse2 pid>",
	Short:      "Stop health-monitor process for a given blobfuse2 pid",
	Deprecated: "",
	Hidden:     true,
	Args:       cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		pid := args[0]
		pids, err := getMonitorPids(pid)
		if err != nil {
			return err
		}
		var errs []string
		for _, p := range pids {
			if err := stopMonitorPid(p); err != nil {
				errs = append(errs, fmt.Sprintf("pid %d: %v", p, err))
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("%s", strings.Join(errs, "; "))
		}
		return nil
	},
}

var healthMonStopAllCmd = &cobra.Command{
	Use:        "health-monitor stop-all",
	Short:      "Stop all running health-monitor processes",
	Deprecated: "",
	Hidden:     true,
	Args:       cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		var out bytes.Buffer
		var errb bytes.Buffer
		cmdExec := exec.Command("ps", "-e", "-o", "pid=,args=")
		cmdExec.Stdout = &out
		cmdExec.Stderr = &errb
		if err := cmdExec.Run(); err != nil {
			return fmt.Errorf("killall failed: %v", err)
		}

		var errs []string
		for _, line := range strings.Split(out.String(), "\n") {
			line = strings.TrimSpace(line)
			if !strings.Contains(line, "bfusemon") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			pid, err := strconv.Atoi(fields[0])
			if err != nil {
				continue
			}
			if err := stopMonitorPid(pid); err != nil {
				errs = append(errs, fmt.Sprintf("pid %d: %v", pid, err))
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("%s", strings.Join(errs, "; "))
		}
		return nil
	},
}
