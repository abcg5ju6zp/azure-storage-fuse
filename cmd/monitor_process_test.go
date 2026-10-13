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
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newSleepProcess(t *testing.T, ignoreTerm bool) *bfusemonProcess {
	t.Helper()
	var cmd *exec.Cmd
	if ignoreTerm {
		cmd = exec.Command("sh", "-c", "trap '' TERM; sleep 30")
	} else {
		cmd = exec.Command("sleep", "30")
	}
	assert.NoError(t, cmd.Start())
	p := &bfusemonProcess{cmd: cmd, done: make(chan error, 1)}
	go func() {
		p.done <- cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return p
}

func TestBfusemonProcessGracefulShutdown(t *testing.T) {
	p := newSleepProcess(t, false)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assert.NoError(t, p.Shutdown(ctx, false))
	assert.WithinDuration(t, start, time.Now(), 2*time.Second, "graceful stop too slow")
}

func TestBfusemonProcessForcedShutdownKills(t *testing.T) {
	// Child ignores SIGTERM: graceful path must escalate to SIGKILL within the
	// bounded context instead of waiting the full 30s.
	p := newSleepProcess(t, true)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	assert.NoError(t, p.Shutdown(ctx, false))
	assert.WithinDuration(t, start, time.Now(), 3*time.Second, "SIGKILL escalation too slow")
}

func TestBfusemonProcessImmediateForce(t *testing.T) {
	p := newSleepProcess(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assert.NoError(t, p.Shutdown(ctx, true))
}
