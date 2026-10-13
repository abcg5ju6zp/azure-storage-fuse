/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |---- |     | |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2026 Microsoft Corporation. All rights reserved.
   Author : <blobfusedev@microsoft.com>

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE
*/

// Package shutdown implements the single-owner, phased shutdown coordinator used
// by the mount pipeline. Every shutdown trigger (terminal signal, parent process
// exit, natural FUSE session teardown) enters the same coordinator so that phases
// run at most once and all waits are bounded.
package shutdown

import (
	"fmt"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common/config"
)

// Bounded shutdown defaults. All of them are overridable through the "stop"
// config section.
const (
	// DefaultOpDrainTimeout bounds how long the coordinator waits for FUSE
	// operations that were in flight when the freeze gate closed.
	DefaultOpDrainTimeout = 5 * time.Second

	// DefaultFlushTimeout bounds the flush/commit phase for dirty handles.
	DefaultFlushTimeout = 30 * time.Second

	// DefaultComponentStopTimeout bounds Stop() of a single pipeline component.
	DefaultComponentStopTimeout = 10 * time.Second

	// DefaultMonitorStopTimeout bounds the graceful termination of bfusemon.
	DefaultMonitorStopTimeout = 5 * time.Second
)

// Exit codes reported by the blobfuse2 mount process. They are documented as
// part of the shutdown contract and must stay stable.
const (
	// ExitOK : shutdown completed and every write was confirmed committed.
	ExitOK = 0
	// ExitGenericError : initialization or generic runtime failure (cobra's
	// historical exit path).
	ExitGenericError = 1
	// ExitDataUnsafe : shutdown completed within bounds but some data was not
	// confirmed committed; it was reported and/or preserved in a recovery
	// manifest. Maps to EX_TEMPFAIL semantics.
	ExitDataUnsafe = 75
)

// RecoveryPolicy decides mount behaviour when a recovery manifest from a
// previous unclean shutdown is present.
type RecoveryPolicy string

const (
	// RecoveryRecover (default): re-upload/commit manifest entries before the
	// filesystem is served. Recovery failure fails the mount and preserves the
	// residue.
	RecoveryRecover RecoveryPolicy = "recover"
	// RecoveryReadonly: on recovery failure fall back to a read-only mount and
	// keep the residue on disk.
	RecoveryReadonly RecoveryPolicy = "readonly"
)

// Config holds the bounded shutdown timeouts and the remount recovery policy.
type Config struct {
	OpDrainTimeout       time.Duration
	FlushTimeout         time.Duration
	ComponentStopTimeout time.Duration
	MonitorStopTimeout   time.Duration
	RecoveryPolicy       RecoveryPolicy
}

// rawConfig mirrors the "stop" yaml/config section. Zero values mean "unset"
// and are replaced by defaults during LoadConfig.
type rawConfig struct {
	OpDrainTimeoutSec       uint32 `config:"op-drain-timeout-sec" yaml:"op-drain-timeout-sec,omitempty"`
	FlushTimeoutSec         uint32 `config:"flush-timeout-sec" yaml:"flush-timeout-sec,omitempty"`
	ComponentStopTimeoutSec uint32 `config:"component-stop-timeout-sec" yaml:"component-stop-timeout-sec,omitempty"`
	MonitorStopTimeoutSec   uint32 `config:"monitor-stop-timeout-sec" yaml:"monitor-stop-timeout-sec,omitempty"`
	RecoveryPolicy          string `config:"recovery-policy" yaml:"recovery-policy,omitempty"`
}

// DefaultConfig returns the timeout and recovery defaults used when the "stop"
// section is absent or only partially configured.
func DefaultConfig() Config {
	return Config{
		OpDrainTimeout:       DefaultOpDrainTimeout,
		FlushTimeout:         DefaultFlushTimeout,
		ComponentStopTimeout: DefaultComponentStopTimeout,
		MonitorStopTimeout:   DefaultMonitorStopTimeout,
		RecoveryPolicy:       RecoveryRecover,
	}
}

// Validate checks that every timeout is non-negative and the recovery policy
// is a known value.
func (c Config) Validate() error {
	if c.OpDrainTimeout < 0 || c.FlushTimeout < 0 ||
		c.ComponentStopTimeout < 0 || c.MonitorStopTimeout < 0 {
		return fmt.Errorf("stop config error [timeouts cannot be negative]")
	}

	switch c.RecoveryPolicy {
	case RecoveryRecover, RecoveryReadonly:
	default:
		return fmt.Errorf("stop config error [invalid recovery-policy %q, allowed: recover|readonly]", c.RecoveryPolicy)
	}

	return nil
}

// LoadConfig reads the optional "stop" config section on top of the defaults.
// A missing section or individual unset keys retain their default values.
func LoadConfig() (Config, error) {
	cfg := DefaultConfig()

	var raw rawConfig
	if config.IsSet("stop") {
		if err := config.UnmarshalKey("stop", &raw); err != nil {
			return cfg, fmt.Errorf("stop config error [%s]", err.Error())
		}
	}

	if raw.OpDrainTimeoutSec > 0 {
		cfg.OpDrainTimeout = time.Duration(raw.OpDrainTimeoutSec) * time.Second
	}
	if raw.FlushTimeoutSec > 0 {
		cfg.FlushTimeout = time.Duration(raw.FlushTimeoutSec) * time.Second
	}
	if raw.ComponentStopTimeoutSec > 0 {
		cfg.ComponentStopTimeout = time.Duration(raw.ComponentStopTimeoutSec) * time.Second
	}
	if raw.MonitorStopTimeoutSec > 0 {
		cfg.MonitorStopTimeout = time.Duration(raw.MonitorStopTimeoutSec) * time.Second
	}
	if raw.RecoveryPolicy != "" {
		cfg.RecoveryPolicy = RecoveryPolicy(raw.RecoveryPolicy)
	}

	return cfg, cfg.Validate()
}
