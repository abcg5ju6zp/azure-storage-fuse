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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type fakeDrainer struct {
	*internal.MockComponent
	name       string
	items      []internal.UncommittedItem
	stopErr    error
	deferred   bool
	cleaned    bool
	cleanedErr error
}

func (f *fakeDrainer) Name() string { return f.name }
func (f *fakeDrainer) DrainForShutdown(ctx context.Context) ([]internal.UncommittedItem, error) {
	return f.items, nil
}
func (f *fakeDrainer) DeferCacheCleanup() { f.deferred = true }
func (f *fakeDrainer) CleanupCache() error {
	f.cleaned = true
	return f.cleanedErr
}

type fakeMonitor struct {
	calls   int
	forced  []bool
	delayed time.Duration
}

func (m *fakeMonitor) Shutdown(ctx context.Context, force bool) error {
	m.calls++
	m.forced = append(m.forced, force)
	if m.delayed > 0 {
		select {
		case <-time.After(m.delayed):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func newOrchTest(t *testing.T, drainer *fakeDrainer) (*shutdownRuntime, *shutdown.Coordinator) {
	t.Helper()
	// Isolate ~/.blobfuse2 (status files, manifests) per test.
	t.Setenv("HOME", t.TempDir())
	common.DefaultWorkDir = "$HOME/.blobfuse2"

	mockCtrl := gomock.NewController(t)
	mock := internal.NewMockComponent(mockCtrl)
	mock.EXPECT().Stop().Return(drainer.stopErr).Times(1)

	// fakeDrainer embeds the mock but overrides Stop itself only via the mock
	// field; wire the embedded mock expectations to serve Stop calls.
	drainer.MockComponent = mock
	drainer.name = "file_cache"

	p := internal.NewPipelineWithComponents([]internal.Component{drainer})
	cfg := shutdown.Config{
		OpDrainTimeout:       50 * time.Millisecond,
		FlushTimeout:         time.Second,
		ComponentStopTimeout: time.Second,
		MonitorStopTimeout:   time.Second,
		RecoveryPolicy:       shutdown.RecoveryRecover,
	}
	coord := shutdown.New(cfg)
	rt := &shutdownRuntime{
		coord:     coord,
		pipeline:  p,
		mountPath: "/mnt/test",
		monitor:   &fakeMonitor{},
	}
	return rt, coord
}

// Natural fuse-exit path, no dirty data: clean exit 0, all phases observed.
func TestOrchestratorNaturalExitClean(t *testing.T) {
	rt, _ := newOrchTest(t, &fakeDrainer{})
	rt.coord.Begin(shutdown.TriggerFuseExit, "unmount")

	code := rt.runPhases(nil, true, nil)
	assert.Equal(t, shutdown.ExitOK, code)

	snap := rt.coord.Snapshot()
	for _, phase := range shutdown.OrderedPhases {
		phaseSeen := false
		for _, r := range snap.Results {
			if r.Phase == phase && !r.End.IsZero() {
				phaseSeen = true
			}
		}
		assert.True(t, phaseSeen, "phase %s not observed", phase)
	}

	mon := rt.monitor.(*fakeMonitor)
	assert.Equal(t, 1, mon.calls)
	assert.Equal(t, []bool{false}, mon.forced)

	workDir := common.ExpandPath(common.DefaultWorkDir)
	st, err := recovery.LoadStatus(recovery.StatusPath(workDir, "/mnt/test"))
	assert.NoError(t, err)
	if assert.NotNil(t, st) {
		assert.Equal(t, 0, st.ExitCode)
	}
	// No manifest without uncommitted items.
	m, err := recovery.LoadManifest(recovery.ManifestPath(workDir, "/mnt/test", "file_cache"))
	assert.NoError(t, err)
	assert.Nil(t, m)
}

// Failed/timed out uploads: exit 75, manifest written, cache file preserved.
func TestOrchestratorUncommittedPreserves(t *testing.T) {
	home := os.Getenv("HOME")
	cacheDir := filepath.Join(home, "tmpcache")
	protectedFile := filepath.Join(cacheDir, "dir", "uncommitted.txt")
	assert.NoError(t, os.MkdirAll(filepath.Dir(protectedFile), 0o755))
	assert.NoError(t, os.WriteFile(protectedFile, []byte("bytes"), 0o644))

	d := &fakeDrainer{
		items: []internal.UncommittedItem{{
			Component: "file_cache",
			Name:      "dir/uncommitted.txt",
			LocalPath: protectedFile,
			Size:      5,
			Reason:    internal.UncommittedFlushTimeout,
		}},
	}
	rt, _ := newOrchTest(t, d)
	rt.coord.Begin(shutdown.TriggerSignal, "sigterm")

	startCh := make(chan error, 1)
	startCh <- nil // fuse_main returned after session exit
	code := rt.runPhases(nil, false, startCh)
	assert.Equal(t, shutdown.ExitDataUnsafe, code)
	assert.True(t, d.deferred, "cache cleanup must be deferred past Stop")
	assert.True(t, d.cleaned, "CleanupCache must run in cache-cleaning phase")

	// File on disk survives.
	_, err := os.Stat(protectedFile)
	assert.NoError(t, err)

	workDir := common.ExpandPath(common.DefaultWorkDir)
	m, err := recovery.LoadManifest(recovery.ManifestPath(workDir, "/mnt/test", "file_cache"))
	assert.NoError(t, err)
	if assert.NotNil(t, m) && assert.Len(t, m.Items, 1) {
		assert.Equal(t, internal.UncommittedFlushTimeout, m.Items[0].Reason)
	}
	st, err := recovery.LoadStatus(recovery.StatusPath(workDir, "/mnt/test"))
	assert.NoError(t, err)
	assert.Equal(t, 75, st.ExitCode)
	assert.Len(t, st.Items, 1)
	assert.Equal(t, "dir/uncommitted.txt", st.Items[0].Name)
}

// Second signal forces the monitor stop to skip to SIGKILL semantics.
func TestOrchestratorForcedMonitorFlag(t *testing.T) {
	rt, coord := newOrchTest(t, &fakeDrainer{})
	coord.Begin(shutdown.TriggerSignal, "sigterm")
	coord.Escalate("second signal")

	code := rt.runPhases(nil, true, nil)
	// Forced (second-signal) path always reports data-unsafe: uploads were
	// skipped by the canceled graceful context.
	assert.Equal(t, shutdown.ExitDataUnsafe, code)
	mon := rt.monitor.(*fakeMonitor)
	if assert.Len(t, mon.forced, 1) {
		assert.True(t, mon.forced[0])
	}
}

// A component Stop error surfaces as data-unsafe exit even with no items.
func TestOrchestratorStopFailure(t *testing.T) {
	rt, _ := newOrchTest(t, &fakeDrainer{stopErr: assertError("azstorage stuck")})
	rt.coord.Begin(shutdown.TriggerFuseExit, "unmount")

	code := rt.runPhases(nil, true, nil)
	assert.Equal(t, shutdown.ExitDataUnsafe, code)
}

type staticError string

func (e staticError) Error() string { return string(e) }

func assertError(msg string) error { return staticError(msg) }
