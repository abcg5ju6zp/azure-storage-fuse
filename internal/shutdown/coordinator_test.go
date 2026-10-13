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

package shutdown

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type captureLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *captureLogger) record(f string, a ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(f, a...))
}
func (l *captureLogger) Debug(f string, a ...interface{}) { l.record(f, a...) }
func (l *captureLogger) Info(f string, a ...interface{})  { l.record(f, a...) }
func (l *captureLogger) Warn(f string, a ...interface{})  { l.record(f, a...) }
func (l *captureLogger) Err(f string, a ...interface{})   { l.record(f, a...) }
func (l *captureLogger) Crit(f string, a ...interface{})  { l.record(f, a...) }

func testCoordinator(t *testing.T) (*Coordinator, *captureLogger) {
	t.Helper()
	l := &captureLogger{}
	c := NewWithLogger(Config{
		OpDrainTimeout:       50 * time.Millisecond,
		FlushTimeout:         200 * time.Millisecond,
		ComponentStopTimeout: 100 * time.Millisecond,
		MonitorStopTimeout:   50 * time.Millisecond,
		RecoveryPolicy:       RecoveryRecover,
	}, l)
	return c, l
}

func TestBeginSingleOwner(t *testing.T) {
	c, _ := testCoordinator(t)

	const n = 128
	var owners int64
	var wg sync.WaitGroup
	triggers := []Trigger{TriggerSignal, TriggerParentExit, TriggerFuseExit}
	var wgReady sync.WaitGroup
	wgReady.Add(n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wgReady.Done()
			<-start
			_, owner := c.Begin(triggers[i%len(triggers)], fmt.Sprintf("r%d", i))
			if owner {
				atomic.AddInt64(&owners, 1)
			}
		}(i)
	}
	wgReady.Wait()
	close(start)
	wg.Wait()

	if owners != 1 {
		t.Fatalf("expected exactly 1 owner, got %d", owners)
	}
	if !c.Started() {
		t.Fatal("coordinator not started after Begin")
	}

	snap := c.Snapshot()
	if snap.Identity.ID == "" {
		t.Fatal("identity id empty")
	}
	if len(snap.Events) != n-1 {
		t.Fatalf("expected %d ignored-trigger events, got %d", n-1, len(snap.Events))
	}
	for _, e := range snap.Events {
		if e.Kind != "trigger-ignored" {
			t.Fatalf("unexpected event kind %q", e.Kind)
		}
	}
}

func TestBeginIdempotentIdentity(t *testing.T) {
	c, _ := testCoordinator(t)
	id1, owner1 := c.Begin(TriggerSignal, "first")
	id2, owner2 := c.Begin(TriggerFuseExit, "second")
	if !owner1 || owner2 {
		t.Fatalf("owner flags wrong: %v %v", owner1, owner2)
	}
	if id1.ID != id2.ID || id1.Pid != id2.Pid {
		t.Fatal("identity mismatch across Begin calls")
	}
}

func TestRunPhaseOrderAndIdempotency(t *testing.T) {
	c, _ := testCoordinator(t)
	c.Begin(TriggerFuseExit, "unmount")

	var runs int64
	fn := func(ctx context.Context) error {
		atomic.AddInt64(&runs, 1)
		return nil
	}

	// Out of order: flushing before freezing/draining.
	if err := c.RunPhase(PhaseFlushing, time.Second, fn); !errors.Is(err, ErrPhaseOutOfOrder) {
		t.Fatalf("expected ErrPhaseOutOfOrder, got %v", err)
	}

	for _, p := range OrderedPhases {
		if err := c.RunPhase(p, time.Second, fn); err != nil {
			t.Fatalf("phase %s failed: %v", p, err)
		}
	}
	if runs != int64(len(OrderedPhases)) {
		t.Fatalf("expected %d phase runs, got %d", len(OrderedPhases), runs)
	}

	// Re-entry never re-executes.
	if err := c.RunPhase(PhaseFlushing, time.Second, func(context.Context) error {
		t.Fatal("phase fn must not re-run")
		return nil
	}); err != nil {
		t.Fatalf("idempotent phase returned %v", err)
	}
	if err := c.SkipPhase(PhaseFreezing, "again"); err != nil {
		t.Fatalf("idempotent skip returned %v", err)
	}
	if runs != int64(len(OrderedPhases)) {
		t.Fatalf("phase re-executed: %d", runs)
	}
}

func TestRunPhasesBeforeBegin(t *testing.T) {
	c, _ := testCoordinator(t)
	if err := c.RunPhase(PhaseFreezing, time.Second, nil); !errors.Is(err, ErrShutdownNotStarted) {
		t.Fatalf("expected ErrShutdownNotStarted, got %v", err)
	}
}

func TestRunPhaseTimeoutAllowsNextPhase(t *testing.T) {
	c, _ := testCoordinator(t)
	c.Begin(TriggerSignal, "sigterm")

	if err := c.RunPhase(PhaseFreezing, time.Second, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	slow := func(ctx context.Context) error {
		select {} // blocks forever; deadline from RunPhase must release the coordinator
	}
	start := time.Now()
	err := c.RunPhase(PhaseDraining, 20*time.Millisecond, slow)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("phase not bounded: %v", elapsed)
	}

	// Next phase still runnable.
	if err := c.RunPhase(PhaseFlushing, time.Second, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("next phase blocked after timeout: %v", err)
	}
	var drainingTimedOut bool
	for _, r := range c.Snapshot().Results {
		if r.Phase == PhaseDraining {
			drainingTimedOut = r.TimedOut
		}
	}
	if !drainingTimedOut {
		t.Fatal("draining phase not marked timed out")
	}
}

func TestEscalateCancelsContext(t *testing.T) {
	c, _ := testCoordinator(t)
	c.Begin(TriggerSignal, "first")

	release := make(chan struct{})
	go func() {
		_ = c.RunPhase(PhaseFreezing, time.Second, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		})
	}()

	time.Sleep(30 * time.Millisecond)
	start := time.Now()
	gen := c.Escalate("second signal")
	if gen != 2 {
		t.Fatalf("expected generation 2, got %d", gen)
	}
	if c.Escalate("third signal") != 3 {
		t.Fatal("escalation generation not bumped")
	}

	// forcedCh closes once and stays closed.
	select {
	case <-c.ForcedCh():
	case <-time.After(time.Second):
		t.Fatal("forced channel not closed")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("escalation did not cancel waiting phase promptly")
	}
	if !c.Forced() {
		t.Fatal("Forced() false after Escalate")
	}
	close(release)
}

func TestEscalateBeforeBegin(t *testing.T) {
	c, _ := testCoordinator(t)
	if c.Escalate("lonely") != 0 {
		t.Fatal("escalation before Begin must return 0")
	}
	select {
	case <-c.ForcedCh():
		t.Fatal("forced channel must not close before Begin")
	default:
	}
}

func TestFinishAndExitCode(t *testing.T) {
	c, _ := testCoordinator(t)
	c.Begin(TriggerSignal, "sigterm")

	c.SetOutcome(Outcome{UncommittedCount: 2, DataUnsafe: true})
	c.SetOutcome(Outcome{UncommittedCount: 1}) // cannot downgrade
	if c.ExitCode() != ExitDataUnsafe {
		t.Fatalf("expected %d, got %d", ExitDataUnsafe, c.ExitCode())
	}

	c.Finish()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done not closed")
	}
	c.Finish() // idempotent
}

func TestCleanExitCode(t *testing.T) {
	c, _ := testCoordinator(t)
	c.Begin(TriggerFuseExit, "unmount")
	if c.ExitCode() != ExitOK {
		t.Fatalf("clean shutdown exit code = %d", c.ExitCode())
	}
	c.Finish()
}

func TestSkipPhaseCompletesSequence(t *testing.T) {
	c, _ := testCoordinator(t)
	c.Begin(TriggerFuseExit, "lazy unmount")
	for _, p := range OrderedPhases {
		if err := c.SkipPhase(p, "noop"); err != nil {
			t.Fatalf("skip %s: %v", p, err)
		}
	}
	if got := c.Snapshot().Current; got != PhaseCacheCleaning {
		t.Fatalf("current = %s", got)
	}
	c.Finish()
	if got := c.Snapshot().Current; got != StateDone {
		t.Fatalf("current after finish = %s", got)
	}
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.OpDrainTimeout != DefaultOpDrainTimeout ||
		cfg.FlushTimeout != DefaultFlushTimeout ||
		cfg.ComponentStopTimeout != DefaultComponentStopTimeout ||
		cfg.MonitorStopTimeout != DefaultMonitorStopTimeout ||
		cfg.RecoveryPolicy != RecoveryRecover {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	bad := cfg
	bad.FlushTimeout = -1
	if err := bad.Validate(); err == nil {
		t.Fatal("negative timeout accepted")
	}

	bad = cfg
	bad.RecoveryPolicy = "whatever"
	if err := bad.Validate(); err == nil {
		t.Fatal("invalid policy accepted")
	}
}

func TestLoadConfigWithoutSection(t *testing.T) {
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RecoveryPolicy != RecoveryRecover {
		t.Fatalf("default policy = %s", cfg.RecoveryPolicy)
	}
}
