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

package shutdown

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common/log"
)

// Trigger identifies who started a shutdown.
type Trigger string

const (
	// TriggerSignal : a terminal signal (SIGINT/SIGTERM/SIGQUIT) was received.
	TriggerSignal Trigger = "signal"
	// TriggerParentExit : the parent daemonizer process exited.
	TriggerParentExit Trigger = "parent-exit"
	// TriggerFuseExit : the FUSE session returned naturally (external
	// fusermount -u / -z).
	TriggerFuseExit Trigger = "fuse-exit"
)

// Phase is one stage of the ordered shutdown sequence. Phases advance strictly
// in the order declared by OrderedPhases.
type Phase string

const (
	PhaseFreezing          Phase = "freezing"
	PhaseDraining          Phase = "draining"
	PhaseFlushing          Phase = "flushing"
	PhaseFuseClosing       Phase = "fuse-closing"
	PhaseComponentStopping Phase = "component-stopping"
	PhaseMonitorStopping   Phase = "monitor-stopping"
	PhaseCacheCleaning     Phase = "cache-cleaning"
)

// StateDone marks the terminal coordinator state after Finish.
const StateDone Phase = "done"

// OrderedPhases is the single allowed shutdown order.
var OrderedPhases = []Phase{
	PhaseFreezing,
	PhaseDraining,
	PhaseFlushing,
	PhaseFuseClosing,
	PhaseComponentStopping,
	PhaseMonitorStopping,
	PhaseCacheCleaning,
}

// Identity uniquely tags one shutdown attempt. Concurrent triggers never
// create a second identity; they either lose the owner race or escalate the
// existing shutdown.
type Identity struct {
	ID         string    `json:"id"`
	Trigger    Trigger   `json:"trigger"`
	Reason     string    `json:"reason"`
	Pid        int       `json:"pid"`
	Generation int       `json:"generation"`
	StartedAt  time.Time `json:"started-at"`
}

// PhaseResult is the observable record of one phase execution.
type PhaseResult struct {
	Phase    Phase     `json:"phase"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end,omitempty"`
	TimedOut bool      `json:"timed-out,omitempty"`
	Err      string    `json:"error,omitempty"`
}

// Event records non-owner triggers and escalations against an active shutdown.
type Event struct {
	At         time.Time `json:"at"`
	Kind       string    `json:"kind"` // "trigger-ignored" | "escalated"
	Source     Trigger   `json:"source,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Generation int       `json:"generation,omitempty"`
}

// Outcome summarizes the shutdown result carried into the exit code and the
// stop status file.
type Outcome struct {
	Forced             bool `json:"forced,omitempty"`
	DataUnsafe         bool `json:"data-unsafe,omitempty"`
	UncommittedCount   int  `json:"uncommitted-count,omitempty"`
	UnrecoverableCount int  `json:"unrecoverable-count,omitempty"`
}

// Snapshot is a point-in-time, lock-free copy of coordinator state.
type Snapshot struct {
	Identity Identity      `json:"identity"`
	Current  Phase         `json:"current-phase"`
	Forced   bool          `json:"forced"`
	Finished bool          `json:"finished"`
	Results  []PhaseResult `json:"results"`
	Events   []Event       `json:"events"`
	Outcome  Outcome       `json:"outcome"`
}

// ErrShutdownNotStarted is returned when phase APIs are used before Begin.
var ErrShutdownNotStarted = errors.New("shutdown has not started")

// ErrPhaseOutOfOrder is returned when phases are requested out of sequence.
var ErrPhaseOutOfOrder = errors.New("shutdown phase out of order")

// Coordinator is the process-wide single owner of shutdown sequencing.
type Coordinator struct {
	cfg    Config
	logger Logger

	mu       sync.Mutex
	started  bool
	finished bool
	identity *Identity
	phaseIdx int // index into OrderedPhases of the last started phase; -1 before first
	results  map[Phase]*PhaseResult
	events   []Event
	outcome  Outcome

	graceCtx    context.Context
	graceCancel context.CancelFunc
	forcedCh    chan struct{}
	forceGen    int

	doneCh    chan struct{}
	startedCh chan struct{}
}

// Logger is the minimal logging surface used by the coordinator so tests can
// inject a capture logger.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Err(format string, args ...interface{})
	Crit(format string, args ...interface{})
}

type defaultLogger struct{}

func (defaultLogger) Debug(f string, a ...interface{}) { log.Debug(f, a...) }
func (defaultLogger) Info(f string, a ...interface{})  { log.Info(f, a...) }
func (defaultLogger) Warn(f string, a ...interface{})  { log.Warn(f, a...) }
func (defaultLogger) Err(f string, a ...interface{})   { log.Err(f, a...) }
func (defaultLogger) Crit(f string, a ...interface{})  { log.Crit(f, a...) }

// New creates a coordinator with the given config and the production logger.
func New(cfg Config) *Coordinator {
	return NewWithLogger(cfg, defaultLogger{})
}

// NewWithLogger creates a coordinator with an injected logger (tests).
func NewWithLogger(cfg Config, logger Logger) *Coordinator {
	return &Coordinator{
		cfg:       cfg,
		logger:    logger,
		phaseIdx:  -1,
		results:   make(map[Phase]*PhaseResult),
		forcedCh:  make(chan struct{}),
		doneCh:    make(chan struct{}),
		startedCh: make(chan struct{}),
	}
}

// Config returns the configured timeouts/policy.
func (c *Coordinator) Config() Config { return c.cfg }

// Started reports whether Begin already succeeded.
func (c *Coordinator) Started() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

// Forced reports whether the shutdown was escalated (second terminal signal).
func (c *Coordinator) Forced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.forceGen > 0
}

// ForcedCh is closed exactly once when Escalate is first called.
func (c *Coordinator) ForcedCh() <-chan struct{} { return c.forcedCh }

// Done is closed after Finish.
func (c *Coordinator) Done() <-chan struct{} { return c.doneCh }

// StartedCh is closed exactly once, when the first Begin succeeds.
func (c *Coordinator) StartedCh() <-chan struct{} { return c.startedCh }

// Begin claims ownership of the shutdown. The first successful caller becomes
// the owner (true). Concurrent or later trigger attempts return the existing
// identity and false; they are still recorded as events.
func (c *Coordinator) Begin(trigger Trigger, reason string) (*Identity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		c.events = append(c.events, Event{
			At:     time.Now(),
			Kind:   "trigger-ignored",
			Source: trigger,
			Reason: reason,
		})
		c.logger.Warn("Shutdown::Begin : trigger [%s: %s] ignored, shutdown id %s already owned",
			trigger, reason, c.identity.ID)
		return cloneIdentity(c.identity), false
	}

	c.started = true
	c.graceCtx, c.graceCancel = context.WithCancel(context.Background())
	c.identity = &Identity{
		ID:         newID(),
		Trigger:    trigger,
		Reason:     reason,
		Pid:        os.Getpid(),
		Generation: 1,
		StartedAt:  time.Now(),
	}
	close(c.startedCh)
	c.logger.Crit("Shutdown::Begin : shutdown id %s started by [%s: %s]",
		c.identity.ID, trigger, reason)
	return cloneIdentity(c.identity), true
}

// Identity returns a copy of the current identity (nil before Begin).
func (c *Coordinator) Identity() *Identity {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneIdentity(c.identity)
}

// Escalate moves an active shutdown into forced mode: the graceful context is
// cancelled immediately and the forced channel is closed. Every call past the
// first only bumps the generation counter (the signal layer restores default
// signal handling after the second escalation).
func (c *Coordinator) Escalate(reason string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		// Escalation before Begin is a programming error at the call site; do
		// not create an identity implicitly.
		c.logger.Err("Shutdown::Escalate : called before Begin [%s]", reason)
		return 0
	}

	c.forceGen++
	c.identity.Generation = 1 + c.forceGen
	c.events = append(c.events, Event{
		At:         time.Now(),
		Kind:       "escalated",
		Reason:     reason,
		Generation: c.identity.Generation,
	})
	if c.forceGen == 1 {
		close(c.forcedCh)
		if c.graceCancel != nil {
			c.graceCancel()
		}
	}
	c.logger.Crit("Shutdown::Escalate : shutdown id %s forced (generation %d): %s",
		c.identity.ID, c.identity.Generation, reason)
	return c.identity.Generation
}

// GraceContext returns the context cancelled on Escalate. Before Begin a
// background context is returned so incidental callers never get a nil ctx.
func (c *Coordinator) GraceContext() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.graceCtx != nil {
		return c.graceCtx
	}
	return context.Background()
}

// PhaseFunc executes the work of one phase under a bounded context.
type PhaseFunc func(ctx context.Context) error

// RunPhase runs exactly one ordered phase. The phase function receives a
// context that is cancelled when either the phase timeout expires or the
// shutdown is escalated. Re-entering a finished phase returns its recorded
// result without re-running fn; requesting an unexpected phase is an error.
func (c *Coordinator) RunPhase(phase Phase, timeout time.Duration, fn PhaseFunc) error {
	if alreadyDone, err := c.enterPhase(phase); err != nil {
		return err
	} else if alreadyDone {
		return nil
	}

	// Phase completion is bounded by wall-clock timeouts and must keep running
	// in order even after the second (forced) signal cancelled the graceful
	// context. Work that should be skipped on force (e.g. uploads) derives its
	// own context from GraceContext() inside the phase function.
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	c.logger.Info("Shutdown::RunPhase : [%s] started (timeout %v)", phase, timeout)
	start := time.Now()
	var err error

	// The worker only reports on a buffered channel; all coordinator state is
	// written from this owner goroutine. On timeout/cancel the late worker
	// send is absorbed by the buffer, so it cannot race the recorded result.
	done := make(chan error, 1)
	go func() {
		var result error
		defer func() {
			if r := recover(); r != nil {
				c.logger.Err("Shutdown::RunPhase : %s panic: %v", phase, r)
				result = fmt.Errorf("phase panic: %v", r)
			}
			done <- result
		}()
		result = fn(ctx)
	}()

	timer := time.NewTimer(timeout)
	if timeout <= 0 {
		timer.Stop()
	}
	select {
	case err = <-done:
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		err = context.DeadlineExceeded
	}
	end := time.Now()

	timedOut := errors.Is(err, context.DeadlineExceeded)
	c.finishPhase(phase, start, end, timedOut, err)

	switch {
	case err == nil:
		c.logger.Info("Shutdown::RunPhase : [%s] completed in %v", phase, end.Sub(start))
	case timedOut:
		c.logger.Err("Shutdown::RunPhase : [%s] timed out after %v [%v]", phase, end.Sub(start), err.Error())
	default:
		c.logger.Err("Shutdown::RunPhase : [%s] failed after %v [%v]", phase, end.Sub(start), err.Error())
	}
	return err
}

// SkipPhase records a phase as a no-op without executing a function. It keeps
// the observable phase sequence complete on fast paths (forced shutdown,
// cache disabled, FUSE session already gone).
func (c *Coordinator) SkipPhase(phase Phase, reason string) error {
	if alreadyDone, err := c.enterPhase(phase); err != nil {
		return err
	} else if alreadyDone {
		return nil
	}
	now := time.Now()
	c.finishPhase(phase, now, now, false, nil)
	c.logger.Info("Shutdown::SkipPhase : [%s] skipped [%s]", phase, reason)
	return nil
}

// enterPhase claims the next phase. alreadyDone==true means this phase was
// already executed and the caller must not re-run it.
func (c *Coordinator) enterPhase(phase Phase) (alreadyDone bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return false, ErrShutdownNotStarted
	}

	// Idempotent re-entry into an already completed phase.
	if res, ok := c.results[phase]; ok && !res.End.IsZero() {
		return true, nil
	}

	expected := c.phaseIdx + 1
	if expected >= len(OrderedPhases) || OrderedPhases[expected] != phase {
		return false, fmt.Errorf("%w: requested %s, expected %s", ErrPhaseOutOfOrder, phase,
			phaseAt(expected))
	}
	c.phaseIdx = expected
	c.results[phase] = &PhaseResult{Phase: phase, Start: time.Now()}
	return false, nil
}

func (c *Coordinator) finishPhase(phase Phase, start, end time.Time, timedOut bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	res, ok := c.results[phase]
	if !ok {
		res = &PhaseResult{Phase: phase}
		c.results[phase] = res
	}
	res.Start = start
	res.End = end
	res.TimedOut = timedOut
	if err != nil {
		res.Err = err.Error()
	}
}

// SetOutcome accumulates the final shutdown classification. Fields only ever
// flip to their unsafe values, so concurrent reporters cannot downgrade it.
func (c *Coordinator) SetOutcome(o Outcome) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if o.Forced {
		c.outcome.Forced = true
	}
	if o.DataUnsafe {
		c.outcome.DataUnsafe = true
	}
	if o.UncommittedCount > 0 {
		c.outcome.UncommittedCount = o.UncommittedCount
	}
	if o.UnrecoverableCount > 0 {
		c.outcome.UnrecoverableCount = o.UnrecoverableCount
	}
}

// Finish closes the shutdown. It is idempotent.
func (c *Coordinator) Finish() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.finished {
		return
	}
	c.finished = true
	c.phaseIdx = len(OrderedPhases)
	if c.graceCancel != nil {
		c.graceCancel()
	}
	c.logger.Crit("Shutdown::Finish : shutdown id %s done, outcome %+v",
		idOf(c.identity), c.outcome)
	close(c.doneCh)
}

// ExitCode maps the final outcome to the documented process exit code.
func (c *Coordinator) ExitCode() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outcome.DataUnsafe ||
		c.outcome.UncommittedCount > 0 ||
		c.outcome.UnrecoverableCount > 0 {
		return ExitDataUnsafe
	}
	return ExitOK
}

// Snapshot returns a detached copy of the current state for status files/tests.
func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	snap := Snapshot{
		Current:  phaseAt(c.phaseIdx),
		Forced:   c.forceGen > 0,
		Finished: c.finished,
		Outcome:  c.outcome,
	}
	if c.identity != nil {
		snap.Identity = *cloneIdentity(c.identity)
	}
	for _, p := range OrderedPhases {
		if r, ok := c.results[p]; ok {
			cp := *r
			snap.Results = append(snap.Results, cp)
		}
	}
	snap.Events = append(snap.Events, c.events...)
	return snap
}

func phaseAt(idx int) Phase {
	if idx < 0 {
		return Phase("idle")
	}
	if idx >= len(OrderedPhases) {
		return StateDone
	}
	return OrderedPhases[idx]
}

func idOf(id *Identity) string {
	if id == nil {
		return "-"
	}
	return id.ID
}

func cloneIdentity(id *Identity) *Identity {
	if id == nil {
		return nil
	}
	cp := *id
	return &cp
}

// newID returns an opaque random shutdown identifier.
func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// rand.Read failing is not practically recoverable; fall back to time.
		return fmt.Sprintf("s%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
