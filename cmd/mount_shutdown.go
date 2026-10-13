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
	"errors"
	"os"
	"sync"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
	"github.com/Azure/azure-storage-fuse/v2/internal/recovery"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// Discovered through pipeline.ByName("libfuse"); keeps the cgo-free cmd
// package compiling without libfuse headers.
type admissionFreezer interface {
	FreezeAdmissions()
}

type fuseSessionCloser interface {
	CloseFuseSession() error
}

// monitorHandle is the optional bfusemon process controller (Task 9).
type monitorHandle interface {
	// Shutdown asks the monitor to exit gracefully; force skips straight to
	// SIGKILL on the second-signal path.
	Shutdown(ctx context.Context, force bool) error
}

// shutdownRuntime executes the ordered shutdown phases once.
type shutdownRuntime struct {
	coord     *shutdown.Coordinator
	pipeline  *internal.Pipeline
	monitor   monitorHandle
	mountPath string
	pipes     []string // named pipes to unlink after monitor shutdown
}

// runPhases drives the full ordered sequence. startErr/startAlreadyExited
// describe whether pipeline.Start (fuse_main) already returned before the
// coordinator owned the shutdown (external unmount path).
func (rt *shutdownRuntime) runPhases(startErr error, startAlreadyExited bool,
	startCh <-chan error) int {

	cfg := rt.coord.Config()

	// ---- freezing: reject every new mutation (Go + native C path) ----
	_ = rt.coord.RunPhase(shutdown.PhaseFreezing, cfg.ComponentStopTimeout,
		func(ctx context.Context) error {
			if lf := rt.pipeline.ByName("libfuse"); lf != nil {
				if freezer, ok := lf.(admissionFreezer); ok {
					freezer.FreezeAdmissions()
				}
			}
			return nil
		})

	// ---- draining: wait for in-flight FUSE ops to reach safe points ----
	_ = rt.coord.RunPhase(shutdown.PhaseDraining, cfg.OpDrainTimeout,
		func(ctx context.Context) error {
			if handlemap.Drain(cfg.OpDrainTimeout) {
				return nil
			}
			log.Warn("Shutdown::draining : %d FUSE op(s) still in flight after %v, continuing",
				handlemap.InFlightOps(), cfg.OpDrainTimeout)
			return nil
		})

	// ---- flushing: bounded upload/commit of dirty handles ----
	var (
		itemsMu sync.Mutex
		items   []internal.UncommittedItem
	)
	_ = rt.coord.RunPhase(shutdown.PhaseFlushing, cfg.FlushTimeout,
		func(_ context.Context) error {
			// Drain work is bounded by the flush timeout and is cancelled
			// immediately by the second (forced) signal via GraceContext.
			flushCtx, cancelFlush := context.WithTimeout(rt.coord.GraceContext(), cfg.FlushTimeout)
			defer cancelFlush()
			for _, drainer := range rt.pipeline.Drainers() {
				its, err := drainer.DrainForShutdown(flushCtx)
				if err != nil {
					log.Err("Shutdown::flushing : drainer returned error [%v]", err)
				}
				// A drain goroutine can keep running after the phase deadline;
				// guard the slice against its late completion.
				itemsMu.Lock()
				items = append(items, its...)
				itemsMu.Unlock()
			}
			return nil
		})
	itemsMu.Lock()
	items = append([]internal.UncommittedItem(nil), items...)
	itemsMu.Unlock()

	// ---- fuse-closing: terminate session so fuse_main returns ----
	_ = rt.coord.RunPhase(shutdown.PhaseFuseClosing, cfg.ComponentStopTimeout,
		func(ctx context.Context) error {
			if startAlreadyExited {
				log.Info("Shutdown::fuse-closing : FUSE session already exited (external unmount)")
				return nil
			}
			if lf := rt.pipeline.ByName("libfuse"); lf != nil {
				if closer, ok := lf.(fuseSessionCloser); ok {
					_ = closer.CloseFuseSession()
				}
			}
			select {
			case startErr = <-startCh:
				log.Info("Shutdown::fuse-closing : fuse_main returned (%v)", startErr)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

	// Cache components must defer their wipe to the cache-cleaning phase.
	for _, cleaner := range rt.pipeline.DeferredCleaners() {
		cleaner.DeferCacheCleanup()
	}

	// ---- component-stopping: bounded Stop in pipeline order ----
	stopErr := rt.coord.RunPhase(shutdown.PhaseComponentStopping, cfg.ComponentStopTimeout,
		func(ctx context.Context) error {
			return rt.pipeline.StopWithDeadline(cfg.ComponentStopTimeout)
		})

	// ---- monitor-stopping: graceful bfusemon termination ----
	_ = rt.coord.RunPhase(shutdown.PhaseMonitorStopping, cfg.MonitorStopTimeout,
		func(ctx context.Context) error {
			for _, p := range rt.pipes {
				_ = os.Remove(p)
			}
			if rt.monitor == nil {
				return nil
			}
			return rt.monitor.Shutdown(ctx, rt.coord.Forced())
		})

	// ---- cache-cleaning: protection-aware wipe ----
	_ = rt.coord.RunPhase(shutdown.PhaseCacheCleaning, cfg.ComponentStopTimeout,
		func(ctx context.Context) error {
			var errs []error
			for _, cleaner := range rt.pipeline.DeferredCleaners() {
				if err := cleaner.CleanupCache(); err != nil {
					errs = append(errs, err)
				}
			}
			return errors.Join(errs...)
		})

	// ---- accounting: outcome first so status/exit code reflect it ----
	// A forced (second-signal) shutdown skipped uploads, so it is always
	// reported as data-unsafe even when no concrete item could be enumerated.
	dataUnsafe := rt.coord.Forced() || len(items) > 0 || stopErr != nil || rt.anyPhaseFailed()
	rt.coord.SetOutcome(shutdown.Outcome{
		Forced:             rt.coord.Forced(),
		DataUnsafe:         dataUnsafe,
		UncommittedCount:   len(items),
		UnrecoverableCount: countReason(items, internal.UncommittedInMemoryLost),
	})

	// ---- recovery manifests and machine-readable status file ----
	rt.persistArtifacts(items)

	rt.coord.Finish()
	return rt.coord.ExitCode()
}

func (rt *shutdownRuntime) anyPhaseFailed() bool {
	for _, r := range rt.coord.Snapshot().Results {
		if r.Err != "" {
			return true
		}
	}
	return false
}

// persistArtifacts writes one recovery manifest per cache component (only
// recoverable items) and the machine-readable stop status file.
func (rt *shutdownRuntime) persistArtifacts(items []internal.UncommittedItem) {
	workDir := common.DefaultWorkDir

	byComponent := make(map[string][]internal.UncommittedItem)
	recoverable := make(map[string][]internal.UncommittedItem)
	for _, it := range items {
		byComponent[it.Component] = append(byComponent[it.Component], it)
		if it.Reason != internal.UncommittedInMemoryLost {
			recoverable[it.Component] = append(recoverable[it.Component], it)
		}
	}

	id := ""
	if ident := rt.coord.Identity(); ident != nil {
		id = ident.ID
	}

	for component, its := range recoverable {
		manifestPath := recovery.ManifestPath(workDir, rt.mountPath, component)
		cachePath := ""
		if c := rt.pipeline.ByName(component); c != nil {
			if provider, ok := c.(internal.CacheRootProvider); ok {
				cachePath = provider.CacheRoot()
			}
		}
		if _, err := recovery.MergeItems(manifestPath, rt.mountPath, cachePath, component, id, its); err != nil {
			log.Err("Shutdown::persistArtifacts : failed to write manifest %s [%v]", manifestPath, err)
		} else {
			log.Crit("Shutdown::persistArtifacts : %d recoverable record(s) preserved in %s",
				len(its), manifestPath)
		}
	}

	status := recovery.StopStatus{
		MountPath: rt.mountPath,
		Pid:       os.Getpid(),
		ExitCode:  rt.coord.ExitCode(),
		Shutdown:  rt.coord.Snapshot(),
		Items:     items,
	}
	statusPath := recovery.StatusPath(workDir, rt.mountPath)
	if err := recovery.SaveStatus(statusPath, &status); err != nil {
		log.Err("Shutdown::persistArtifacts : failed to write status %s [%v]", statusPath, err)
	}
}

func countReason(items []internal.UncommittedItem, reason string) int {
	n := 0
	for _, it := range items {
		if it.Reason == reason {
			n++
		}
	}
	return n
}
