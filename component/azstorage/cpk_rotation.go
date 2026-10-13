/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |-----| |----  |-----  ------
   | --- |  |      |     | |     | |---- |     | |-----| |-----  ------
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

package azstorage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
)

// cpkRemoteObject is one object observed during a migration sweep.
type cpkRemoteObject struct {
	name string
	// sha is the customer-provided key identifier returned by the service;
	// empty for objects not encrypted with a customer-provided key.
	sha  string
	etag string
	// isDir marks a namespace directory (HNS) whose metadata cannot safely be
	// rewritten through the blob endpoint.
	isDir bool
}

// cpkRemoteBackend is the storage-facing side of migration. Production code
// implements it on BlockBlob; tests supply an in-memory fake.
type cpkRemoteBackend interface {
	cpkIdentityResolver

	// enumerateCPKObjects lists every object under the mounted prefix together
	// with its authoritative encryption key identifier and version (etag).
	enumerateCPKObjects() ([]cpkRemoteObject, error)

	// migrateObject republishes one object under newSHA. It must read with the
	// object's current key, preserve metadata/content settings and publish only
	// when the remote version still equals srcETag (If-Match). The returned
	// etag identifies the new version. definitive=false with an error means the
	// outcome is unknown and must be confirmed remotely before any retry.
	migrateObject(obj cpkRemoteObject, newSHA, srcETag string) (newETag string, err error)
}

// cpkSweepSummary holds the counters of one migration pass.
type cpkSweepSummary struct {
	remoteTotal        int
	migrating          int
	unmanaged          int // encrypted with a key not configured on this mount
	skippedDirectories int
	openVersions       int // deferred because an open handle pins the object
	processed          int
	confirmed          int
	failed             int
	unconfirmed        int
	cumulative         int64
}

// cpkMigrator runs background, restart-safe re-encryption of objects that still
// use a retained key.
type cpkMigrator struct {
	mgr        *CPKManager
	backend    cpkRemoteBackend
	interval   time.Duration
	maxPerTick int

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

const (
	cpkDefaultMigrationInterval = 30 * time.Second
	cpkDefaultObjectsPerTick    = 500
	cpkFailureBackoff           = 60 * time.Second
)

func newCPKMigrator(mgr *CPKManager, backend cpkRemoteBackend, interval time.Duration, maxPerTick int) *cpkMigrator {
	if interval <= 0 {
		interval = cpkDefaultMigrationInterval
	}
	if maxPerTick <= 0 {
		maxPerTick = cpkDefaultObjectsPerTick
	}
	return &cpkMigrator{
		mgr:        mgr,
		backend:    backend,
		interval:   interval,
		maxPerTick: maxPerTick,
	}
}

// start launches the background sweep loop. It is a no-op when CPK is disabled
// or a sweep is already running.
func (mi *cpkMigrator) start() {
	if mi == nil || mi.mgr == nil || !mi.mgr.Enabled() {
		return
	}
	mi.mu.Lock()
	if mi.running {
		mi.mu.Unlock()
		return
	}
	mi.stopCh = make(chan struct{})
	mi.doneCh = make(chan struct{})
	mi.running = true
	mi.mu.Unlock()

	go func() {
		defer close(mi.doneCh)
		// Run one reconciliation shortly after mount/rotation so unconfirmed
		// objects are recovered without waiting a full interval.
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-mi.stopCh:
				return
			case <-timer.C:
				mi.safeRunOnce(context.Background())
				timer.Reset(mi.interval)
			}
		}
	}()
}

// stop signals the loop to terminate and waits for the in-flight sweep.
func (mi *cpkMigrator) stop() {
	if mi == nil {
		return
	}
	mi.mu.Lock()
	if !mi.running {
		mi.mu.Unlock()
		return
	}
	stop, done := mi.stopCh, mi.doneCh
	mi.running = false
	mi.mu.Unlock()
	close(stop)
	<-done
}

// trigger asks for an immediate sweep (e.g. right after a rotation starts).
func (mi *cpkMigrator) trigger() {
	if mi == nil {
		return
	}
	go mi.safeRunOnce(context.Background())
}

func (mi *cpkMigrator) safeRunOnce(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Err("cpkMigrator::safeRunOnce : panic recovered [%v]", r)
		}
	}()
	if err := mi.runOnce(ctx); err != nil {
		log.Err("cpkMigrator::safeRunOnce : migration sweep failed [%s]", err.Error())
	}
}

// runOnce performs one enumeration + reconciliation + bounded publish pass.
// It is exported through runOnce for direct use from unit tests.
func (mi *cpkMigrator) runOnce(_ context.Context) error {
	if mi.mgr.Phase() != cpkPhaseRotating {
		// Nothing to migrate. NewKeyRejected pauses the migration by design.
		return nil
	}
	activeSHA := mi.mgr.ActiveSHA()

	// 1. Recover objects whose last publish result is unknown from a previous
	//    crash or ambiguous response.
	summary := mi.recoverUnconfirmed(activeSHA)

	// 2. Authoritative enumeration of remote key identifiers.
	objects, err := mi.backend.enumerateCPKObjects()
	if err != nil {
		return err
	}
	refs := make(map[string]int)
	j := mi.mgr.Journal()
	for _, obj := range objects {
		summary.remoteTotal++
		if obj.isDir {
			// HNS directory metadata is not rewritten through the blob path;
			// it honestly keeps the old key referenced.
			if obj.sha != "" && obj.sha != activeSHA {
				summary.skippedDirectories++
				refs[obj.sha]++
			}
			continue
		}
		mi.mgr.RecordRemoteIdentity(obj.name, obj.sha, obj.etag)
		if obj.sha == "" || obj.sha == activeSHA {
			continue
		}
		if _, err := mi.mgr.KeyForSHA(obj.sha); err != nil {
			// Object uses a key this mount does not hold: cannot migrate or
			// read it, but enumeration keeps revocation honest.
			summary.unmanaged++
			refs[obj.sha]++
			continue
		}
		refs[obj.sha]++
		mi.enqueue(obj, activeSHA)
	}
	mi.mgr.noteRemoteSweep(refs)

	// 3. Process a bounded slice of unfinished records.
	records := j.activeRecords()
	budget := mi.maxPerTick
	now := time.Now()
	for _, rec := range records {
		if budget <= 0 {
			break
		}
		if rec.State == cpkObjDone {
			continue
		}
		if rec.State == cpkObjFailed && rec.NextAttemptAt.After(now) {
			continue
		}
		if rec.State == cpkObjUnconfirmed {
			// handled in step 1
			continue
		}
		if mi.mgr.HasOpenVersion(rec.Path) {
			summary.openVersions++
			continue
		}

		// Reconcile the source version before publishing: the object may have
		// been modified since it was enqueued.
		sha, etag, ierr := mi.backend.remoteIdentity(rec.Path)
		if ierr != nil {
			log.Warn("cpkMigrator::runOnce : identity lookup failed for %s [%s], deferred", rec.Path, ierr.Error())
			continue
		}
		if sha == activeSHA {
			// Another sweep already moved it.
			mi.mgr.RecordRemoteIdentity(rec.Path, sha, etag)
			_ = j.MarkDone(rec.Path, etag)
			summary.confirmed++
			continue
		}
		if sha == "" {
			// Object no longer CPK encrypted (e.g. rewritten by an external
			// writer): drop it from the rotation.
			mi.mgr.RecordRemoteIdentity(rec.Path, sha, etag)
			_ = j.Delete(rec.Path)
			continue
		}
		if sha != rec.OldSHA {
			// Object moved to a different retained key somehow; re-enqueue
			// under its actual identity.
			_ = j.Upsert(cpkJournalRecord{
				Path: rec.Path, OldSHA: sha, NewSHA: activeSHA,
				SourceETag: etag, State: cpkObjPending, UpdatedAt: time.Now(),
			})
			continue
		}
		if rec.SourceETag != "" && etag != "" && etag != rec.SourceETag {
			// Content changed since enqueue: reset the version guard, the
			// object stays on its current key until this tick's publish runs.
			rec.SourceETag = etag
		}
		if rec.SourceETag == "" {
			rec.SourceETag = etag
		}

		obj := cpkRemoteObject{name: rec.Path, sha: rec.OldSHA, etag: rec.SourceETag}
		budget--
		summary.processed++

		_ = j.Upsert(cpkJournalRecord{
			Path: rec.Path, OldSHA: rec.OldSHA, NewSHA: activeSHA,
			SourceETag: rec.SourceETag, State: cpkObjInProgress,
			Attempts: rec.Attempts, UpdatedAt: time.Now(),
		})

		newETag, perr := mi.backend.migrateObject(obj, activeSHA, rec.SourceETag)
		if perr != nil {
			if errors.Is(perr, ErrCPKKeyUnavailable) {
				_ = j.Upsert(withError(rec, cpkObjPending, "", "old key unavailable, deferred: "+perr.Error()))
				continue
			}
			if isDefinitiveCPKPublishError(perr) {
				if isConditionNotMet(perr) || isNotFound(perr) {
					// Source changed concurrently or the object is gone: next
					// sweep reconciles via enumeration.
					state := cpkObjPending
					if isNotFound(perr) {
						_ = j.Delete(rec.Path)
						mi.mgr.NotifyDeleted(rec.Path)
						continue
					}
					next := rec
					next.Attempts++
					_ = j.Upsert(withError(next, state, "", "source version changed: "+perr.Error()))
					continue
				}
				summary.failed++
				next := rec
				next.Attempts++
				next.NextAttemptAt = time.Now().Add(cpkFailureBackoff)
				_ = j.Upsert(withError(next, cpkObjFailed, "", perr.Error()))
				continue
			}
			// Response is ambiguous (timeout/connection): never retry blind and
			// never expose the object as new-key content until confirmed.
			summary.unconfirmed++
			_ = j.Upsert(withError(rec, cpkObjUnconfirmed, "", perr.Error()))
			continue
		}

		// Publish returned a version. Confirm with the remote version AND its
		// encryption identifier before declaring success.
		confSHA, confETag, cerr := mi.backend.remoteIdentity(rec.Path)
		if cerr != nil || confSHA != activeSHA || (newETag != "" && confETag != "" && confETag != newETag) {
			summary.unconfirmed++
			reason := "post-publish confirmation incomplete"
			if cerr != nil {
				reason = cerr.Error()
			}
			_ = j.Upsert(withError(rec, cpkObjUnconfirmed, newETag, reason))
			continue
		}

		mi.mgr.RecordRemoteIdentity(rec.Path, activeSHA, confETag)
		_ = j.MarkDone(rec.Path, confETag)
		summary.confirmed++
	}

	// 4. Update progress counters and the revocation view.
	summary.cumulative = j.CumulativeMigrated()
	summary.migrating = mi.countMigrating(refs, j)
	mi.mgr.noteSweep(summary)
	log.Crit("cpkMigrator::runOnce : rotation %s: remote=%d migrating=%d confirmed=%d unconfirmed=%d failed=%d openVersions=%d unmanaged=%d skippedDirs=%d cumulative=%d",
		mi.mgr.ActiveSHA(), summary.remoteTotal, summary.migrating, summary.confirmed,
		summary.unconfirmed, summary.failed, summary.openVersions, summary.unmanaged,
		summary.skippedDirectories, summary.cumulative)
	mi.maybeComplete(refs, j)
	return nil
}

// recoverUnconfirmed reconciles every Unconfirmed record using the authoritative
// remote identity. This is what makes ambiguous write-back responses and process
// restarts safe.
func (mi *cpkMigrator) recoverUnconfirmed(activeSHA string) cpkSweepSummary {
	var summary cpkSweepSummary
	j := mi.mgr.Journal()
	for _, rec := range j.activeRecords() {
		if rec.State != cpkObjUnconfirmed {
			continue
		}
		sha, etag, err := mi.backend.remoteIdentity(rec.Path)
		if err != nil {
			log.Warn("cpkMigrator::recoverUnconfirmed : %s still unresolved [%s]", rec.Path, err.Error())
			summary.unconfirmed++
			continue
		}
		switch sha {
		case activeSHA:
			mi.mgr.RecordRemoteIdentity(rec.Path, sha, etag)
			_ = j.MarkDone(rec.Path, etag)
			summary.confirmed++
		case "":
			_ = j.Delete(rec.Path)
		default:
			// Still under the old key (publish never landed): retry from a
			// clean pending state.
			next := rec
			next.State = cpkObjPending
			next.TargetETag = ""
			next.LastError = "recovered: object still on old key"
			next.SourceETag = etag
			_ = j.Upsert(next)
			summary.migrating++
		}
	}
	return summary
}

// enqueue makes sure an old-key object discovered during enumeration is tracked.
func (mi *cpkMigrator) enqueue(obj cpkRemoteObject, activeSHA string) {
	j := mi.mgr.Journal()
	if _, ok := j.Get(obj.name); ok {
		return
	}
	_ = j.Upsert(cpkJournalRecord{
		Path:       obj.name,
		OldSHA:     obj.sha,
		NewSHA:     activeSHA,
		SourceETag: obj.etag,
		State:      cpkObjPending,
		UpdatedAt:  time.Now(),
	})
}

// countMigrating counts remote objects still on old keys plus journal records
// not yet confirmed.
func (mi *cpkMigrator) countMigrating(refs map[string]int, j *cpkJournal) int {
	n := 0
	for sha, c := range refs {
		if sha != mi.mgr.ActiveSHA() {
			n += c
		}
	}
	for _, r := range j.activeRecords() {
		if r.State == cpkObjPending || r.State == cpkObjInProgress ||
			r.State == cpkObjUnconfirmed || r.State == cpkObjFailed {
			n++
		}
	}
	return n
}

// maybeComplete reports completion only when the sweep proves no remote object
// and no open version references a retained key anymore.
func (mi *cpkMigrator) maybeComplete(refs map[string]int, j *cpkJournal) {
	status := mi.mgr.Status()
	if len(j.activeRecords()) > 0 {
		return
	}
	for _, k := range status.Keys {
		if k.Active {
			continue
		}
		if refs[k.SHA] > 0 || k.OpenHandles > 0 {
			return
		}
	}
	// Only flip to Complete after a real sweep ran while rotating.
	if len(status.Keys) > 1 || status.RotationID > 0 {
		mi.mgr.setPhase(cpkPhaseRotationComplete)
		log.Crit("cpkMigrator::maybeComplete : rotation %d complete; retained keys %v are reportable as revocable",
			status.RotationID, mi.mgr.RevocableKeys())
	}
}

func withError(rec cpkJournalRecord, state cpkObjectState, targetETag, reason string) cpkJournalRecord {
	rec.State = state
	rec.LastError = reason
	rec.TargetETag = targetETag
	if state == cpkObjFailed {
		rec.NextAttemptAt = time.Now().Add(cpkFailureBackoff)
	}
	rec.UpdatedAt = time.Now()
	return rec
}

// isDefinitiveCPKPublishError reports whether the service gave an unambiguous
// rejection. Transport/timeouts leave the server state unknown instead.
func isDefinitiveCPKPublishError(err error) bool {
	var respErr *azcore.ResponseError
	return errors.As(err, &respErr)
}

func isConditionNotMet(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return bloberror.Code(respErr.ErrorCode) == bloberror.ConditionNotMet
	}
	return false
}

func isNotFound(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return bloberror.Code(respErr.ErrorCode) == bloberror.BlobNotFound
	}
	return false
}
