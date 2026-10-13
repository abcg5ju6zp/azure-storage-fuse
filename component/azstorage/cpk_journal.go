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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common/log"
)

// cpkObjectState is the per-object migration state persisted in the journal.
type cpkObjectState string

const (
	// cpkObjPending: object was observed encrypted with an old key and still has
	// to be published under the active key.
	cpkObjPending cpkObjectState = "Pending"
	// cpkObjInProgress: a migration publish for the object is running. Recovered
	// back to Pending on restart because the result is unknown.
	cpkObjInProgress cpkObjectState = "InProgress"
	// cpkObjUnconfirmed: the publish/write-back response was uncertain (timeout,
	// connection reset). The remote object version and encryption identifier must
	// be checked before the object is retried or declared done.
	cpkObjUnconfirmed cpkObjectState = "Unconfirmed"
	// cpkObjFailed: the last migration attempt failed definitively; the record
	// keeps the error and is retried on later sweeps.
	cpkObjFailed cpkObjectState = "Failed"
	// cpkObjDone: remote confirmation proved the object is now under the active
	// key. Done records are pruned from the journal after being counted.
	cpkObjDone cpkObjectState = "Done"
)

// cpkJournalRecord is one object's migration record. Key material is never
// persisted here, only key sha identifiers.
type cpkJournalRecord struct {
	Path       string          `json:"path"`
	OldSHA     string          `json:"old_sha"`
	NewSHA     string          `json:"new_sha"`
	SourceETag string          `json:"source_etag,omitempty"`
	TargetETag string          `json:"target_etag,omitempty"`
	State      cpkObjectState  `json:"state"`
	Attempts   int             `json:"attempts,omitempty"`
	LastError  string          `json:"last_error,omitempty"`
	NextAttemptAt             time.Time `json:"next_attempt_at,omitempty"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// cpkJournalData is the on-disk schema of the rotation state file.
type cpkJournalData struct {
	Version            int       `json:"version"`
	Scope              string    `json:"scope"`
	RotationID         uint64    `json:"rotation_id"`
	ActiveSHA          string    `json:"active_sha"`
	StartedAt          time.Time `json:"started_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	CumulativeMigrated int64     `json:"cumulative_migrated"`
	Objects            []cpkJournalRecord `json:"objects"`
}

const cpkJournalVersion = 1

// cpkJournal persists queryable rotation progress so partial failures and
// process restarts can resume without re-publishing confirmed objects.
type cpkJournal struct {
	mu       sync.Mutex
	filePath string
	data     cpkJournalData
}

// cpkJournalFileName returns the state file name for one account/container scope.
func cpkJournalFileName(statePath, scope string) string {
	name := fmt.Sprintf("cpk-rotation-%s.json", sanitizeScope(scope))
	if statePath == "" {
		return name
	}
	return filepath.Join(statePath, name)
}

// openCPKJournal opens (or creates) the state file, loading any previously
// persisted rotation progress. In-progress records from a crashed process are
// normalised back to Pending because their outcome is unknown.
func openCPKJournal(statePath, scope string) (*cpkJournal, error) {
	if statePath != "" {
		if err := os.MkdirAll(statePath, 0700); err != nil {
			return nil, fmt.Errorf("failed to create cpk state directory %s [%w]", statePath, err)
		}
	}
	j := &cpkJournal{
		filePath: cpkJournalFileName(statePath, scope),
		data: cpkJournalData{
			Version: cpkJournalVersion,
			Scope:   scope,
			Objects: make([]cpkJournalRecord, 0),
		},
	}

	raw, err := os.ReadFile(j.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return j, nil
		}
		return nil, fmt.Errorf("failed to read cpk state file %s [%w]", j.filePath, err)
	}

	var loaded cpkJournalData
	if err := json.Unmarshal(raw, &loaded); err != nil {
		return nil, fmt.Errorf("cpk state file %s is corrupt [%w]; resolve or remove it before mounting", j.filePath, err)
	}
	if loaded.Version > cpkJournalVersion {
		return nil, fmt.Errorf("cpk state file %s was written by a newer version (%d)", j.filePath, loaded.Version)
	}
	if loaded.Scope != "" && loaded.Scope != scope {
		return nil, fmt.Errorf("cpk state file %s belongs to scope %s, expected %s", j.filePath, loaded.Scope, scope)
	}

	for i := range loaded.Objects {
		// A process died while publishing: never assume the result, recover as
		// unconfirmed so version + encryption identifier are checked first.
		if loaded.Objects[i].State == cpkObjInProgress {
			loaded.Objects[i].State = cpkObjUnconfirmed
			loaded.Objects[i].LastError = "recovered after restart while publish was in progress"
		}
	}
	loaded.Objects = sortJournalRecords(loaded.Objects)
	j.data = loaded
	log.Crit("openCPKJournal : loaded cpk rotation state from %s, rotation %d, %d unfinished object records",
		j.filePath, loaded.RotationID, len(loaded.Objects))
	return j, nil
}

func sortJournalRecords(in []cpkJournalRecord) []cpkJournalRecord {
	sort.Slice(in, func(i, j int) bool { return in[i].Path < in[j].Path })
	return in
}

// persistLocked atomically replaces the state file (temp file + rename) so a
// crash can never leave a half-written journal. Caller holds j.mu.
func (j *cpkJournal) persistLocked() error {
	j.data.Version = cpkJournalVersion
	j.data.UpdatedAt = time.Now()
	j.data.Objects = sortJournalRecords(j.data.Objects)

	raw, err := json.MarshalIndent(&j.data, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(j.filePath)
	tmp, err := os.CreateTemp(dir, ".cpk-rotation-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, j.filePath)
}

// beginRotation marks a new rotation generation. Records of older rotations are
// dropped only once they are in a terminal state; unfinished records stay to be
// reconciled against the new active key.
func (j *cpkJournal) beginRotation(rotationID uint64, activeSHA, previousActive string, retired []string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.data.StartedAt.IsZero() {
		j.data.StartedAt = time.Now()
	}
	j.data.RotationID = rotationID
	j.data.ActiveSHA = activeSHA

	kept := make([]cpkJournalRecord, 0, len(j.data.Objects))
	for _, r := range j.data.Objects {
		if r.State == cpkObjDone {
			j.data.CumulativeMigrated++
			continue
		}
		// Any object left from a previous rotation is re-targeted at the new
		// active key and its result must be reconfirmed.
		r.NewSHA = activeSHA
		if r.State == cpkObjInProgress {
			r.State = cpkObjUnconfirmed
		}
		kept = append(kept, r)
	}
	j.data.Objects = kept
	return j.persistLocked()
}

// Get returns the record for a path.
func (j *cpkJournal) Get(path string) (cpkJournalRecord, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, r := range j.data.Objects {
		if r.Path == path {
			return r, true
		}
	}
	return cpkJournalRecord{}, false
}

// Upsert inserts or replaces the record for a path.
func (j *cpkJournal) Upsert(rec cpkJournalRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	rec.UpdatedAt = time.Now()
	for i, r := range j.data.Objects {
		if r.Path == rec.Path {
			j.data.Objects[i] = rec
			return j.persistLocked()
		}
	}
	j.data.Objects = append(j.data.Objects, rec)
	return j.persistLocked()
}

// Delete removes a path (object deleted or renamed away).
func (j *cpkJournal) Delete(path string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := j.data.Objects[:0]
	for _, r := range j.data.Objects {
		if r.Path == path {
			continue
		}
		out = append(out, r)
	}
	if len(out) == len(j.data.Objects) {
		return nil
	}
	j.data.Objects = out
	return j.persistLocked()
}

// RenamePath rewrites a record after a rename while preserving migration state.
func (j *cpkJournal) RenamePath(src, dst string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i := range j.data.Objects {
		if j.data.Objects[i].Path == src {
			j.data.Objects[i].Path = dst
			j.data.Objects[i].UpdatedAt = time.Now()
			return j.persistLocked()
		}
	}
	return nil
}

// MarkDone records a remotely confirmed migration and counts it before pruning.
func (j *cpkJournal) MarkDone(path, targetETag string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i := range j.data.Objects {
		if j.data.Objects[i].Path == path {
			j.data.Objects = append(j.data.Objects[:i], j.data.Objects[i+1:]...)
			j.data.CumulativeMigrated++
			return j.persistLocked()
		}
	}
	// Object was not tracked (e.g. confirmed by recovery sweep): still count it.
	j.data.CumulativeMigrated++
	return j.persistLocked()
}

// activeRecords returns a copy of all non-terminal records.
func (j *cpkJournal) activeRecords() []cpkJournalRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]cpkJournalRecord, len(j.data.Objects))
	copy(out, j.data.Objects)
	return out
}

// Snapshot returns a copy of the persisted state for status/reporting.
func (j *cpkJournal) Snapshot() cpkJournalData {
	j.mu.Lock()
	defer j.mu.Unlock()
	cp := j.data
	cp.Objects = make([]cpkJournalRecord, len(j.data.Objects))
	copy(cp.Objects, j.data.Objects)
	return cp
}

// ActiveSHA returns the active sha recorded in the journal.
func (j *cpkJournal) ActiveSHA() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.data.ActiveSHA
}

// RotationID returns the persisted rotation generation.
func (j *cpkJournal) RotationID() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.data.RotationID
}

// CumulativeMigrated returns the number of confirmed migrations across restarts.
func (j *cpkJournal) CumulativeMigrated() int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.data.CumulativeMigrated
}
