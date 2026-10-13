//go:build !authtest
// +build !authtest

/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |-----| |----  |-----  ------
   | --- |  |      |     | |     | |---- |     | |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____
*/

package azstorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCPKJournalUpsertDeleteRenameAndReload(t *testing.T) {
	dir := t.TempDir()
	j, err := openCPKJournal(dir, "acct-cont")
	require.NoError(t, err)

	require.NoError(t, j.beginRotation(7, "newsha", "oldsha", []string{"oldsha"}))

	rec := cpkJournalRecord{Path: "a", OldSHA: "oldsha", NewSHA: "newsha", SourceETag: "e1", State: cpkObjPending}
	require.NoError(t, j.Upsert(rec))
	require.NoError(t, j.Upsert(cpkJournalRecord{Path: "b", OldSHA: "oldsha", NewSHA: "newsha", SourceETag: "e2", State: cpkObjInProgress}))

	got, ok := j.Get("a")
	require.True(t, ok)
	assert.Equal(t, "oldsha", got.OldSHA)

	require.NoError(t, j.RenamePath("a", "a2"))
	_, ok = j.Get("a")
	assert.False(t, ok)
	got, ok = j.Get("a2")
	assert.True(t, ok)

	require.NoError(t, j.Delete("b"))
	_, ok = j.Get("b")
	assert.False(t, ok)

	// Reopen: state must survive restart.
	j2, err := openCPKJournal(dir, "acct-cont")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), j2.RotationID())
	assert.Equal(t, "newsha", j2.ActiveSHA())
	_, ok = j2.Get("a2")
	assert.True(t, ok)
}

func TestCPKJournalRecoversInProgressOnRestart(t *testing.T) {
	dir := t.TempDir()
	j, err := openCPKJournal(dir, "acct-cont")
	require.NoError(t, err)
	require.NoError(t, j.beginRotation(1, "newsha", "oldsha", nil))
	require.NoError(t, j.Upsert(cpkJournalRecord{Path: "x", OldSHA: "oldsha", NewSHA: "newsha", State: cpkObjInProgress}))

	j2, err := openCPKJournal(dir, "acct-cont")
	require.NoError(t, err)
	rec, ok := j2.Get("x")
	require.True(t, ok)
	assert.Equal(t, cpkObjUnconfirmed, rec.State, "an in-flight publish is unknown after a crash")
}

func TestCPKJournalMarkDoneCountsAndPrunes(t *testing.T) {
	dir := t.TempDir()
	j, err := openCPKJournal(dir, "acct-cont")
	require.NoError(t, err)
	require.NoError(t, j.beginRotation(1, "newsha", "oldsha", nil))
	require.NoError(t, j.Upsert(cpkJournalRecord{Path: "x", OldSHA: "oldsha", NewSHA: "newsha", State: cpkObjPending}))
	require.NoError(t, j.MarkDone("x", "en"))
	assert.Equal(t, int64(1), j.CumulativeMigrated())
	_, ok := j.Get("x")
	assert.False(t, ok)

	// beginRotation at a later generation counts terminal records from the
	// previous rotation instead of resurrecting them.
	require.NoError(t, j.beginRotation(2, "newsha2", "newsha", nil))
	assert.Equal(t, int64(1), j.CumulativeMigrated())
}

func TestCPKJournalCorruptAndScopeMismatch(t *testing.T) {
	dir := t.TempDir()
	fp := cpkJournalFileName(dir, "acct-cont")
	require.NoError(t, os.WriteFile(fp, []byte("{not json"), 0600))
	_, err := openCPKJournal(dir, "acct-cont")
	assert.Error(t, err)

	// A state file whose embedded scope disagrees with the mount scope must be
	// rejected instead of driving rotation on the wrong account/container.
	fp3 := cpkJournalFileName(dir, "acct3-cont3")
	require.NoError(t, os.WriteFile(fp3, []byte(`{"version":1,"scope":"some-other-scope"}`), 0600))
	_, err = openCPKJournal(dir, "acct3-cont3")
	assert.Error(t, err)
}

func TestCPKJournalFileNameLocation(t *testing.T) {
	assert.Equal(t, filepath.Join("/var/lib/cpk", "cpk-rotation-a_b.json"), cpkJournalFileName("/var/lib/cpk", "a/b"))
	assert.Contains(t, cpkJournalFileName("", "x"), "cpk-rotation-x.json")
}

func TestCPKJournalBackoffRecordShape(t *testing.T) {
	dir := t.TempDir()
	j, err := openCPKJournal(dir, "acct-cont")
	require.NoError(t, err)
	require.NoError(t, j.beginRotation(1, "newsha", "oldsha", nil))
	next := time.Now().Add(time.Minute)
	require.NoError(t, j.Upsert(withError(cpkJournalRecord{
		Path: "p", OldSHA: "oldsha", NewSHA: "newsha", SourceETag: "e", Attempts: 2,
	}, cpkObjFailed, "", "boom")))
	rec, _ := j.Get("p")
	assert.Equal(t, cpkObjFailed, rec.State)
	assert.Equal(t, "boom", rec.LastError)
	assert.True(t, rec.NextAttemptAt.After(next) || rec.NextAttemptAt.Equal(next) || rec.NextAttemptAt.After(time.Now()))
}
