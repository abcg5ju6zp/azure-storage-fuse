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
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRemoteObject struct {
	sha   string
	etag  string
	isDir bool
}

// fakeCPKBackend is an in-memory account that enforces the same rules the
// migrator relies on: migrate calls are conditional on the source etag and can
// be scripted to succeed ambiguously or reject definitively.
type fakeCPKBackend struct {
	mu          sync.Mutex
	objects     map[string]*fakeRemoteObject
	etagSeq     int
	migrateHits map[string]int
	// hook may mutate state (simulating a write that landed) and return an
	// error. Returning a non-ResponseError while mutating simulates an
	// ambiguous response that actually committed.
	hook func(name string, o *fakeRemoteObject, newSHA string) error
}

func newFakeCPKBackend(objs map[string]*fakeRemoteObject) *fakeCPKBackend {
	return &fakeCPKBackend{objects: objs, etagSeq: len(objs), migrateHits: map[string]int{}}
}

func (b *fakeCPKBackend) remoteIdentity(name string) (string, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, ok := b.objects[name]
	if !ok {
		return "", "", syscall.ENOENT
	}
	return o.sha, o.etag, nil
}

func (b *fakeCPKBackend) enumerateCPKObjects() ([]cpkRemoteObject, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]cpkRemoteObject, 0, len(b.objects))
	for n, o := range b.objects {
		out = append(out, cpkRemoteObject{name: n, sha: o.sha, etag: o.etag, isDir: o.isDir})
	}
	return out, nil
}

func (b *fakeCPKBackend) migrateObject(obj cpkRemoteObject, newSHA, srcETag string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, ok := b.objects[obj.name]
	if !ok {
		return "", &azcore.ResponseError{ErrorCode: "BlobNotFound", StatusCode: 404}
	}
	if o.etag != srcETag {
		return "", &azcore.ResponseError{ErrorCode: "ConditionNotMet", StatusCode: 412}
	}
	b.migrateHits[obj.name]++
	if b.hook != nil {
		if err := b.hook(obj.name, o, newSHA); err != nil {
			return "", err
		}
	}
	o.sha = newSHA
	b.etagSeq++
	o.etag = fmt.Sprintf("e%d", b.etagSeq)
	return o.etag, nil
}

type cpkRotationFixture struct {
	mgr    *CPKManager
	mr     *cpkMigrator
	backend *fakeCPKBackend
	oldSHA string
	newSHA string
	dir    string
}

func newRotationFixture(t *testing.T, objs map[string]*fakeRemoteObject) cpkRotationFixture {
	t.Helper()
	oldK := testCPKMaterial(t)
	newK := testCPKMaterial(t)
	dir := t.TempDir()
	m, err := newCPKManager(cpkManagerConfig{
		enabled: true, scope: "acct-cont", statePath: dir,
		identityTTL: time.Second, keys: []cpkKeyMaterial{newK, oldK},
	})
	require.NoError(t, err)
	b := newFakeCPKBackend(objs)
	m.SetResolver(b)
	mr := newCPKMigrator(m, b, time.Hour, 0)
	return cpkRotationFixture{mgr: m, mr: mr, backend: b, oldSHA: oldK.sha, newSHA: newK.sha, dir: dir}
}

func seedOldObjects(names ...string) map[string]*fakeRemoteObject {
	objs := make(map[string]*fakeRemoteObject, len(names))
	for i, n := range names {
		objs[n] = &fakeRemoteObject{sha: "old-sha-placeholder", etag: fmt.Sprintf("e%d", i+1)}
	}
	return objs
}

func (f *cpkRotationFixture) seedOldKeys(objs map[string]*fakeRemoteObject) {
	for _, o := range objs {
		if o.sha == "old-sha-placeholder" {
			o.sha = f.oldSHA
		}
	}
}

func TestCPKMigrationHappyPath(t *testing.T) {
	objs := seedOldObjects("a", "b")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(objs)

	require.NoError(t, fx.mr.runOnce(context.Background()))
	// First sweep: objects migrated but completion needs a confirming sweep.
	require.NoError(t, fx.mr.runOnce(context.Background()))

	b := fx.backend
	assert.Equal(t, fx.newSHA, b.objects["a"].sha)
	assert.Equal(t, fx.newSHA, b.objects["b"].sha)
	assert.NotEmpty(t, b.objects["a"].etag)
	assert.Equal(t, cpkPhaseRotationComplete, fx.mgr.Phase())
	st := fx.mgr.Status()
	assert.Equal(t, int64(2), st.CumulativeMigrated)
	assert.Contains(t, fx.mgr.RevocableKeys(), fx.oldSHA)
}

func TestCPKMigrationAmbiguousResponseDidNotLand(t *testing.T) {
	objs := seedOldObjects("a", "b")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(objs)

	var calls int
	var mu sync.Mutex
	fx.backend.hook = func(name string, _ *fakeRemoteObject, _ string) error {
		if name != "a" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			// Transport timeout, server state unknown and unchanged.
			return errors.New("read tcp: i/o timeout")
		}
		return nil
	}

	require.NoError(t, fx.mr.runOnce(context.Background()))
	rec, ok := fx.mgr.Journal().Get("a")
	require.True(t, ok)
	assert.Equal(t, cpkObjUnconfirmed, rec.State)

	require.NoError(t, fx.mr.runOnce(context.Background()))
	require.NoError(t, fx.mr.runOnce(context.Background()))

	assert.Equal(t, fx.newSHA, fx.backend.objects["a"].sha)
	assert.Equal(t, fx.newSHA, fx.backend.objects["b"].sha)
	assert.Equal(t, cpkPhaseRotationComplete, fx.mgr.Phase())
	assert.Equal(t, int64(2), fx.mgr.Status().CumulativeMigrated)
}

func TestCPKMigrationAmbiguousResponseActuallyLanded(t *testing.T) {
	objs := seedOldObjects("a")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(objs)

	fx.backend.hook = func(name string, o *fakeRemoteObject, newSHA string) error {
		// Simulate the commit landing despite an ambiguous client error.
		o.sha = newSHA
		fx.backend.etagSeq++
		o.etag = fmt.Sprintf("e%d", fx.backend.etagSeq)
		return errors.New("connection reset by peer")
	}

	require.NoError(t, fx.mr.runOnce(context.Background()))
	assert.Equal(t, 1, fx.backend.migrateHits["a"], "ambiguous publish attempted once")
	assert.Equal(t, cpkObjUnconfirmed, fx.mgr.Journal().activeRecords()[0].State)

	require.NoError(t, fx.mr.runOnce(context.Background()))
	// Recovery confirms via remote identifier+version, no republish happens.
	assert.Equal(t, 1, fx.backend.migrateHits["a"])
	_, tracked := fx.mgr.Journal().Get("a")
	assert.False(t, tracked)
	assert.Equal(t, int64(1), fx.mgr.Journal().CumulativeMigrated())
}

func TestCPKMigrationSourceVersionChanged(t *testing.T) {
	objs := seedOldObjects("a")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(objs)

	// Journal holds a stale source version from an earlier observation.
	require.NoError(t, fx.mgr.Journal().beginRotation(1, fx.newSHA, fx.oldSHA, nil))
	require.NoError(t, fx.mgr.Journal().Upsert(cpkJournalRecord{
		Path: "a", OldSHA: fx.oldSHA, NewSHA: fx.newSHA,
		SourceETag: "stale-etag", State: cpkObjPending,
	}))

	require.NoError(t, fx.mr.runOnce(context.Background()))
	require.NoError(t, fx.mr.runOnce(context.Background()))

	assert.Equal(t, fx.newSHA, fx.backend.objects["a"].sha)
	assert.Equal(t, cpkPhaseRotationComplete, fx.mgr.Phase())
}

func TestCPKMigrationDefinitiveFailureKeepsProgress(t *testing.T) {
	objs := seedOldObjects("a", "b")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(objs)

	fx.backend.hook = func(name string, _ *fakeRemoteObject, _ string) error {
		if name == "a" {
			return &azcore.ResponseError{ErrorCode: "AuthenticationFailed", StatusCode: 403}
		}
		return nil
	}

	require.NoError(t, fx.mr.runOnce(context.Background()))
	rec, ok := fx.mgr.Journal().Get("a")
	require.True(t, ok)
	assert.Equal(t, cpkObjFailed, rec.State)
	assert.Equal(t, 1, rec.Attempts)
	assert.True(t, rec.NextAttemptAt.After(time.Now()))
	assert.Equal(t, fx.oldSHA, fx.backend.objects["a"].sha, "failed object never changes key")
	assert.Equal(t, fx.newSHA, fx.backend.objects["b"].sha, "other objects still migrate")

	st := fx.mgr.Status()
	assert.Equal(t, 1, st.Failed)
	assert.NotEqual(t, cpkPhaseRotationComplete, fx.mgr.Phase(), "partial failure keeps rotation open")
}

func TestCPKMigrationWaitsForOpenHandle(t *testing.T) {
	objs := seedOldObjects("a", "b")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(objs)

	token := fx.mgr.BindHandle("a", fx.oldSHA)
	require.NoError(t, fx.mr.runOnce(context.Background()))

	_, tracked := fx.mgr.Journal().Get("a")
	assert.True(t, tracked, "object stays queued while a version is open")
	assert.Equal(t, fx.oldSHA, fx.backend.objects["a"].sha)
	assert.Equal(t, fx.newSHA, fx.backend.objects["b"].sha)
	st := fx.mgr.Status()
	assert.GreaterOrEqual(t, st.ObjectsMigratingNow, 1)

	fx.mgr.ReleaseHandle(token)
	require.NoError(t, fx.mr.runOnce(context.Background()))
	require.NoError(t, fx.mr.runOnce(context.Background()))
	assert.Equal(t, fx.newSHA, fx.backend.objects["a"].sha)
	assert.Equal(t, cpkPhaseRotationComplete, fx.mgr.Phase())
}

func TestCPKMigrationUnknownKeyObjectIsUnmanaged(t *testing.T) {
	objs := seedOldObjects("a", "alien")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(map[string]*fakeRemoteObject{"a": objs["a"]})
	objs["alien"].sha = "sha-not-configured"

	require.NoError(t, fx.mr.runOnce(context.Background()))
	require.NoError(t, fx.mr.runOnce(context.Background()))

	st := fx.mgr.Status()
	assert.GreaterOrEqual(t, st.UnmanagedObjects, 1)
	_, tracked := fx.mgr.Journal().Get("alien")
	assert.False(t, tracked)
	// The known old key is still fully drained and reportable as revocable.
	assert.Contains(t, fx.mgr.RevocableKeys(), fx.oldSHA)
}

func TestCPKMigrationSkipsHNSDirectories(t *testing.T) {
	objs := seedOldObjects("a", "dir")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(objs)
	objs["dir"].isDir = true

	require.NoError(t, fx.mr.runOnce(context.Background()))
	require.NoError(t, fx.mr.runOnce(context.Background()))

	assert.Equal(t, fx.newSHA, fx.backend.objects["a"].sha)
	assert.Equal(t, fx.oldSHA, fx.backend.objects["dir"].sha, "HNS directory is never rewritten")
	assert.GreaterOrEqual(t, fx.mgr.Status().SkippedDirectories, 1)
	assert.NotContains(t, fx.mgr.RevocableKeys(), fx.oldSHA, "directory still pins the old key")
}

func TestCPKMigrationRecoversAcrossRestart(t *testing.T) {
	objs := seedOldObjects("a", "b")
	fx := newRotationFixture(t, objs)
	fx.seedOldKeys(objs)

	// First lifetime: b migrates, a gets an ambiguous non-landed failure.
	var mu sync.Mutex
	var aTried bool
	fx.backend.hook = func(name string, _ *fakeRemoteObject, _ string) error {
		if name == "a" {
			mu.Lock()
			defer mu.Unlock()
			if !aTried {
				aTried = true
				return errors.New("i/o timeout")
			}
		}
		return nil
	}
	require.NoError(t, fx.mr.runOnce(context.Background()))
	assert.Equal(t, int64(1), fx.mgr.Journal().CumulativeMigrated())

	// Simulate a process restart with a fresh in-memory "account view" rebuilt
	// from the same persisted journal: b is new-key, a is still old-key.
	oldK := cpkKeyMaterial{sha: fx.oldSHA, key: fxKeyFromSHA(t, fx.oldSHA, fx)}
	newK := cpkKeyMaterial{sha: fx.newSHA, key: fxKeyFromSHA(t, fx.newSHA, fx)}

	m2, err := newCPKManager(cpkManagerConfig{
		enabled: true, scope: "acct-cont", statePath: fx.dir,
		identityTTL: time.Second, keys: []cpkKeyMaterial{newK, oldK},
	})
	require.NoError(t, err)
	assert.Equal(t, cpkPhaseRotating, m2.Phase(), "rotation resumes from the state file")

	b2 := newFakeCPKBackend(map[string]*fakeRemoteObject{
		"a": {sha: fx.oldSHA, etag: "e1"},
		"b": {sha: fx.newSHA, etag: "e-migrated"},
	})
	m2.SetResolver(b2)
	mr2 := newCPKMigrator(m2, b2, time.Hour, 0)
	require.NoError(t, mr2.runOnce(context.Background()))
	require.NoError(t, mr2.runOnce(context.Background()))

	assert.Equal(t, fx.newSHA, b2.objects["a"].sha)
	assert.Equal(t, int64(2), m2.Journal().CumulativeMigrated())
	assert.Equal(t, cpkPhaseRotationComplete, m2.Phase())
}

// fxKeyFromSHA fethes the raw key for a sha out of the fixture ring so a
// restarted manager can be rebuilt with the same material.
func fxKeyFromSHA(t *testing.T, sha string, fx cpkRotationFixture) string {
	t.Helper()
	k, err := fx.mgr.KeyForSHA(sha)
	require.NoError(t, err)
	return k.material.key
}
