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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCPKMaterial(t *testing.T) cpkKeyMaterial {
	t.Helper()
	raw := make([]byte, 32)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	return cpkKeyMaterial{
		key: base64.StdEncoding.EncodeToString(raw),
		sha: base64.StdEncoding.EncodeToString(sum[:]),
	}
}

func TestValidateCPKMaterial(t *testing.T) {
	m := testCPKMaterial(t)
	assert.NoError(t, ValidateCPKMaterial(m.key, m.sha))

	// wrong sha
	other := testCPKMaterial(t)
	err := ValidateCPKMaterial(m.key, other.sha)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")

	// not base64
	assert.Error(t, ValidateCPKMaterial("not-base64!!", m.sha))

	// wrong key length
	short := base64.StdEncoding.EncodeToString(make([]byte, 16))
	assert.Error(t, ValidateCPKMaterial(short, m.sha))
}

func newTestRing(t *testing.T, enabled bool, keys []cpkKeyMaterial, ttl time.Duration) *CPKManager {
	t.Helper()
	m, err := newCPKManager(cpkManagerConfig{
		enabled:     enabled,
		scope:       "acct-cont",
		statePath:   t.TempDir(),
		identityTTL: ttl,
		keys:        keys,
	})
	require.NoError(t, err)
	return m
}

// fakeResolver is a scriptable remote encryption identifier resolver.
type fakeResolver struct {
	mu       sync.Mutex
	identies map[string]string
	etags    map[string]string
	err      error
}

func (f *fakeResolver) remoteIdentity(name string) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", "", f.err
	}
	return f.identies[name], f.etags[name], nil
}

func (f *fakeResolver) set(name, sha, etag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.identies[name] = sha
	f.etags[name] = etag
}

type failingCPKProber struct{ err error }

func (f *failingCPKProber) probeCPKKey(cpkKeyMaterial) error { return f.err }

func TestCPKSingleKeyModeIsStaticAndCompatible(t *testing.T) {
	k := testCPKMaterial(t)
	m := newTestRing(t, true, []cpkKeyMaterial{k}, time.Second)
	assert.True(t, m.Enabled())
	assert.True(t, m.SingleKeyMode())
	assert.Equal(t, cpkPhaseStable, m.Phase())

	// No resolver is ever contacted in single-key mode: every object gets the
	// sole configured key, just like the legacy static mount.
	rk, err := m.ReadKey("any/path")
	require.NoError(t, err)
	assert.Equal(t, k.sha, rk.material.sha)
	wk, err := m.WriteKey("any/path")
	require.NoError(t, err)
	assert.Equal(t, k.sha, wk.material.sha)
	nk, err := m.NewObjectKey()
	require.NoError(t, err)
	assert.Equal(t, k.sha, nk.material.sha)

	assert.NotNil(t, m.BlobCPKInfo(k.sha))
	assert.NotNil(t, m.FileCPKInfo(k.sha))
}

func TestCPKDisabledRing(t *testing.T) {
	m, err := newCPKManager(cpkManagerConfig{enabled: false})
	require.NoError(t, err)
	assert.False(t, m.Enabled())
	_, err = m.ReadKey("x")
	assert.ErrorIs(t, err, ErrCPKDisabled)
	assert.Nil(t, m.BlobCPKInfo("sha"))
}

func TestCPKRotationSelectsPerObjectKey(t *testing.T) {
	oldK := testCPKMaterial(t)
	newK := testCPKMaterial(t)

	// Mount with the old key, then rotate to the new key.
	m := newTestRing(t, true, []cpkKeyMaterial{oldK}, time.Second)
	resolver := &fakeResolver{identies: map[string]string{}, etags: map[string]string{}}
	m.SetResolver(resolver)

	rotated, err := m.ApplyConfiguredKeys([]cpkKeyMaterial{newK, oldK}, nil)
	require.NoError(t, err)
	assert.True(t, rotated)
	assert.Equal(t, cpkPhaseRotating, m.Phase())
	assert.Equal(t, newK.sha, m.ActiveSHA())

	// Brand new objects use the new key.
	nk, err := m.NewObjectKey()
	require.NoError(t, err)
	assert.Equal(t, newK.sha, nk.material.sha)

	// Existing object discovered on the old key via the remote identifier.
	resolver.set("old-file", oldK.sha, "e1")
	rk, err := m.ReadKey("old-file")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, rk.material.sha)

	// Remote says another object uses a key this mount does not hold.
	resolver.set("alien-file", "sha-not-configured", "e9")
	_, err = m.ReadKey("alien-file")
	assert.ErrorIs(t, err, ErrCPKKeyNotRegistered)

	// Writes to the existing old-key object stay on the old key (Azure requires
	// all blocks of a blob to share one key).
	wk, err := m.WriteKey("old-file")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, wk.material.sha)

	// A brand new path (no remote identity) writes with the active new key.
	wk2, err := m.WriteKey("brand-new-file")
	require.NoError(t, err)
	assert.Equal(t, newK.sha, wk2.material.sha)
}

func TestCPKOpenHandleKeepsOldKeyUntilReleased(t *testing.T) {
	oldK := testCPKMaterial(t)
	newK := testCPKMaterial(t)
	m := newTestRing(t, true, []cpkKeyMaterial{oldK}, time.Second)
	resolver := &fakeResolver{identies: map[string]string{"f": oldK.sha}, etags: map[string]string{"f": "e1"}}
	m.SetResolver(resolver)

	// Handle opened before rotation pins the old key.
	token := m.BindHandle("f", oldK.sha)
	_, err := m.ApplyConfiguredKeys([]cpkKeyMaterial{newK, oldK}, nil)
	require.NoError(t, err)

	wk, err := m.WriteKey("f")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, wk.material.sha, "open version must stay on the pinned old key")
	rk, err := m.ReadKey("f")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, rk.material.sha)

	// A second, post-rotation handle must not flip the object to the new key
	// while the older version is still open (oldest pin wins: no mixed object).
	token2 := m.BindHandle("f", newK.sha)
	wk, err = m.WriteKey("f")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, wk.material.sha)

	// Releasing only the old handle still leaves the new one pinned.
	m.ReleaseHandle(token)
	wk, err = m.WriteKey("f")
	require.NoError(t, err)
	assert.Equal(t, newK.sha, wk.material.sha)
	m.ReleaseHandle(token2)

	// No more open versions: the object identity (old key) decides until the
	// migrator republishes it.
	wk, err = m.WriteKey("f")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, wk.material.sha)
}

func TestCPKNewKeyRejectionPausesNewWritesOnly(t *testing.T) {
	oldK := testCPKMaterial(t)
	badK := testCPKMaterial(t)
	m := newTestRing(t, true, []cpkKeyMaterial{oldK}, time.Second)
	resolver := &fakeResolver{identies: map[string]string{"f": oldK.sha}, etags: map[string]string{"f": "e1"}}
	m.SetResolver(resolver)

	_, err := m.ApplyConfiguredKeys([]cpkKeyMaterial{badK, oldK}, &failingCPKProber{err: errors.New("403 key rejected")})
	assert.Error(t, err)
	assert.Equal(t, cpkPhaseNewKeyRejected, m.Phase())
	assert.Equal(t, oldK.sha, m.ActiveSHA(), "active key must not change on rejection")

	// New writes are paused.
	_, err = m.NewObjectKey()
	assert.ErrorIs(t, err, ErrCPKNewWritesPaused)
	_, err = m.WriteKey("never-seen")
	assert.ErrorIs(t, err, ErrCPKNewWritesPaused)

	// Old reads and old-key writes continue.
	rk, err := m.ReadKey("f")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, rk.material.sha)
	wk, err := m.WriteKey("f")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, wk.material.sha)

	// A corrected config is accepted and the rotation proceeds.
	goodK := testCPKMaterial(t)
	rotated, err := m.ApplyConfiguredKeys([]cpkKeyMaterial{goodK, oldK}, nil)
	require.NoError(t, err)
	assert.True(t, rotated)
	nk, err := m.NewObjectKey()
	require.NoError(t, err)
	assert.Equal(t, goodK.sha, nk.material.sha)
}

func TestCPKIdentityTTLRereadsRemoteIdentifier(t *testing.T) {
	oldK := testCPKMaterial(t)
	newK := testCPKMaterial(t)
	m := newTestRing(t, true, []cpkKeyMaterial{newK, oldK}, 40*time.Millisecond)
	resolver := &fakeResolver{identies: map[string]string{"f": oldK.sha}, etags: map[string]string{"f": "e1"}}
	m.SetResolver(resolver)
	assert.Equal(t, cpkPhaseRotating, m.Phase())

	rk, err := m.ReadKey("f")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, rk.material.sha)

	// Remote object migrated (by another mount) to the new key. Within the TTL
	// the cached identity may be reused; once stale it must be revalidated and
	// a cache hit can never keep driving an old key against a new-key object.
	resolver.set("f", newK.sha, "e2")
	time.Sleep(80 * time.Millisecond)
	rk, err = m.ReadKey("f")
	require.NoError(t, err)
	assert.Equal(t, newK.sha, rk.material.sha)
}

func TestCPKRetainedKeyUnavailability(t *testing.T) {
	oldK := testCPKMaterial(t)
	newK := testCPKMaterial(t)
	m := newTestRing(t, true, []cpkKeyMaterial{newK, oldK}, time.Second)
	resolver := &fakeResolver{identies: map[string]string{"f": oldK.sha}, etags: map[string]string{"f": "e1"}}
	m.SetResolver(resolver)

	m.MarkKeyAvailability(oldK.sha, false)
	_, err := m.ReadKey("f")
	assert.ErrorIs(t, err, ErrCPKKeyUnavailable)
	_, err = m.WriteKey("f")
	assert.ErrorIs(t, err, ErrCPKKeyUnavailable)

	// Reads of new-key objects are unaffected.
	resolver.set("g", newK.sha, "e2")
	rk, err := m.ReadKey("g")
	require.NoError(t, err)
	assert.Equal(t, newK.sha, rk.material.sha)

	// Key restored after config refresh.
	m.MarkKeyAvailability(oldK.sha, true)
	rk, err = m.ReadKey("f")
	require.NoError(t, err)
	assert.Equal(t, oldK.sha, rk.material.sha)
}

func TestCPKRevocationRequiresZeroObjectsAndHandles(t *testing.T) {
	oldK := testCPKMaterial(t)
	newK := testCPKMaterial(t)
	m := newTestRing(t, true, []cpkKeyMaterial{newK, oldK}, time.Second)

	// Before a remote sweep the old key cannot be declared revocable.
	st := m.Status()
	var oldStatus *CPKKeyStatus
	for i := range st.Keys {
		if st.Keys[i].SHA == oldK.sha {
			oldStatus = &st.Keys[i]
		}
	}
	require.NotNil(t, oldStatus)
	assert.False(t, oldStatus.Revocable)

	// Sweep still finds objects on the old key: not revocable.
	m.noteRemoteSweep(map[string]int{oldK.sha: 2})
	assert.False(t, m.revocable(oldK.sha))

	// Zero objects but an open handle: not revocable.
	m.noteRemoteSweep(map[string]int{oldK.sha: 0})
	token := m.BindHandle("f", oldK.sha)
	assert.False(t, m.revocable(oldK.sha))
	m.ReleaseHandle(token)

	// Zero objects and zero handles: revocable.
	assert.True(t, m.revocable(oldK.sha))
	assert.Contains(t, m.RevocableKeys(), oldK.sha)

	// The active key is never revocable.
	assert.NotContains(t, m.RevocableKeys(), newK.sha)
}

func TestBuildCPKMaterialsAcceptsSingleAndRingConfig(t *testing.T) {
	k := testCPKMaterial(t)

	// Legacy single-key pair.
	mats, err := buildCPKMaterials(AzStorageOptions{CPKEncryptionKey: k.key, CPKEncryptionKeySha256: k.sha})
	require.NoError(t, err)
	require.Len(t, mats, 1)
	assert.Equal(t, k.sha, mats[0].sha)

	// Multi-key ring, first entry is active.
	k2 := testCPKMaterial(t)
	mats, err = buildCPKMaterials(AzStorageOptions{
		CPKEnabled: true,
		CPKEncryptionKeys: []CPKKeyOptions{
			{Key: k.key, KeySha256: k.sha},
			{Key: k2.key, KeySha256: k2.sha},
		},
	})
	require.NoError(t, err)
	require.Len(t, mats, 2)
	assert.Equal(t, k.sha, mats[0].sha)

	// Missing key.
	_, err = buildCPKMaterials(AzStorageOptions{CPKEnabled: true})
	assert.Error(t, err)

	// Bad sha in a ring entry.
	_, err = buildCPKMaterials(AzStorageOptions{
		CPKEncryptionKeys: []CPKKeyOptions{
			{Key: k.key, KeySha256: k.sha},
			{Key: k2.key, KeySha256: "wrong"},
		},
	})
	assert.Error(t, err)
}

// TestCPKSelectionErrnoMapping documents the user-visible failure codes.
func TestCPKSelectionErrnoMapping(t *testing.T) {
	assert.ErrorIs(t, mapCPKSelectionErr(ErrCPKNewWritesPaused), syscall.EAGAIN)
	assert.ErrorIs(t, mapCPKSelectionErr(ErrCPKKeyUnavailable), syscall.EAGAIN)
	assert.ErrorIs(t, mapCPKSelectionErr(ErrCPKKeyNotRegistered), syscall.EACCES)
	assert.NoError(t, mapCPKSelectionErr(nil))
}
