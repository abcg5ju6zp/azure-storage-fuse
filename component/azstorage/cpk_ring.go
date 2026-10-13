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
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
)

// Sentinel errors returned by the CPK key ring. Storage methods translate these
// to errno style errors so that callers can distinguish a paused write path from
// a key that is temporarily unavailable.
var (
	// ErrCPKDisabled is returned when the mount is not configured with CPK.
	ErrCPKDisabled = errors.New("customer-provided key is not enabled for this mount")
	// ErrCPKNewWritesPaused is returned while the new rotation key has not been
	// validated: existing objects remain readable with old keys but no object may
	// be published under the rejected new key.
	ErrCPKNewWritesPaused = errors.New("new encryption key has not been validated, new writes are paused")
	// ErrCPKKeyUnavailable is returned when an object needs a retained key whose
	// material is temporarily unavailable. Reads/writes of objects encrypted with
	// other keys are unaffected.
	ErrCPKKeyUnavailable = errors.New("the customer-provided key required by this object is temporarily unavailable")
	// ErrCPKKeyNotRegistered is returned when a remote object is encrypted with a
	// key that is neither active nor retained by this mount, so presenting a key
	// would be wrong.
	ErrCPKKeyNotRegistered = errors.New("object is encrypted with a customer-provided key that is not configured on this mount")
	// ErrCPKIdentityUnknown is returned when the remote encryption identifier of
	// an object cannot be resolved and no handle pins a key for it.
	ErrCPKIdentityUnknown = errors.New("remote encryption key identifier for the object could not be resolved")
)

// cpkKeyMaterial is one configured AES-256 customer-provided key. key and sha
// are the base64 encoded values accepted verbatim by the REST headers.
type cpkKeyMaterial struct {
	key string
	sha string
}

// ValidateCPKMaterial verifies that the configured key is 256 bits of base64 and
// the supplied SHA-256 really is SHA-256 of the decoded key. It performs no
// network access.
func ValidateCPKMaterial(keyB64, shaB64 string) error {
	raw, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil {
		return fmt.Errorf("cpk-encryption-key must be base64 encoded [%w]", err)
	}
	if len(raw) != 32 {
		return fmt.Errorf("cpk-encryption-key must decode to 32 bytes (AES-256), got %d bytes", len(raw))
	}
	wantSHA := cpkSHAFromKeyB64(keyB64)
	if wantSHA == "" {
		return errors.New("failed to compute sha256 of cpk-encryption-key")
	}
	if shaB64 != wantSHA {
		return errors.New("cpk-encryption-key-sha256 does not match SHA-256 of cpk-encryption-key")
	}
	return nil
}

// cpkRotationPhase describes where a key rotation stands.
type cpkRotationPhase string

const (
	// cpkPhaseStable: a single key is in use (or nothing to migrate).
	cpkPhaseStable cpkRotationPhase = "Stable"
	// cpkPhaseNewKeyRejected: a new key failed validation; new writes stay paused,
	// reads continue with retained keys.
	cpkPhaseNewKeyRejected cpkRotationPhase = "NewKeyRejected"
	// cpkPhaseRotating: a new key is active for new objects while retained old
	// keys still protect existing objects; migration is pending/running.
	cpkPhaseRotating cpkRotationPhase = "Rotating"
	// cpkPhaseRotationComplete: the last remote sweep found no object encrypted
	// with an old key; only open handles may still pin old keys before revocation.
	cpkPhaseRotationComplete cpkRotationPhase = "Complete"
)

// cpkKeyVersion is a key retained by the ring together with its lifecycle state.
type cpkKeyVersion struct {
	material  cpkKeyMaterial
	active    bool
	available bool
	retiredAt time.Time
}

// objectIdentity is the last known remote key identity of one object.
type objectIdentity struct {
	sha  string
	etag string
	at   time.Time
	// remote is true when sha/etag were observed on a service response (list,
	// properties or a confirmed publish); false when it is only a hint carried
	// by an upstream cached attribute.
	remote bool
}

// handleBinding pins one opened/created file version to the key it was opened
// with until that version completes (flush/close). Bindings are tracked with a
// ring allocated token stored on the handlemap.Handle, because the kernel handle
// id is assigned later by an upstream component.
type handleBinding struct {
	path string
	sha  string
	seq  uint64
}

// cpkIdentityResolver returns the authoritative (key sha, etag) of a remote
// object without presenting any key, using the List Blobs encryption identifier.
// Empty sha means the object is not encrypted with a customer-provided key.
type cpkIdentityResolver interface {
	remoteIdentity(name string) (sha string, etag string, err error)
}

// cpkKeyProber validates that a candidate new key is accepted by the service.
// The candidate material is passed explicitly because it is not admitted into
// the ring until validation succeeds.
type cpkKeyProber interface {
	probeCPKKey(material cpkKeyMaterial) error
}

// cpkBlobInfo builds an azblob CPK option from raw key material.
func cpkBlobInfo(keyB64, shaB64 string) *blob.CPKInfo {
	return &blob.CPKInfo{
		EncryptionKey:       &keyB64,
		EncryptionKeySHA256: &shaB64,
		EncryptionAlgorithm: to.Ptr(blob.EncryptionAlgorithmTypeAES256),
	}
}

// cpkFileInfo builds an azdatalake CPK option from raw key material.
func cpkFileInfo(keyB64, shaB64 string) *file.CPKInfo {
	return &file.CPKInfo{
		EncryptionKey:       &keyB64,
		EncryptionKeySHA256: &shaB64,
		EncryptionAlgorithm: to.Ptr(file.EncryptionAlgorithmTypeAES256),
	}
}

// CPKManager is the key ring for a mount. It keeps the active key used for new
// objects together with every retained key that remote objects or open versions
// still reference, and selects the correct key for each individual object based
// on the encryption identifier returned by the service.
type CPKManager struct {
	mu sync.RWMutex

	enabled     bool
	statePath   string
	identityTTL time.Duration
	scope       string

	keys      map[string]*cpkKeyVersion
	activeSHA string

	// objects indexes the last observed key identity per mount-relative path.
	objects map[string]*objectIdentity

	// bindings pins open versions: token -> binding, and a per-path sequence of
	// tokens so the oldest (pre-rotation) pin decides the write key.
	bindings     map[uint64]*handleBinding
	pathBindings map[string][]uint64
	bindSeq      uint64

	phase        cpkRotationPhase
	rejectReason string
	rotationID   uint64
	startedAt    time.Time
	updatedAt    time.Time

	// remoteRefs is the result of the most recent full enumeration: number of
	// remote objects still encrypted with each retained key sha.
	remoteRefs       map[string]int
	remoteRefsAt     time.Time
	remoteRefsScoped bool

	resolver cpkIdentityResolver
	journal  *cpkJournal

	// lastSweepSummary records progress counters for status queries.
	lastSweepAt          time.Time
	lastSweepTotal       int
	lastSweepMigrating   int
	lastSweepUnmanaged   int
	lastSweepSkippedDirs int
	lastSweepOpenWaiting int
	cumulativeMigrated   int64
}

// cpkManagerConfig carries the construction parameters for the ring.
type cpkManagerConfig struct {
	enabled     bool
	scope       string
	statePath   string
	identityTTL time.Duration
	keys        []cpkKeyMaterial
}

// newCPKManager builds the ring. The first entry of keys is the active key; a
// single entry selects compatibility mode where the mount behaves exactly like
// the legacy static CPK mount (no identity resolution, no journal).
func newCPKManager(cfg cpkManagerConfig) (*CPKManager, error) {
	m := &CPKManager{
		enabled:      cfg.enabled,
		statePath:    cfg.statePath,
		identityTTL:  cfg.identityTTL,
		scope:        cfg.scope,
		keys:         make(map[string]*cpkKeyVersion),
		objects:      make(map[string]*objectIdentity),
		bindings:     make(map[uint64]*handleBinding),
		pathBindings: make(map[string][]uint64),
		remoteRefs:   make(map[string]int),
		phase:        cpkPhaseStable,
		startedAt:    time.Now(),
		updatedAt:    time.Now(),
	}

	now := time.Now()
	for i, km := range cfg.keys {
		if err := ValidateCPKMaterial(km.key, km.sha); err != nil {
			return nil, err
		}
		if _, dup := m.keys[km.sha]; dup {
			return nil, fmt.Errorf("duplicate cpk-encryption-keys entry for key sha256 %s", km.sha)
		}
		version := &cpkKeyVersion{
			material:  km,
			active:    i == 0,
			available: true,
		}
		// Entries after the active one are retained keys from an ongoing
		// rotation: mark them retired so revocation reporting works.
		if i > 0 {
			version.retiredAt = now
		}
		m.keys[km.sha] = version
		if i == 0 {
			m.activeSHA = km.sha
		}
	}

	if m.enabled {
		j, err := openCPKJournal(m.statePath, m.scope)
		if err != nil {
			return nil, err
		}
		m.journal = j

		// Recover an interrupted rotation: the persisted active key matches the
		// configured one and unfinished objects remain, so resume migration.
		snap := j.Snapshot()
		if snap.RotationID > 0 && snap.ActiveSHA == m.activeSHA && len(snap.Objects) > 0 {
			m.phase = cpkPhaseRotating
			m.rotationID = snap.RotationID
			if !snap.StartedAt.IsZero() {
				m.startedAt = snap.StartedAt
			}
			log.Crit("newCPKManager : resuming CPK rotation %d from state file with %d unfinished object records",
				snap.RotationID, len(snap.Objects))
		}
	}

	// Configuring several keys means a retained key exists and a rotation is in
	// progress even before the first object is discovered.
	if m.phase == cpkPhaseStable && len(m.keys) > 1 {
		m.phase = cpkPhaseRotating
		m.rotationID = 1
		if m.journal != nil {
			_ = m.journal.beginRotation(1, m.activeSHA, "", nil)
		}
	}

	return m, nil
}

// revocable reports whether a retained key currently has zero remote objects,
// zero journal records and zero open versions referencing it.
func (m *CPKManager) revocable(sha string) bool {
	for _, k := range m.Status().Keys {
		if k.SHA == sha {
			return k.Revocable
		}
	}
	return false
}

// StartupReconcile handles a state file whose active key is older than the one
// supplied by configuration at mount time: the configured key either continues
// an in-flight rotation or, with a service probe, starts a new one.
func (m *CPKManager) StartupReconcile(materials []cpkKeyMaterial, prober cpkKeyProber) (bool, error) {
	if m == nil || !m.enabled || m.journal == nil {
		return false, nil
	}
	jActive := m.journal.ActiveSHA()
	jID := m.journal.RotationID()
	if jID > 0 && jActive != "" && jActive != m.ActiveSHA() {
		return m.ApplyConfiguredKeys(materials, prober)
	}
	return false, nil
}

// Enabled reports whether CPK is configured for the mount.
func (m *CPKManager) Enabled() bool {
	return m != nil && m.enabled
}

// SingleKeyMode reports whether the ring currently holds exactly one key. It
// is safe to call without holding the manager lock.
func (m *CPKManager) SingleKeyMode() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.keys) <= 1
}

// SetResolver attaches the remote identity resolver once the storage pipeline
// has been built.
func (m *CPKManager) SetResolver(r cpkIdentityResolver) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resolver = r
}

// ActiveSHA returns the sha256 identifier of the active key.
func (m *CPKManager) ActiveSHA() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeSHA
}

// Phase returns the current rotation phase.
func (m *CPKManager) Phase() cpkRotationPhase {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.phase
}

// Journal exposes the persistent progress journal (nil when CPK is disabled).
func (m *CPKManager) Journal() *cpkJournal {
	if m == nil {
		return nil
	}
	return m.journal
}

// BlobCPKInfo builds the azblob CPK option for a registered key sha. Returns nil
// when sha is empty (object not encrypted with a customer key).
func (m *CPKManager) BlobCPKInfo(sha string) *blob.CPKInfo {
	if m == nil || sha == "" {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	k, ok := m.keys[sha]
	if !ok {
		return nil
	}
	return cpkBlobInfo(k.material.key, k.material.sha)
}

// FileCPKInfo builds the azdatalake CPK option for a registered key sha.
func (m *CPKManager) FileCPKInfo(sha string) *file.CPKInfo {
	if m == nil || sha == "" {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	k, ok := m.keys[sha]
	if !ok {
		return nil
	}
	return cpkFileInfo(k.material.key, k.material.sha)
}

// keyAvailable returns the version for a sha when its material is present.
func (m *CPKManager) keyAvailable(sha string) (*cpkKeyVersion, bool) {
	k, ok := m.keys[sha]
	return k, ok && k.available
}

// RecordRemoteIdentity stores a service-observed encryption identity for an
// object. Every observation is authoritative for the reported object version.
func (m *CPKManager) RecordRemoteIdentity(path, sha, etag string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[path] = &objectIdentity{sha: sha, etag: etag, at: time.Now(), remote: true}
}

// RecordPublish records the identity of a version this mount just published.
// The migrator additionally confirms the identity from the service before
// treating a migration as done.
func (m *CPKManager) RecordPublish(path, sha, etag string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[path] = &objectIdentity{sha: sha, etag: etag, at: time.Now(), remote: true}
}

// NotifyDeleted drops all knowledge of an object after a successful delete and
// removes it from the migration journal.
func (m *CPKManager) NotifyDeleted(path string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	delete(m.objects, path)
	j := m.journal
	m.mu.Unlock()
	if j != nil {
		_ = j.Delete(path)
	}
}

// NotifyRenamed moves an identity record from source to destination after a
// rename; the caller must re-confirm the destination identity from the service
// because copy/rename responses are not proof of the destination key.
func (m *CPKManager) NotifyRenamed(src, dst string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if id, ok := m.objects[src]; ok {
		id := *id
		id.remote = false // force reconfirmation against the destination
		m.objects[dst] = &id
		delete(m.objects, src)
	}
	// Rewrite still-open bindings so later write-backs target the new path while
	// keeping the key version they were opened with.
	for _, t := range m.pathBindings[src] {
		if b, ok := m.bindings[t]; ok {
			b.path = dst
		}
	}
	if pins := m.pathBindings[src]; len(pins) > 0 {
		m.pathBindings[dst] = append(m.pathBindings[dst], pins...)
		delete(m.pathBindings, src)
	}
	j := m.journal
	m.mu.Unlock()
	if j != nil {
		_ = j.RenamePath(src, dst)
	}
}

// BindHandle pins the key version an open/created file version must keep using
// until it completes. sha may be empty for objects that are not CPK encrypted,
// in which case the active key is pinned. The returned token must be passed to
// ReleaseHandle.
func (m *CPKManager) BindHandle(path, sha string) uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if sha == "" {
		sha = m.activeSHA
	}
	m.bindSeq++
	token := m.bindSeq
	m.bindings[token] = &handleBinding{path: path, sha: sha, seq: token}
	m.pathBindings[path] = append(m.pathBindings[path], token)
	return token
}

// ReleaseHandle removes an open-version pin.
func (m *CPKManager) ReleaseHandle(token uint64) {
	if m == nil || token == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.bindings[token]
	if !ok {
		return
	}
	pins := m.pathBindings[b.path]
	for i, t := range pins {
		if t == token {
			m.pathBindings[b.path] = append(pins[:i], pins[i+1:]...)
			break
		}
	}
	if len(m.pathBindings[b.path]) == 0 {
		delete(m.pathBindings, b.path)
	}
	delete(m.bindings, token)
}

// boundSHALocked returns the key sha that in-flight versions of path must keep
// using. When several generations of handles are open concurrently the oldest
// pin (opened before the rotation) decides so a file never mixes two keys.
// Caller must hold m.mu (read or write).
func (m *CPKManager) boundSHALocked(path string) (string, bool) {
	pins := m.pathBindings[path]
	if len(pins) == 0 {
		return "", false
	}
	var oldest uint64
	first := true
	for _, t := range pins {
		if first || t < oldest {
			oldest = t
		}
		first = false
	}
	if b, ok := m.bindings[oldest]; ok {
		return b.sha, true
	}
	return "", false
}

// resolveIdentityLocked returns the remote encryption sha/etag for an object,
// consulting and refreshing the in-memory index. Caller holds m.mu (read is
// upgraded at the resolver call site by resolveIdentity).
func (m *CPKManager) cachedIdentityFresh(path string) (*objectIdentity, bool) {
	id, ok := m.objects[path]
	if !ok || !id.remote {
		return nil, false
	}
	// While no rotation is in flight a remote identity cannot change key without
	// this mount observing it (all writes go through the ring), so the index is
	// authoritative. During rotation re-validate stale entries against the list.
	if m.phase == cpkPhaseStable || m.phase == cpkPhaseRotationComplete || m.identityTTL <= 0 {
		return id, true
	}
	if time.Since(id.at) <= m.identityTTL {
		return id, true
	}
	return id, false
}

func (m *CPKManager) resolveIdentity(path string) (string, string, error) {
	m.mu.RLock()
	id, fresh := m.cachedIdentityFresh(path)
	resolver := m.resolver
	m.mu.RUnlock()
	if fresh {
		return id.sha, id.etag, nil
	}
	if resolver == nil {
		if id != nil {
			return id.sha, id.etag, nil
		}
		return "", "", ErrCPKIdentityUnknown
	}
	sha, etag, err := resolver.remoteIdentity(path)
	if err != nil {
		if id != nil {
			// List hiccup: fall back to the last observed identity rather than
			// failing a read for an unreadable directory listing.
			log.Warn("CPKManager::resolveIdentity : remote identity lookup failed for %s, using last observed key [%s]", path, err.Error())
			return id.sha, id.etag, nil
		}
		return "", "", err
	}
	m.RecordRemoteIdentity(path, sha, etag)
	return sha, etag, nil
}

// ReadKey selects the key for reading/getting an existing object. Open versions
// are honoured first; otherwise the remote encryption identifier decides.
func (m *CPKManager) ReadKey(path string) (*cpkKeyVersion, error) {
	if m == nil || !m.enabled {
		return nil, ErrCPKDisabled
	}

	m.mu.RLock()
	if len(m.keys) <= 1 {
		k, _ := m.keyAvailable(m.activeSHA)
		m.mu.RUnlock()
		return k, nil
	}
	if sha, ok := m.boundSHALocked(path); ok {
		k, avail := m.keyAvailable(sha)
		m.mu.RUnlock()
		if !avail {
			return nil, fmt.Errorf("%w for %s", ErrCPKKeyUnavailable, path)
		}
		return k, nil
	}
	m.mu.RUnlock()

	sha, _, err := m.resolveIdentity(path)
	if err != nil {
		return nil, err
	}
	if sha == "" {
		// Object is not CPK encrypted: presenting the active key preserves the
		// legacy behaviour of the mount for mixed/account-key objects.
		sha = m.activeSHA
	}
	m.mu.RLock()
	k, avail := m.keyAvailable(sha)
	_, known := m.keys[sha]
	m.mu.RUnlock()
	if !avail {
		if known {
			return nil, fmt.Errorf("%w for %s", ErrCPKKeyUnavailable, path)
		}
		return nil, fmt.Errorf("%w for %s", ErrCPKKeyNotRegistered, path)
	}
	return k, nil
}

// WriteKey selects the key for a write to an existing object. The object keeps
// publishing under its current key version (Azure requires every block of a
// block blob to share one CPK); only brand-new objects and migration publish in
// the active key. Callers creating a new object must use NewObjectKey.
func (m *CPKManager) WriteKey(path string) (*cpkKeyVersion, error) {
	if m == nil || !m.enabled {
		return nil, ErrCPKDisabled
	}

	m.mu.RLock()
	if len(m.keys) <= 1 {
		k, _ := m.keyAvailable(m.activeSHA)
		m.mu.RUnlock()
		return k, nil
	}
	if sha, ok := m.boundSHALocked(path); ok {
		k, avail := m.keyAvailable(sha)
		m.mu.RUnlock()
		if !avail {
			return nil, fmt.Errorf("%w for %s", ErrCPKKeyUnavailable, path)
		}
		return k, nil
	}
	id, has := m.objects[path]
	m.mu.RUnlock()

	sha := ""
	if has && id.remote {
		sha = id.sha
	} else {
		resolved, _, err := m.resolveIdentity(path)
		if err == nil {
			sha = resolved
		} else if !errors.Is(err, ErrCPKIdentityUnknown) {
			return nil, err
		}
	}

	// No existing identity => a write creates a new remote version: this goes
	// through NewObjectKey, which is paused while a candidate key is rejected.
	// Writes to an object observed on an existing key always continue under
	// that key, even while new writes are paused.
	if sha == "" {
		return m.NewObjectKey()
	}

	m.mu.RLock()
	k, avail := m.keyAvailable(sha)
	_, known := m.keys[sha]
	m.mu.RUnlock()
	if !avail {
		if known {
			return nil, fmt.Errorf("%w for %s", ErrCPKKeyUnavailable, path)
		}
		return nil, fmt.Errorf("%w for %s", ErrCPKKeyNotRegistered, path)
	}
	return k, nil
}

// NewObjectKey returns the key for a brand-new object. While a candidate key has
// failed validation new writes are paused, but retained old keys keep serving
// reads and writes to already-open versions.
func (m *CPKManager) NewObjectKey() (*cpkKeyVersion, error) {
	if m == nil || !m.enabled {
		return nil, ErrCPKDisabled
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.phase == cpkPhaseNewKeyRejected {
		return nil, ErrCPKNewWritesPaused
	}
	k, avail := m.keyAvailable(m.activeSHA)
	if !avail {
		return nil, fmt.Errorf("%w: active key", ErrCPKKeyUnavailable)
	}
	return k, nil
}

// KeyForSHA returns the registered version for a remote-observed sha. It fails
// with ErrCPKKeyNotRegistered when the sha belongs to a key the mount does not
// hold.
func (m *CPKManager) KeyForSHA(sha string) (*cpkKeyVersion, error) {
	if m == nil || !m.enabled {
		return nil, ErrCPKDisabled
	}
	if sha == "" {
		m.mu.RLock()
		sha = m.activeSHA
		m.mu.RUnlock()
	}
	m.mu.RLock()
	k, avail := m.keyAvailable(sha)
	m.mu.RUnlock()
	if !avail {
		if _, known := m.keys[sha]; known {
			return nil, fmt.Errorf("%w for %s", ErrCPKKeyUnavailable, sha)
		}
		return nil, fmt.Errorf("%w: %s", ErrCPKKeyNotRegistered, sha)
	}
	return k, nil
}

// MarkKeyAvailability lets the storage layer report that a retained key started
// failing (e.g. transient vault issue) or became usable again. Reads of objects
// on other keys continue unaffected.
func (m *CPKManager) MarkKeyAvailability(sha string, available bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if k, ok := m.keys[sha]; ok {
		k.available = available
		log.Warn("CPKManager::MarkKeyAvailability : key %s availability set to %v", sha, available)
	}
}

// ApplyConfiguredKeys reconciles the ring with the ordered key list from config
// (first entry is the desired active key). Behaviour:
//   - identical active key: retained keys are reconciled, no rotation;
//   - new active key: remotely probed; on success the old active key is retained
//     and a rotation begins, on failure new writes stay paused and the old key
//     remains active for reads;
//   - keys no longer configured are kept (possibly unavailable) while objects or
//     open versions still reference them, and are only reported revocable once
//     nothing references them.
func (m *CPKManager) ApplyConfiguredKeys(keys []cpkKeyMaterial, prober cpkKeyProber) (rotated bool, err error) {
	if m == nil || !m.enabled {
		return false, nil
	}
	if len(keys) == 0 {
		return false, errors.New("cpk is enabled but no encryption key is configured")
	}
	for _, km := range keys {
		if err = ValidateCPKMaterial(km.key, km.sha); err != nil {
			return false, err
		}
	}

	wantedActive := keys[0]

	m.mu.Lock()
	currentActive := m.activeSHA
	phase := m.phase
	m.mu.Unlock()

	if wantedActive.sha == currentActive && phase != cpkPhaseNewKeyRejected {
		m.reconcileRetainedKeys(keys)
		log.Info("CPKManager::ApplyConfiguredKeys : active key unchanged, key ring reconciled (%d keys)", len(keys))
		return false, nil
	}

	// Validate the candidate key before changing anything. Local validation
	// already passed; the service probe confirms the key is accepted end to end.
	if prober != nil {
		if perr := prober.probeCPKKey(wantedActive); perr != nil {
			m.mu.Lock()
			m.phase = cpkPhaseNewKeyRejected
			m.rejectReason = perr.Error()
			m.updatedAt = time.Now()
			// Make sure the candidate material is present so it can be retried
			// without an unmount, but keep it inactive and unavailable.
			m.upsertKeyLocked(wantedActive, false, false)
			m.mu.Unlock()
			log.Err("CPKManager::ApplyConfiguredKeys : new key validation failed, new writes paused [%s]", perr.Error())
			return false, fmt.Errorf("new cpk validation failed, writes are paused: %w", perr)
		}
	}

	m.mu.Lock()
	oldActive := m.activeSHA
	m.upsertKeyLocked(wantedActive, true, true)
	m.activeSHA = wantedActive.sha
	for _, k := range m.keys {
		k.active = k.material.sha == wantedActive.sha
		if !k.active && k.retiredAt.IsZero() {
			k.retiredAt = time.Now()
		}
	}
	m.phase = cpkPhaseRotating
	m.rejectReason = ""
	m.rotationID++
	rid := m.rotationID
	m.updatedAt = time.Now()
	j := m.journal
	m.mu.Unlock()

	m.reconcileRetainedKeys(keys)

	if j != nil {
		retired := m.retainedSHAs()
		if jerr := j.beginRotation(rid, wantedActive.sha, oldActive, retired); jerr != nil {
			log.Err("CPKManager::ApplyConfiguredKeys : failed to persist rotation start [%s]", jerr.Error())
		}
	}

	log.Crit("CPKManager::ApplyConfiguredKeys : rotation %d started, active key %s, old key %s retained for existing objects",
		rid, wantedActive.sha, oldActive)
	return true, nil
}

// upsertKeyLocked inserts or refreshes a key. Caller holds m.mu.
func (m *CPKManager) upsertKeyLocked(km cpkKeyMaterial, active, available bool) {
	if k, ok := m.keys[km.sha]; ok {
		k.material = km
		k.active = active
		k.available = available || km.sha == m.activeSHA
		return
	}
	m.keys[km.sha] = &cpkKeyVersion{
		material:  km,
		active:    active,
		available: available,
	}
}

// reconcileRetainedKeys refreshes material of keys still present in config and
// marks keys dropped from config unavailable (they remain in the ring while
// referenced and become garbage once revocable).
func (m *CPKManager) reconcileRetainedKeys(keys []cpkKeyMaterial) {
	want := make(map[string]cpkKeyMaterial, len(keys))
	for _, km := range keys {
		want[km.sha] = km
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for sha, k := range m.keys {
		if km, ok := want[sha]; ok {
			k.material = km
			k.available = true
		} else if !k.active {
			k.available = false
		}
	}
}

// retainedSHAs returns shas of retired keys currently in the ring.
func (m *CPKManager) retainedSHAs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.keys))
	for sha, k := range m.keys {
		if !k.active {
			out = append(out, sha)
		}
	}
	return out
}

// noteSweep is used by the migrator to record progress counters and phase
// transition inputs after each sweep.
func (m *CPKManager) noteSweep(s cpkSweepSummary) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSweepAt = time.Now()
	m.lastSweepTotal = s.remoteTotal
	m.lastSweepMigrating = s.migrating
	m.lastSweepUnmanaged = s.unmanaged
	m.lastSweepSkippedDirs = s.skippedDirectories
	m.lastSweepOpenWaiting = s.openVersions
	m.cumulativeMigrated = s.cumulative
	m.updatedAt = time.Now()
}

// HasOpenVersion reports whether an unfinished file version (open/created
// handle) keeps path pinned, so migration must not republish it yet.
func (m *CPKManager) HasOpenVersion(path string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.boundSHALocked(path)
	return ok
}

func (m *CPKManager) noteRemoteSweep(refs map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.remoteRefs = make(map[string]int, len(refs))
	for k, v := range refs {
		m.remoteRefs[k] = v
	}
	m.remoteRefsAt = time.Now()
	m.remoteRefsScoped = true
}

func (m *CPKManager) setPhase(phase cpkRotationPhase) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.phase = phase
	m.updatedAt = time.Now()
}

// CPKKeyStatus is the queryable per-key view used for revocation reporting.
type CPKKeyStatus struct {
	SHA            string
	Active         bool
	Available      bool
	OpenHandles    int
	JournalObjects int
	RemoteObjects  int
	RemoteSwept    bool
	RetiredAt      time.Time
	Revocable      bool
}

// CPKStatusSnapshot is the queryable rotation progress view.
type CPKStatusSnapshot struct {
	Enabled             bool
	SingleKeyMode       bool
	Phase               cpkRotationPhase
	ActiveSHA           string
	RejectReason        string
	RotationID          uint64
	StartedAt           time.Time
	UpdatedAt           time.Time
	LastSweepAt         time.Time
	LastSweepObjects    int
	ObjectsMigratingNow int
	CumulativeMigrated  int64
	// UnmanagedObjects are encrypted with a key sha that is not configured on
	// this mount; they cannot be migrated until the matching retained key is
	// supplied again.
	UnmanagedObjects int
	// SkippedDirectories are HNS directory entries that are intentionally not
	// rewritten through the blob endpoint and still pin their old key.
	SkippedDirectories int
	Pending            int
	InProgress         int
	Unconfirmed        int
	Failed             int
	Keys               []CPKKeyStatus
}

// Status assembles a consistent progress snapshot from the ring and journal.
func (m *CPKManager) Status() CPKStatusSnapshot {
	if m == nil {
		return CPKStatusSnapshot{Enabled: false, Phase: cpkPhaseStable}
	}

	m.mu.RLock()
	snap := CPKStatusSnapshot{
		Enabled:             m.enabled,
		SingleKeyMode:       len(m.keys) <= 1,
		Phase:               m.phase,
		ActiveSHA:           m.activeSHA,
		RejectReason:        m.rejectReason,
		RotationID:          m.rotationID,
		StartedAt:           m.startedAt,
		UpdatedAt:           m.updatedAt,
		LastSweepAt:         m.lastSweepAt,
		LastSweepObjects:    m.lastSweepTotal,
		ObjectsMigratingNow: m.lastSweepMigrating,
		UnmanagedObjects:    m.lastSweepUnmanaged,
		SkippedDirectories:  m.lastSweepSkippedDirs,
		CumulativeMigrated:  m.cumulativeMigrated,
	}

	bindPerSHA := make(map[string]int)
	for _, b := range m.bindings {
		bindPerSHA[b.sha]++
	}

	journalRefs := map[string]int{}
	if m.journal != nil {
		for _, r := range m.journal.activeRecords() {
			journalRefs[r.OldSHA]++
			switch r.State {
			case cpkObjPending:
				snap.Pending++
			case cpkObjInProgress:
				snap.InProgress++
			case cpkObjUnconfirmed:
				snap.Unconfirmed++
			case cpkObjFailed:
				snap.Failed++
			}
		}
	}

	for sha, k := range m.keys {
		st := CPKKeyStatus{
			SHA:            sha,
			Active:         k.active,
			Available:      k.available,
			OpenHandles:    bindPerSHA[sha],
			JournalObjects: journalRefs[sha],
			RetiredAt:      k.retiredAt,
		}
		if m.remoteRefsScoped {
			st.RemoteSwept = true
			st.RemoteObjects = m.remoteRefs[sha]
		}
		// A retired key may only be revoked after a fresh remote sweep found no
		// object under its identifier, the journal has no unfinished record and
		// no open version still pins it.
		st.Revocable = !k.active && !k.retiredAt.IsZero() && st.RemoteSwept &&
			st.RemoteObjects == 0 && st.JournalObjects == 0 && st.OpenHandles == 0
		snap.Keys = append(snap.Keys, st)
	}
	m.mu.RUnlock()

	return snap
}

// RevocableKeys lists retired keys that nothing references anymore.
func (m *CPKManager) RevocableKeys() []string {
	snap := m.Status()
	out := make([]string, 0)
	for _, k := range snap.Keys {
		if k.Revocable {
			out = append(out, k.SHA)
		}
	}
	return out
}

// PruneRevoked drops revocable retained keys from the ring. A key is only
// dropped after RevocableKeys reports it, so it can never be removed while an
// object or handle still needs it.
func (m *CPKManager) PruneRevoked() []string {
	if m == nil {
		return nil
	}
	revocable := m.RevocableKeys()
	if len(revocable) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sha := range revocable {
		delete(m.keys, sha)
		log.Crit("CPKManager::PruneRevoked : retained key %s dropped, no object or handle references it", sha)
	}
	return revocable
}

// cpkSHAFromKeyB64 computes the base64 SHA-256 that Azure stores/returns as the
// customer-provided key identifier.
func cpkSHAFromKeyB64(keyB64 string) string {
	raw, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// sanitizeScope replaces characters unsafe for a local state file name.
func sanitizeScope(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, string('\\'), "_")
	s = strings.ReplaceAll(s, ":", "_")
	if s == "" {
		s = "default"
	}
	return s
}

// migrationSeq is shared for test deterministic counters if needed.
var cpkRuntimeSeq atomic.Uint64
