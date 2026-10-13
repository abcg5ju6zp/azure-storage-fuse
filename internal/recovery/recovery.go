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

// Package recovery persists uncommitted shutdown data (recovery manifest) and
// the machine-readable result of the last shutdown (stop status) so that data
// survives process exit and can be re-uploaded on the next mount.
package recovery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

// SchemaVersion is the manifest/status file format version.
const SchemaVersion = 1

// Manifest lists cache data that was not confirmed committed when a mount
// shut down. One manifest exists per (mount, cache component).
type Manifest struct {
	SchemaVersion int                        `json:"schema-version"`
	MountPath     string                     `json:"mount-path"`
	CachePath     string                     `json:"cache-path,omitempty"`
	Component     string                     `json:"component"`
	Identity      string                     `json:"identity,omitempty"`
	CreatedAt     time.Time                  `json:"created-at"`
	UpdatedAt     time.Time                  `json:"updated-at"`
	Items         []internal.UncommittedItem `json:"items"`
}

// StopStatus is the machine-readable outcome of the last mount shutdown.
type StopStatus struct {
	SchemaVersion int                        `json:"schema-version"`
	MountPath     string                     `json:"mount-path"`
	Pid           int                        `json:"pid"`
	ExitCode      int                        `json:"exit-code"`
	FinishedAt    time.Time                  `json:"finished-at"`
	Shutdown      shutdown.Snapshot          `json:"shutdown"`
	Items         []internal.UncommittedItem `json:"items"`
}

// EscapeMountPath converts a mount path into the stable file name fragment
// shared with the daemon pid file ("a/b" -> "_a_b").
func EscapeMountPath(mountPath string) string {
	return strings.ReplaceAll(mountPath, "/", "_")
}

// ManifestPath returns the manifest location for a mount/component pair.
func ManifestPath(workDir, mountPath, component string) string {
	name := fmt.Sprintf("%s-%s.json", EscapeMountPath(mountPath), component)
	return common.ExpandPath(filepath.Join(workDir, "recovery", name))
}

// StatusPath returns the stop status file location for a mount.
func StatusPath(workDir, mountPath string) string {
	return common.ExpandPath(filepath.Join(workDir, EscapeMountPath(mountPath)+".stop.json"))
}

// LoadManifest reads and validates a manifest. A missing file returns
// (nil, nil); parse/version errors are returned so callers fail conservatively
// instead of wiping the residue.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	if m.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("manifest %s schema version %d unsupported (want %d)",
			path, m.SchemaVersion, SchemaVersion)
	}
	return &m, nil
}

// SaveManifest atomically writes (temp + fsync + rename + fsync dir) the
// manifest with 0600 permissions.
func SaveManifest(path string, m *Manifest) error {
	m.SchemaVersion = SchemaVersion
	if m.UpdatedAt.IsZero() {
		m.UpdatedAt = time.Now()
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = m.UpdatedAt
	}
	return atomicWriteJSON(path, m, 0o600)
}

// MergeItems loads any existing manifest for the same target, appends new
// items (de-duplicated by component+name+localPath+blockID), and saves the
// merged result, preserving residue across repeated abnormal shutdowns.
func MergeItems(path string, mountPath, cachePath, component, identity string,
	items []internal.UncommittedItem) (*Manifest, error) {

	m, err := LoadManifest(path)
	if err != nil {
		return nil, err
	}
	if m == nil {
		m = &Manifest{
			MountPath: mountPath,
			CachePath: cachePath,
			Component: component,
			Identity:  identity,
			Items:     make([]internal.UncommittedItem, 0, len(items)),
		}
	}

	existing := make(map[string]struct{}, len(m.Items))
	for _, it := range m.Items {
		existing[itemKey(it)] = struct{}{}
	}
	for _, it := range items {
		if _, ok := existing[itemKey(it)]; ok {
			continue
		}
		m.Items = append(m.Items, it)
		existing[itemKey(it)] = struct{}{}
	}
	m.Identity = identity
	if err := SaveManifest(path, m); err != nil {
		return nil, err
	}
	return m, nil
}

// DeleteManifest removes a manifest after all items were recovered. A missing
// file is not an error.
func DeleteManifest(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// HasManifest reports whether a recovery manifest currently exists for a
// mount/component pair. Cache components consult it at Configure time so the
// legacy "temp directory not empty" guard does not reject a mount whose
// predecessor deliberately preserved cache residue for startup recovery.
func HasManifest(workDir, mountPath, component string) bool {
	_, err := os.Stat(ManifestPath(workDir, mountPath, component))
	return err == nil
}

func itemKey(it internal.UncommittedItem) string {
	block := strings.Join(it.BlockIDs, ",")
	return it.Component + "|" + it.Name + "|" + it.LocalPath + "|" + block + "|" + it.Reason
}

// SaveStatus atomically writes the stop status file (0644).
func SaveStatus(path string, s *StopStatus) error {
	s.SchemaVersion = SchemaVersion
	if s.FinishedAt.IsZero() {
		s.FinishedAt = time.Now()
	}
	return atomicWriteJSON(path, s, 0o644)
}

// LoadStatus reads the latest stop status. Missing file returns (nil, nil).
func LoadStatus(path string) (*StopStatus, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var s StopStatus
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse stop status %s: %w", path, err)
	}
	return &s, nil
}

// atomicWriteJSON marshals v to a temp file in the same directory, fsyncs it,
// renames into place and fsyncs the directory, so a crash never leaves a
// half-written JSON at the final path.
func atomicWriteJSON(path string, v any, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create manifest dir: %w", err)
	}

	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// ProtectedSet is the set of absolute local paths that cache cleanup must not
// delete. It protects both the files themselves and any directory entry that
// contains a protected file (TempCacheCleanup removes whole first-level
// subtrees).
type ProtectedSet struct {
	paths map[string]struct{}
}

// NewProtectedSet builds a set from the given absolute paths.
func NewProtectedSet(paths ...string) *ProtectedSet {
	s := &ProtectedSet{paths: make(map[string]struct{}, len(paths))}
	for _, p := range paths {
		s.Add(p)
	}
	return s
}

// Add inserts one path; empty paths are ignored.
func (s *ProtectedSet) Add(path string) {
	if path != "" {
		s.paths[filepath.Clean(path)] = struct{}{}
	}
}

// Len returns the number of protected paths.
func (s *ProtectedSet) Len() int { return len(s.paths) }

// Has reports exact membership.
func (s *ProtectedSet) Has(path string) bool {
	if s == nil {
		return false
	}
	_, ok := s.paths[filepath.Clean(path)]
	return ok
}

// IsProtected reports whether path itself or anything below it is protected,
// or whether path lives inside a protected directory.
func (s *ProtectedSet) IsProtected(path string) bool {
	if s == nil {
		return false
	}
	p := filepath.Clean(path)
	if _, ok := s.paths[p]; ok {
		return true
	}
	for protected := range s.paths {
		if strings.HasPrefix(protected, p+string(os.PathSeparator)) ||
			strings.HasPrefix(p, protected+string(os.PathSeparator)) {
			return true
		}
	}
	return false
}

// Paths returns a sorted snapshot of protected paths.
func (s *ProtectedSet) Paths() []string {
	out := make([]string, 0, len(s.paths))
	for p := range s.paths {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// ProtectedSetFromManifest collects manifest item local paths.
func ProtectedSetFromManifest(m *Manifest) *ProtectedSet {
	s := NewProtectedSet()
	if m == nil {
		return s
	}
	for _, it := range m.Items {
		s.Add(it.LocalPath)
	}
	return s
}
