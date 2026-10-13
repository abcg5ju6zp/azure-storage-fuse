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

package recovery

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/Azure/azure-storage-fuse/v2/internal/shutdown"
)

func TestManifestPathAndEscape(t *testing.T) {
	if got := EscapeMountPath("/mnt/a/b"); got != "_mnt_a_b" {
		t.Fatalf("escape = %q", got)
	}
	p := ManifestPath("$HOME/.blobfuse2", "/mnt/x", "file_cache")
	if filepath.Base(p) != "_mnt_x-file_cache.json" || filepath.Base(filepath.Dir(p)) != "recovery" {
		t.Fatalf("manifest path = %q", p)
	}
	if filepath.Base(StatusPath("$HOME/.blobfuse2", "/mnt/x")) != "_mnt_x.stop.json" {
		t.Fatalf("status path = %q", p)
	}
}

func TestManifestSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")

	m := &Manifest{
		MountPath: "/mnt/x",
		Component: "file_cache",
		Items: []internal.UncommittedItem{
			{Component: "file_cache", Name: "a", LocalPath: "/cache/a", Reason: internal.UncommittedFlushTimeout},
		},
	}
	if err := SaveManifest(path, m); err != nil {
		t.Fatal(err)
	}

	got, err := LoadManifest(path)
	if err != nil || got == nil {
		t.Fatalf("load: %v %v", got, err)
	}
	if got.SchemaVersion != SchemaVersion || len(got.Items) != 1 ||
		got.Items[0].Reason != internal.UncommittedFlushTimeout {
		t.Fatalf("bad roundtrip: %+v", got)
	}

	// No leftover temp file.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
}

func TestLoadMissingAndCorrupt(t *testing.T) {
	dir := t.TempDir()

	m, err := LoadManifest(filepath.Join(dir, "missing.json"))
	if err != nil || m != nil {
		t.Fatalf("missing manifest: %v %v", m, err)
	}

	corrupt := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(corrupt); err == nil {
		t.Fatal("corrupt manifest must return error, not be silently ignored")
	}
}

func TestMergeItemsDedupAcrossShutdowns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")

	items1 := []internal.UncommittedItem{
		{Component: "file_cache", Name: "a", LocalPath: dir + "/a", Reason: internal.UncommittedUploadFailed},
	}
	m, err := MergeItems(path, "/mnt", dir, "file_cache", "id1", items1)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Items) != 1 {
		t.Fatalf("items = %d", len(m.Items))
	}

	items2 := []internal.UncommittedItem{
		{Component: "file_cache", Name: "a", LocalPath: dir + "/a", Reason: internal.UncommittedUploadFailed}, // dup
		{Component: "file_cache", Name: "b", LocalPath: dir + "/b", Reason: internal.UncommittedForcedSignal},
	}
	m, err = MergeItems(path, "/mnt", dir, "file_cache", "id2", items2)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Items) != 2 {
		t.Fatalf("merged items = %d, want 2", len(m.Items))
	}
	if m.Identity != "id2" {
		t.Fatalf("identity not updated: %s", m.Identity)
	}

	if err := DeleteManifest(path); err != nil {
		t.Fatal(err)
	}
	if m, err := LoadManifest(path); err != nil || m != nil {
		t.Fatalf("manifest not deleted: %v %v", m, err)
	}
}

func TestConcurrentSaveNoHalfFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := &Manifest{
				Component: "file_cache",
				Items: []internal.UncommittedItem{{
					Component: "file_cache",
					Name:      string(rune('a' + i)),
					Reason:    internal.UncommittedFlushTimeout,
				}},
			}
			_ = SaveManifest(path, m)
		}(i)
	}
	wg.Wait()

	// A reader must always see a complete JSON document.
	m, err := LoadManifest(path)
	if err != nil || m == nil {
		t.Fatalf("final manifest unreadable: %v", err)
	}
	if len(m.Items) != 1 {
		t.Fatalf("unexpected items: %d", len(m.Items))
	}
}

func TestProtectedSet(t *testing.T) {
	s := NewProtectedSet("/tmp/cache/a", "/tmp/cache/dir/sub")

	if !s.Has("/tmp/cache/a") {
		t.Fatal("exact member not found")
	}
	// Parent subtree of a protected member is protected (cleanup removes
	// first-level subtrees wholesale).
	if !s.IsProtected("/tmp/cache/dir") {
		t.Fatal("parent subtree not protected")
	}
	if !s.IsProtected("/tmp/cache/a") {
		t.Fatal("exact file not protected")
	}
	// Unrelated entries are not protected.
	if s.IsProtected("/tmp/cache/other") {
		t.Fatal("unrelated path protected")
	}
	// Path inside a protected directory is protected.
	s.Add("/tmp/cache/x")
	if !s.IsProtected("/tmp/cache/x/deep/nested") {
		t.Fatal("nested child not protected")
	}

	var nilSet *ProtectedSet
	if nilSet.IsProtected("/x") {
		t.Fatal("nil set must protect nothing")
	}
}

func TestProtectedSetFromManifest(t *testing.T) {
	m := &Manifest{Items: []internal.UncommittedItem{
		{LocalPath: "/c/1"},
		{LocalPath: ""}, // in-memory-lost has no local path
		{LocalPath: "/c/2"},
	}}
	s := ProtectedSetFromManifest(m)
	if s.Len() != 2 {
		t.Fatalf("protected len = %d", s.Len())
	}
}

func TestStatusSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.stop.json")

	cfg := shutdown.DefaultConfig()
	c := shutdown.New(cfg)
	id, owner := c.Begin(shutdown.TriggerSignal, "sigterm")
	if !owner {
		t.Fatal("owner")
	}
	c.SetOutcome(shutdown.Outcome{DataUnsafe: true, UncommittedCount: 2})
	c.Finish()

	s := &StopStatus{
		MountPath: "/mnt/x",
		Pid:       123,
		ExitCode:  shutdown.ExitDataUnsafe,
		Shutdown:  c.Snapshot(),
		Items: []internal.UncommittedItem{
			{Component: "file_cache", Name: "a", Reason: internal.UncommittedFlushTimeout},
		},
	}
	if err := SaveStatus(path, s); err != nil {
		t.Fatal(err)
	}
	got, err := LoadStatus(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExitCode != 75 || got.Shutdown.Identity.ID != id.ID || got.Items[0].Name != "a" {
		t.Fatalf("bad status roundtrip: %+v", got)
	}
}
