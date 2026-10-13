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

package common

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTempCacheCleanupProtected(t *testing.T) {
	root := t.TempDir()

	// Layout: root/keep/file, root/keep/nested/deep, root/remove/file
	mustFile := func(p string) {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustFile(filepath.Join(root, "keep", "a"))
	mustFile(filepath.Join(root, "keep", "nested", "deep", "b"))
	mustFile(filepath.Join(root, "remove", "c"))
	mustFile(filepath.Join(root, "plain"))

	protected := NewCleanupProtectorForTest(root, filepath.Join(root, "keep", "a"))

	report, err := TempCacheCleanupProtected(root, protected)
	if err != nil {
		t.Fatal(err)
	}
	if report.Skipped != 1 || report.Removed != 2 {
		t.Fatalf("report = %+v", report)
	}

	// The protected subtree survives including nested files.
	if _, err := os.Stat(filepath.Join(root, "keep", "nested", "deep", "b")); err != nil {
		t.Fatalf("protected nested file removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "remove", "c")); !os.IsNotExist(err) {
		t.Fatal("unprotected subtree not removed")
	}
	if _, err := os.Stat(filepath.Join(root, "plain")); !os.IsNotExist(err) {
		t.Fatal("unprotected file not removed")
	}
}

func TestTempCacheCleanupNilPredicateMatchesLegacy(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	report, err := TempCacheCleanupProtected(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Removed != 1 || report.Skipped != 0 {
		t.Fatalf("report = %+v", report)
	}
	if err := TempCacheCleanup(root); err != nil {
		t.Fatal(err)
	}
}

// NewCleanupProtectorForTest builds a simple predicate protecting one file.
func NewCleanupProtectorForTest(root, protectedFile string) func(absPath string) bool {
	return func(absPath string) bool {
		// Protect a first-level entry when it equals or contains the file.
		return absPath == protectedFile ||
			(len(protectedFile) > len(absPath) &&
				protectedFile[:len(absPath)] == absPath &&
				protectedFile[len(absPath)] == filepath.Separator)
	}
}
