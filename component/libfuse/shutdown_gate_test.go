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

package libfuse

import (
	"syscall"
	"testing"

	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
)

func TestAdmitOpBeforeFreeze(t *testing.T) {
	handlemap.ResetGateForTest()
	for _, op := range []string{
		opGetAttr, opStatFs, opOpenDir, opReadDir, opReleaseDir, opReadLink,
		opMkDir, opRmDir, opCreate, opTruncate, opUnlink, opRename,
		opSymLink, opChmod, opChown, opUTimens, opRelease, opFSync, opFSyncDir,
	} {
		if !admitOp(op) {
			t.Fatalf("op %s rejected before freeze", op)
		}
	}
}

func TestAdmitOpAfterFreeze(t *testing.T) {
	handlemap.ResetGateForTest()
	handlemap.Freeze()
	defer handlemap.ResetGateForTest()

	mustReject := []string{
		opMkDir, opRmDir, opCreate, opTruncate, opUnlink, opRename,
		opSymLink, opChmod, opChown, opUTimens,
	}
	for _, op := range mustReject {
		if admitOp(op) {
			t.Fatalf("mutating op %s admitted after freeze", op)
		}
	}

	mustAllow := []string{
		opGetAttr, opStatFs, opOpenDir, opReadDir, opReleaseDir, opReadLink,
		// lifecycle / drain path stays open
		opRelease, opFSync, opFSyncDir,
	}
	for _, op := range mustAllow {
		if !admitOp(op) {
			t.Fatalf("op %s must stay admitted after freeze", op)
		}
	}
}

func TestEnterFuseOpPairsCounter(t *testing.T) {
	handlemap.ResetGateForTest()
	defer handlemap.ResetGateForTest()

	if !enterFuseOp(opMkDir) {
		t.Fatal("mkdir admitted expected")
	}
	if handlemap.InFlightOps() != 1 {
		t.Fatalf("inflight = %d", handlemap.InFlightOps())
	}
	handlemap.LeaveOp()
	if handlemap.InFlightOps() != 0 {
		t.Fatal("counter not balanced")
	}

	handlemap.Freeze()
	if enterFuseOp(opMkDir) {
		t.Fatal("mkdir must be rejected frozen")
	}
	if handlemap.InFlightOps() != 0 {
		t.Fatal("rejected op left counter non-zero")
	}
}

func TestAdmitOpenFlags(t *testing.T) {
	handlemap.ResetGateForTest()
	defer handlemap.ResetGateForTest()

	rdonly := uint32(syscall.O_RDONLY)
	if !admitOpenFlags(rdonly) {
		t.Fatal("rdonly rejected before freeze")
	}

	handlemap.Freeze()
	cases := []struct {
		name  string
		flags uint32
		want  bool
	}{
		{"rdonly", uint32(syscall.O_RDONLY), true},
		{"wronly", uint32(syscall.O_WRONLY), false},
		{"rdwr", uint32(syscall.O_RDWR), false},
		{"rdonly-trunc", uint32(syscall.O_RDONLY | syscall.O_TRUNC), false},
		{"rdonly-append", uint32(syscall.O_RDONLY | syscall.O_APPEND), false},
		{"rdonly-creat", uint32(syscall.O_RDONLY | syscall.O_CREAT), false},
	}
	for _, tc := range cases {
		if got := admitOpenFlags(tc.flags); got != tc.want {
			t.Fatalf("%s: admit=%v want %v", tc.name, got, tc.want)
		}
	}
}
