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

	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"
)

// FUSE operation classes used by the shutdown admission gate. The names mirror
// the libfuse callback suffixes; only the decision lives in this file so it can
// be unit tested without a live FUSE session.
const (
	opGetAttr    = "getattr"
	opStatFs     = "statfs"
	opOpenDir    = "opendir"
	opReadDir    = "readdir"
	opReleaseDir = "releasedir"
	opReadLink   = "readlink"
	opMkDir      = "mkdir"
	opRmDir      = "rmdir"
	opCreate     = "create"
	opRelease    = "release"
	opFSync      = "fsync"
	opFSyncDir   = "fsyncdir"
	opTruncate   = "truncate"
	opUnlink     = "unlink"
	opRename     = "rename"
	opSymLink    = "symlink"
	opChmod      = "chmod"
	opChown      = "chown"
	opUTimens    = "utimens"
)

// mutatingOps produce dirty state or namespace changes and are rejected with
// ESHUTDOWN once the freeze gate is closed. flush/fsync/release are NOT in this
// set: they are the drain path and must still run to completion.
var mutatingOps = map[string]struct{}{
	opMkDir:    {},
	opRmDir:    {},
	opCreate:   {},
	opTruncate: {},
	opUnlink:   {},
	opRename:   {},
	opSymLink:  {},
	opChmod:    {},
	opChown:    {},
	opUTimens:  {},
}

// IsMutatingOp reports whether the operation changes data or the namespace.
func IsMutatingOp(op string) bool {
	_, ok := mutatingOps[op]
	return ok
}

// admitOp returns false when the operation must return -ESHUTDOWN.
func admitOp(op string) bool {
	return !handlemap.IsFrozen() || !IsMutatingOp(op)
}

// enterFuseOp accounts an in-flight Go-side FUSE callback and enforces the
// freeze gate. A rejected call is already paired with LeaveOp on return.
func enterFuseOp(op string) bool {
	handlemap.EnterOp()
	if !admitOp(op) {
		handlemap.LeaveOp()
		return false
	}
	return true
}

// admitOpenFlags keeps open() usable for pure reads after the freeze but
// rejects anything that can create or mutate a file (write modes, O_TRUNC,
// O_APPEND, O_CREAT). Kernel writeback caching cannot then turn a frozen
// session into new dirty data.
func admitOpenFlags(flags uint32) bool {
	if !handlemap.IsFrozen() {
		return true
	}
	if flags&uint32(syscall.O_ACCMODE) != uint32(syscall.O_RDONLY) {
		return false
	}
	if flags&(uint32(syscall.O_TRUNC)|uint32(syscall.O_APPEND)|uint32(syscall.O_CREAT)) != 0 {
		return false
	}
	return true
}

// enterFuseOpen is the open() variant of enterFuseOp.
func enterFuseOpen(flags uint32) bool {
	handlemap.EnterOp()
	if !admitOpenFlags(flags) {
		handlemap.LeaveOp()
		return false
	}
	return true
}

// freezeAdmissions closes both the Go and the native-C shutdown gates. After
// it returns, all mutating FUSE callbacks observe the frozen state.
func freezeAdmissions() {
	handlemap.Freeze()
	freezeNativeGate()
}
