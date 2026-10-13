/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |---- |     | |-----| |-----  ------
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

package handlemap

import (
	"container/list"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/cache_policy"

	"go.uber.org/atomic"
)

type HandleID uint64

const InvalidHandleID HandleID = 0

// Flags represented in BitMap for various flags in the handle
const (
	HandleFlagUnknown uint64 = iota
	HandleFlagDirty          // File has been modified with write operation or is a new file
	HandleFlagFSynced        // User has called fsync on the file explicitly
	HandleFlagCached         // File is cached in the local system by blobfuse2
)

// Structure to hold in memory cache for streaming layer
type Cache struct {
	sync.RWMutex
	*cache_policy.LRUCache
	*common.BlockOffsetList
	StreamOnly  bool
	HandleCount int64
}

type Buffers struct {
	Cooked  *list.List
	Cooking *list.List
}

type Handle struct {
	sync.RWMutex
	FObj     *os.File // File object being represented by this handle
	CacheObj *Cache   // Streaming layer cache for this handle
	Buffers  *Buffers
	ID       HandleID // Blobfuse assigned unique ID to this handle
	Size     int64    // Size of the file being handled here
	Mtime    time.Time
	UnixFD   uint64          // Unix FD created by create/open syscall
	OptCnt   uint64          // Number of operations done on this file
	Flags    common.BitMap64 // Various states of the file
	Path     string          // Always holds path relative to mount dir
	nativeFH atomic.Uintptr  // C file_handle_t set by libfuse direct-IO open
	values   map[string]any  // Map to hold other info if application wants to store
}

// SetNativeFH records the libfuse native C file_handle_t so shutdown draining
// can observe the dirty byte maintained by the direct-IO native write path.
func (handle *Handle) SetNativeFH(fh uintptr) {
	handle.nativeFH.Store(fh)
}

// NativeFH returns the native file handle pointer, or 0 when unset.
func (handle *Handle) NativeFH() uintptr { return handle.nativeFH.Load() }

func NewHandle(path string) *Handle {
	return &Handle{
		ID:       InvalidHandleID,
		Path:     path,
		Size:     0,
		Flags:    0,
		OptCnt:   0,
		values:   make(map[string]any),
		CacheObj: nil,
		FObj:     nil,
		Buffers:  nil,
	}
}

// Dirty : Handle is dirty or not
func (handle *Handle) Dirty() bool {
	return handle.Flags.IsSet(HandleFlagDirty)
}

// nativeDirtyMu/nativeDirtyCheck bridge the pure-Go handlemap to the libfuse
// cgo layer: direct-IO writes only set a byte inside the native C
// file_handle_t until flush/release promotes it to the Go dirty flag. libfuse
// registers a reader at startup; a nil check means no cgo owner is present.
var (
	nativeDirtyMu    sync.RWMutex
	nativeDirtyCheck func(fh uintptr) bool
)

// RegisterNativeDirtyCheck installs the native (cgo) dirty-byte reader. It is
// invoked once by the libfuse component at startup.
func RegisterNativeDirtyCheck(f func(fh uintptr) bool) {
	nativeDirtyMu.Lock()
	nativeDirtyCheck = f
	nativeDirtyMu.Unlock()
}

// ReconcileDirty reports whether the handle holds writes not known committed,
// reconciling the native direct-IO dirty byte with the Go flag. When the
// native byte is set the Go flag is promoted here: Drain has already returned
// (no native write/flush is in flight) and the cache components' FlushFile
// paths decide on the Go flag, so without promotion a direct-IO handle would
// be skipped and its last write silently dropped at shutdown.
func (handle *Handle) ReconcileDirty() bool {
	if handle.Flags.IsSet(HandleFlagDirty) {
		return true
	}
	fh := handle.nativeFH.Load()
	if fh == 0 {
		return false
	}
	nativeDirtyMu.RLock()
	check := nativeDirtyCheck
	nativeDirtyMu.RUnlock()
	if check != nil && check(fh) {
		handle.Flags.Set(HandleFlagDirty)
		return true
	}
	return false
}

// Fsynced : Handle is Fsynced or not
func (handle *Handle) Fsynced() bool {
	return handle.Flags.IsSet(HandleFlagFSynced)
}

// Cached : File is cached on local disk or not
func (handle *Handle) Cached() bool {
	return handle.Flags.IsSet(HandleFlagCached)
}

// GetFileObject : Get the OS.File handle stored within
func (handle *Handle) GetFileObject() *os.File {
	return handle.FObj
}

// SetFileObject : Store the OS.File handle
func (handle *Handle) SetFileObject(f *os.File) {
	handle.FObj = f
}

// FD : Get Unix file descriptor
func (handle *Handle) FD() int {
	return int(handle.UnixFD)
}

// SetValue : Store user defined parameter inside handle
func (handle *Handle) SetValue(key string, value any) {
	handle.values[key] = value
}

// GetValue : Retrieve user defined parameter from handle
func (handle *Handle) GetValue(key string) (any, bool) {
	val, ok := handle.values[key]
	return val, ok
}

// GetValue : Retrieve user defined parameter from handle
func (handle *Handle) RemoveValue(key string) (any, bool) {
	val, ok := handle.values[key]
	delete(handle.values, key)
	return val, ok
}

// Cleanup : Delete all user defined parameter from handle
func (handle *Handle) Cleanup() {
	for key := range handle.values {
		delete(handle.values, key)
	}
}

// defaultHandleMap holds a synchronized map[ HandleID ]*Handle
var defaultHandleMap sync.Map
var nextHandleID = *atomic.NewUint64(uint64(0))

// Shutdown freeze gate. The hot path (every FUSE callback) pays only an
// atomic load plus, while the filesystem is active, an atomic increment;
// there are no mutex or channel operations per I/O.
var (
	gateFrozen  = atomic.NewBool(false)
	inFlightOps = atomic.NewInt64(0)

	drainCondMu sync.Mutex
	drainCond   = sync.NewCond(&drainCondMu)
)

// Freeze closes the admission gate: once it returns, IsFrozen is observable by
// every FUSE callback thread. It is idempotent.
func Freeze() {
	gateFrozen.Store(true)
}

// IsFrozen reports whether new mutating FUSE operations must be rejected.
func IsFrozen() bool {
	return gateFrozen.Load()
}

// EnterOp accounts for one FUSE operation entering the pipeline. It must be
// paired with LeaveOp. While frozen, libfuse rejects new mutations before they
// touch any component, but in-flight operations still need to reach a safe
// point; those that entered before the freeze are tracked here.
func EnterOp() {
	inFlightOps.Inc()
}

// LeaveOp marks one FUSE operation as finished. Unbalanced calls are a
// programming error that would make Drain wait forever.
func LeaveOp() {
	if inFlightOps.Dec() == 0 {
		// Take the cond lock around Broadcast so it can never race a Drain
		// that is between its counter check and Cond.Wait. The mutex is taken
		// only when the counter reaches zero, never on the per-I/O hot path.
		drainCondMu.Lock()
		drainCond.Broadcast()
		drainCondMu.Unlock()
	}
}

// InFlightOps returns the number of FUSE operations currently executing.
func InFlightOps() int64 {
	return inFlightOps.Load()
}

// externalDrainCheck, when registered, reports whether a second operation
// counter (the libfuse native C I/O path) has also drained. Components register
// it once at startup.
var (
	externalDrainMu    sync.RWMutex
	externalDrainCheck func() bool
)

// RegisterDrainCheck installs an additional zero-check consulted by Drain.
func RegisterDrainCheck(f func() bool) {
	externalDrainMu.Lock()
	externalDrainCheck = f
	externalDrainMu.Unlock()
}

// NotifyExternalDrained wakes a Drain waiter after an external counter (e.g.
// the native libfuse C callbacks) reached zero.
func NotifyExternalDrained() {
	drainCondMu.Lock()
	drainCond.Broadcast()
	drainCondMu.Unlock()
}

func externalDrained() bool {
	externalDrainMu.RLock()
	f := externalDrainCheck
	externalDrainMu.RUnlock()
	return f == nil || f()
}

// Drain waits until all in-flight operations have completed or maxWait
// elapses. It returns true when the operation set drained, false on timeout.
// A non-positive maxWait performs a single non-blocking check.
func Drain(maxWait time.Duration) bool {
	if inFlightOps.Load() == 0 && externalDrained() {
		return true
	}
	if maxWait <= 0 {
		return false
	}

	deadline := time.Now().Add(maxWait)
	drainCondMu.Lock()
	defer drainCondMu.Unlock()
	for inFlightOps.Load() > 0 || !externalDrained() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		// Re-arm the timeout on every wake: completion broadcasts can happen
		// while the counter is still non-zero, and a one-shot timer must not
		// consume the whole deadline prematurely.
		timer := time.AfterFunc(remaining, func() { drainCond.Broadcast() })
		drainCond.Wait()
		timer.Stop()
	}
	return true
}

// Snapshot returns the handles registered at the time of the call. Callers
// must not mutate the handles without their own locking; the slice itself is
// detached from the live map. It is used by the shutdown coordinator to freeze
// the set of handles that may need a final flush.
func Snapshot() []*Handle {
	handles := make([]*Handle, 0)
	defaultHandleMap.Range(func(_, value any) bool {
		if h, ok := value.(*Handle); ok {
			handles = append(handles, h)
		}
		return true
	})
	return handles
}

// ResetGateForTest restores the gate to an unfrozen, zero-inflight state.
// Exported for cross-package unit tests (component/libfuse); never call from
// production code.
func ResetGateForTest() {
	gateFrozen.Store(false)
	inFlightOps.Store(0)
	externalDrainMu.Lock()
	externalDrainCheck = nil
	externalDrainMu.Unlock()
	nativeDirtyMu.Lock()
	nativeDirtyCheck = nil
	nativeDirtyMu.Unlock()
}

// Add : Add the newly created handle to map and allocate a handle id
func Add(handle *Handle) HandleID {
	var ok = true
	var key HandleID
	for ok {
		key = HandleID(nextHandleID.Inc())
		_, ok = defaultHandleMap.LoadOrStore(key, handle)
	}
	handle.ID = key
	return key
}

// Delete : Remove handle object from map
func Delete(key HandleID) {
	defaultHandleMap.Delete(key)
}

func CreateCacheObject(capacity int64, handle *Handle) {
	handle.CacheObj = &Cache{
		sync.RWMutex{},
		cache_policy.NewLRUCache(capacity),
		&common.BlockOffsetList{},
		false,
		0,
	}
}

// GetHandles : Get map of handles stored
func GetHandles() *sync.Map {
	return &defaultHandleMap
}

// Load : Search the handle object based on its id
func Load(key HandleID) (*Handle, bool) {
	handleIF, ok := defaultHandleMap.Load(key)
	if !ok {
		return nil, false
	}
	handle := handleIF.(*Handle)
	return handle, true
}

// Store function must not be used in production application.
// This is a utility function present only for test scenarios.
func Store(key HandleID, path string, _ uintptr) *Handle {
	handle := &Handle{
		ID:   key,
		Path: path,
	}
	defaultHandleMap.Store(key, handle)
	return handle
}
