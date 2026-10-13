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

package handlemap

import (
	"sync"
	"testing"
	"time"
)

func TestFreezeGate(t *testing.T) {
	ResetGateForTest()
	if IsFrozen() {
		t.Fatal("gate must start unfrozen")
	}
	Freeze()
	if !IsFrozen() {
		t.Fatal("Freeze not observed")
	}
	Freeze() // idempotent
	if !IsFrozen() {
		t.Fatal("second Freeze changed state")
	}
	ResetGateForTest()
}

func TestDrainEmpty(t *testing.T) {
	ResetGateForTest()
	if !Drain(time.Second) {
		t.Fatal("Drain with no in-flight ops must return true immediately")
	}
	if Drain(0) != true {
		t.Fatal("zero-wait drain on empty set must return true")
	}
}

func TestDrainWaitsForInFlight(t *testing.T) {
	ResetGateForTest()
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		EnterOp()
	}
	if InFlightOps() != n {
		t.Fatalf("inflight = %d", InFlightOps())
	}
	if Drain(0) {
		t.Fatal("non-blocking Drain must be false while ops run")
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			LeaveOp()
		}()
	}

	start := time.Now()
	if !Drain(5 * time.Second) {
		t.Fatal("Drain did not observe completion of all ops")
	}
	if time.Since(start) > time.Second {
		t.Fatal("Drain waited far longer than the op completion time")
	}
	wg.Wait()
	if InFlightOps() != 0 {
		t.Fatalf("inflight = %d after drain", InFlightOps())
	}
}

func TestDrainTimeout(t *testing.T) {
	ResetGateForTest()
	EnterOp() // never paired with LeaveOp
	start := time.Now()
	if Drain(40 * time.Millisecond) {
		t.Fatal("Drain returned true for an op that never completed")
	}
	if time.Since(start) > time.Second {
		t.Fatal("Drain blocked past its timeout")
	}
	LeaveOp() // balance the counter so other tests stay clean
	ResetGateForTest()
}

func TestSnapshotContainsRegisteredHandles(t *testing.T) {
	h1 := NewHandle("dir/a")
	id1 := Add(h1)
	h2 := NewHandle("dir/b")
	id2 := Add(h2)
	defer Delete(id1)
	defer Delete(id2)

	found := map[HandleID]bool{}
	for _, h := range Snapshot() {
		found[h.ID] = true
	}
	if !found[id1] || !found[id2] {
		t.Fatalf("snapshot missing handles: %v in %v", []HandleID{id1, id2}, found)
	}

	Delete(id1)
	found = map[HandleID]bool{}
	for _, h := range Snapshot() {
		found[h.ID] = true
	}
	if found[id1] || !found[id2] {
		t.Fatal("snapshot did not reflect concurrent delete")
	}
}

func TestSnapshotConcurrentAddDelete(t *testing.T) {
	ResetGateForTest()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				id := Add(NewHandle("p"))
				Delete(id)
			}
		}()
	}

	// Concurrent snapshots must never panic or race with Add/Delete.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = Snapshot()
			}
		}
	}()

	wg.Wait()
	close(stop)
}

func TestReconcileDirty(t *testing.T) {
	ResetGateForTest()

	// Clean Go handle, no native FH, no hook.
	h := NewHandle("a")
	if h.ReconcileDirty() {
		t.Fatal("fresh handle must reconcile as clean")
	}

	// Go dirty flag short-circuits without any native hook.
	h.Flags.Set(HandleFlagDirty)
	if !h.ReconcileDirty() {
		t.Fatal("Go-dirty handle must be dirty")
	}
	h.Flags.Clear(HandleFlagDirty)

	// Native FH present but no cgo owner registered: treated as clean.
	h.SetNativeFH(0xdeadbeef)
	if h.ReconcileDirty() {
		t.Fatal("native FH without registered check must be clean")
	}

	// Hook reports a native direct-IO write: flag is promoted onto the Go
	// handle so cache FlushFile paths cannot skip the upload.
	queried := 0
	RegisterNativeDirtyCheck(func(fh uintptr) bool {
		queried++
		return fh == 0xdeadbeef
	})
	if !h.ReconcileDirty() {
		t.Fatal("native dirty byte not reconciled")
	}
	if !h.Dirty() {
		t.Fatal("ReconcileDirty must promote the Go dirty flag")
	}
	if queried == 0 {
		t.Fatal("registered dirty check was not consulted")
	}

	// Once promoted, the hook need not be consulted again.
	queried = 0
	if !h.ReconcileDirty() || queried != 0 {
		t.Fatal("Go-dirty handle must short-circuit the native check")
	}

	// Clean native byte keeps a clean handle clean.
	h2 := NewHandle("b")
	h2.SetNativeFH(0x1234)
	if h2.ReconcileDirty() {
		t.Fatal("clean native byte must reconcile clean")
	}

	// Zero FH is never passed to the hook.
	h3 := NewHandle("c")
	if h3.ReconcileDirty() {
		t.Fatal("zero native FH must be clean")
	}

	ResetGateForTest()
}

func TestReconcileDirtyConcurrent(t *testing.T) {
	ResetGateForTest()
	RegisterNativeDirtyCheck(func(fh uintptr) bool { return fh != 0 })

	const writers = 8
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h := NewHandle("c")
			done := make(chan struct{})
			go func() {
				h.SetNativeFH(uintptr(0x1000 + i))
				close(done)
			}()
			for {
				select {
				case <-done:
					return
				default:
					_ = h.ReconcileDirty()
				}
			}
		}(w)
	}
	wg.Wait()
	ResetGateForTest()
}
