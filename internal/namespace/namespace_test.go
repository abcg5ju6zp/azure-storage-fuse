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

package namespace

import (
	"testing"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/internal/handlemap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type NamespaceTestSuite struct {
	suite.Suite
	assert *assert.Assertions
	reg    *Registry
}

func (suite *NamespaceTestSuite) SetupTest() {
	suite.assert = assert.New(suite.T())
	suite.reg = NewRegistry()
}

func newTestHandle(path string) *handlemap.Handle {
	return handlemap.NewHandle(path)
}

// Bind on the same name shares one object ID; a second name gets its own.
func (suite *NamespaceTestSuite) TestBindSharesIdentityPerName() {
	h1 := newTestHandle("a")
	h2 := newTestHandle("a")
	h3 := newTestHandle("b")

	id1, gen1 := suite.reg.Bind(h1, "a", Identity{})
	id2, gen2 := suite.reg.Bind(h2, "a", Identity{})
	id3, _ := suite.reg.Bind(h3, "b", Identity{})

	suite.assert.Equal(id1, id2)
	suite.assert.NotEqual(id1, id3)
	suite.assert.Equal(gen1, gen2)
	suite.assert.Greater(gen1, uint64(0))
	suite.assert.Equal(h1.Path, "a")
}

// A committed rename moves the object ID to the destination, repaths all
// handles, tombstones the source and leaves a single writable name.
func (suite *NamespaceTestSuite) TestCommitRenameRepathsHandles() {
	h := newTestHandle("src")
	id, srcGen := suite.reg.Bind(h, "src", Identity{ETag: "e1"})

	txn := suite.reg.PrepareRename("src", "dst", Identity{ETag: "e1"}, Identity{}, true, false)
	suite.assert.Equal(id, txn.SrcID)
	suite.assert.Equal(srcGen, txn.SrcGen)

	plan := suite.reg.CommitRename(txn)

	// Source name is now a negative record with a new generation.
	suite.assert.False(suite.reg.Present("src"))
	suite.assert.True(suite.reg.CheckGen("src", plan.SrcGen))
	suite.assert.False(suite.reg.CheckGen("src", srcGen))
	suite.assert.True(suite.reg.Present("dst"))

	// The handle now resolves at dst.
	suite.assert.Equal("dst", h.Path)
	suite.assert.Equal(plan.DstGen, h.NameGen)

	b, ok := suite.reg.Resolve(id)
	suite.assert.True(ok)
	suite.assert.Equal("dst", b.Path)
	suite.assert.Equal(StateAlive, b.State)
	suite.assert.True(b.WriteBackAllowed())
	suite.assert.Equal("e1", b.Self.ETag)

	// Only one writable name: the moved handle is the single one.
	suite.assert.Equal(uint32(1), plan.MovedCount)
	suite.assert.Empty(plan.ReplacedHandles)
}

// Renaming over a name with open handles: the overwritten destination object
// becomes deleted for its handles while source handles move to dst, so two
// handles never write to two names.
func (suite *NamespaceTestSuite) TestRenameOverOpenDestination() {
	srcH := newTestHandle("src")
	dstH := newTestHandle("dst")
	srcID, _ := suite.reg.Bind(srcH, "src", Identity{ETag: "s"})
	dstID, _ := suite.reg.Bind(dstH, "dst", Identity{ETag: "d"})

	txn := suite.reg.PrepareRename("src", "dst", Identity{ETag: "s"}, Identity{ETag: "d"}, true, true)
	plan := suite.reg.CommitRename(txn)

	suite.assert.Len(plan.MovedHandles, 1)
	suite.assert.Len(plan.ReplacedHandles, 1)

	// Both handles carry the destination path now.
	suite.assert.Equal("dst", srcH.Path)
	suite.assert.Equal("dst", dstH.Path)

	srcBinding, ok := suite.reg.Resolve(srcID)
	suite.assert.True(ok)
	suite.assert.Equal(StateAlive, srcBinding.State)

	// The replaced destination object must never write back.
	dstBinding, ok := suite.reg.Resolve(dstID)
	suite.assert.True(ok)
	suite.assert.Equal(StateDeleted, dstBinding.State)
	suite.assert.False(dstBinding.WriteBackAllowed())
}

// Delete commit: late handle write-back is fenced, name is a negative record.
func (suite *NamespaceTestSuite) TestCommitDeleteFencesWriteBack() {
	h := newTestHandle("f")
	id, gen := suite.reg.Bind(h, "f", Identity{ETag: "e"})

	txn := suite.reg.PrepareDelete("f", Identity{ETag: "e"}, true)
	newGen, conflict := suite.reg.CommitDelete(txn)
	suite.assert.False(conflict)
	suite.assert.Greater(newGen, gen)
	suite.assert.False(suite.reg.Present("f"))

	b, ok := suite.reg.Resolve(id)
	suite.assert.True(ok)
	suite.assert.Equal(StateDeleted, b.State)
	suite.assert.False(b.WriteBackAllowed())
}

// Stale cache cleanup captured at the old generation must not touch a name
// recreated by a newer operation.
func (suite *NamespaceTestSuite) TestGenerationFenceProtectsRecreatedName() {
	h := newTestHandle("f")
	id, gen := suite.reg.Bind(h, "f", Identity{})

	txn := suite.reg.PrepareDelete("f", Identity{}, true)
	delGen, _ := suite.reg.CommitDelete(txn)

	// Old guard from before the delete is stale.
	suite.assert.False(suite.reg.CheckGen("f", gen))
	// Current tombstone generation is valid.
	suite.assert.True(suite.reg.CheckGen("f", delGen))

	// A new object is created at the same name.
	newGen := suite.reg.TouchCreate("f")
	h2 := newTestHandle("f")
	newID, bindGen := suite.reg.Bind(h2, "f", Identity{})
	suite.assert.NotEqual(id, newID)
	suite.assert.True(suite.reg.Present("f"))
	suite.assert.False(suite.reg.CheckGen("f", delGen), "delete-era cleanup must not reach the recreated object")
	suite.assert.True(suite.reg.CheckGen("f", newGen))
	suite.assert.Equal(bindGen, newGen)

	// Old handle remains fenced even though the name is occupied again.
	b, ok := suite.reg.Resolve(id)
	suite.assert.True(ok)
	suite.assert.Equal(StateDeleted, b.State)
}

// A name that advanced between prepare and commit is never clobbered by the
// committing rename/delete.
func (suite *NamespaceTestSuite) TestCommitRenameConflictPreservesName() {
	h := newTestHandle("src")
	id, _ := suite.reg.Bind(h, "src", Identity{})

	txn := suite.reg.PrepareRename("src", "dst", Identity{}, Identity{}, true, false)
	// Another create at src advances the generation while the remote call is
	// in flight.
	suite.reg.TouchCreate("src")

	plan := suite.reg.CommitRename(txn)
	suite.assert.True(plan.SrcConflict)
	suite.assert.True(suite.reg.Present("src"), "recreated source must remain present")
	// The moved object itself still resolves at dst for its handles.
	b, ok := suite.reg.Resolve(id)
	suite.assert.True(ok)
	suite.assert.Equal("dst", b.Path)
}

// Detach after release drops the handle; with no handles left the object
// binding is freed and the name record keeps its generation.
func (suite *NamespaceTestSuite) TestDetach() {
	h := newTestHandle("a")
	id, gen := suite.reg.Bind(h, "a", Identity{})
	suite.reg.Detach(h)

	_, ok := suite.reg.Resolve(id)
	suite.assert.False(ok)
	suite.assert.True(suite.reg.CheckGen("a", gen))
}

// Decision table for uncertain rename responses.
func (suite *NamespaceTestSuite) TestDecideRename() {
	src := Identity{ETag: "e1"}
	txn := &Txn{Kind: TxnRename, Src: "a", Dst: "b", SrcIdentity: src}

	suite.assert.Equal(RenameCommitted, DecideRename(txn, Probe{SrcExists: false, DstExists: true, Dst: Identity{ETag: "e2"}}))
	suite.assert.Equal(RenameComplete, DecideRename(txn, Probe{SrcExists: true, DstExists: true, Src: src, Dst: Identity{ETag: "e2"}}))
	suite.assert.Equal(RenameSrcRecreated, DecideRename(txn, Probe{SrcExists: true, DstExists: true, Src: Identity{ETag: "new"}, Dst: Identity{ETag: "e2"}}))
	suite.assert.Equal(RenameRolledBack, DecideRename(txn, Probe{SrcExists: true, DstExists: false, Src: src}))
	suite.assert.Equal(RenameInconclusive, DecideRename(txn, Probe{SrcExists: false, DstExists: false}))
	suite.assert.Equal(RenameInconclusive, DecideRename(txn, Probe{SrcExists: true, DstExists: false, Src: Identity{ETag: "other"}}))
}

// Decision table for uncertain delete responses.
func (suite *NamespaceTestSuite) TestDecideDelete() {
	src := Identity{ETag: "e1"}
	txn := &Txn{Kind: TxnDelete, Src: "a", SrcIdentity: src}

	suite.assert.Equal(DeleteCommitted, DecideDelete(txn, false, Identity{}))
	suite.assert.Equal(DeleteRolledBack, DecideDelete(txn, true, src))
	suite.assert.Equal(DeleteRecreated, DecideDelete(txn, true, Identity{ETag: "e2"}))
}

func (suite *NamespaceTestSuite) TestSameIdentity() {
	mt := time.Unix(1000, 0)
	suite.assert.True(SameIdentity(Identity{ETag: "x"}, Identity{ETag: "x"}))
	suite.assert.False(SameIdentity(Identity{ETag: "x"}, Identity{ETag: "y"}))
	suite.assert.True(SameIdentity(Identity{Mtime: mt, Size: 10}, Identity{Mtime: mt, Size: 10}))
	suite.assert.False(SameIdentity(Identity{Mtime: mt, Size: 10}, Identity{Mtime: mt, Size: 11}))
	suite.assert.False(SameIdentity(Identity{}, Identity{}))
}

// Opening a name from a negative record starts a fresh generation and a new
// object ID; the stale tombstone cannot shadow it.
func (suite *NamespaceTestSuite) TestBindAfterDeleteBumpsGeneration() {
	gen0 := suite.reg.Generation("f")
	suite.assert.Equal(uint64(0), gen0)

	txn := suite.reg.PrepareDelete("f", Identity{}, true)
	_, _ = suite.reg.CommitDelete(txn)
	tombGen := suite.reg.Generation("f")

	h := newTestHandle("f")
	id, openGen := suite.reg.Bind(h, "f", Identity{})
	suite.assert.True(suite.reg.Present("f"))
	suite.assert.Greater(openGen, tombGen)
	suite.assert.NotEqual(handlemap.InvalidObjectID, id)
}

func TestNamespaceTestSuite(t *testing.T) {
	suite.Run(t, new(NamespaceTestSuite))
}
