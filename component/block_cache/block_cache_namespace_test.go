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

package block_cache

import (
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-fuse/v2/internal"
)

// Blocks staged through a block-cache handle while it stays open must commit to
// the destination name after a rename; the source name must not reappear.
func (suite *blockCacheTestSuite) TestRenameOpenHandleCommitsToDestination() {
	tobj, err := setupPipeline("")
	suite.assert.NoError(err)
	defer tobj.cleanupPipeline()

	src, dst := "bc_ns_src", "bc_ns_dst"
	h, err := tobj.blockCache.CreateFile(internal.CreateFileOptions{Name: src, Mode: 0777})
	suite.assert.NoError(err)

	n, err := tobj.blockCache.WriteFile(&internal.WriteFileOptions{Handle: h, Offset: 0, Data: []byte("Hello")})
	suite.assert.NoError(err)
	suite.assert.Equal(5, n)
	suite.assert.True(h.Dirty())

	err = tobj.blockCache.RenameFile(internal.RenameFileOptions{Src: src, Dst: dst})
	suite.assert.NoError(err)

	// The handle follows the object to the destination.
	suite.assert.Equal(dst, h.Path)
	suite.assert.NoFileExists(filepath.Join(tobj.fake_storage_path, src))

	// Late block commit lands on the destination only.
	err = tobj.blockCache.FlushFile(internal.FlushFileOptions{Handle: h, CloseInProgress: true})
	suite.assert.NoError(err)
	suite.assert.False(h.Dirty())

	suite.assert.NoFileExists(filepath.Join(tobj.fake_storage_path, src))
	d, rerr := os.ReadFile(filepath.Join(tobj.fake_storage_path, dst))
	suite.assert.NoError(rerr)
	suite.assert.Equal([]byte("Hello"), d)

	suite.assert.NoError(tobj.blockCache.ReleaseFile(internal.ReleaseFileOptions{Handle: h}))
	suite.assert.NoFileExists(filepath.Join(tobj.fake_storage_path, src))
}

// A delete while a dirty block-cache handle is open must fence the commit: the
// later flush/release cannot recreate the object.
func (suite *blockCacheTestSuite) TestDeleteOpenDirtyHandleFenced() {
	tobj, err := setupPipeline("")
	suite.assert.NoError(err)
	defer tobj.cleanupPipeline()

	name := "bc_ns_del"
	h, err := tobj.blockCache.CreateFile(internal.CreateFileOptions{Name: name, Mode: 0777})
	suite.assert.NoError(err)
	_, err = tobj.blockCache.WriteFile(&internal.WriteFileOptions{Handle: h, Offset: 0, Data: []byte("World")})
	suite.assert.NoError(err)

	err = tobj.blockCache.DeleteFile(internal.DeleteFileOptions{Name: name})
	suite.assert.NoError(err)
	suite.assert.NoFileExists(filepath.Join(tobj.fake_storage_path, name))

	// Dirty blocks are dropped, never committed back to the deleted name.
	err = tobj.blockCache.FlushFile(internal.FlushFileOptions{Handle: h, CloseInProgress: true})
	suite.assert.NoError(err)
	suite.assert.False(h.Dirty())
	suite.assert.NoFileExists(filepath.Join(tobj.fake_storage_path, name))

	suite.assert.NoError(tobj.blockCache.ReleaseFile(internal.ReleaseFileOptions{Handle: h}))
	suite.assert.NoFileExists(filepath.Join(tobj.fake_storage_path, name))
}
