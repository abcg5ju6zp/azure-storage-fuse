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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
)

// Compile-time assertion that BlockBlob drives key rotation remotely.
var _ cpkRemoteBackend = &BlockBlob{}
var _ cpkKeyProber = &BlockBlob{}

// quoteETag returns the quoted ETag value required by conditional headers.
func quoteETag(etag string) azcore.ETag {
	if etag == "" {
		return azcore.ETag("")
	}
	if !strings.HasPrefix(etag, "\"") {
		etag = "\"" + etag + "\""
	}
	if !strings.HasSuffix(etag, "\"") {
		etag += "\""
	}
	return azcore.ETag(etag)
}

// remoteIdentity returns the authoritative (customer key sha256, etag) of an
// object from the flat list endpoint, which reports the encryption identifier
// without presenting any key. Empty sha means the object is not CPK encrypted.
func (bb *BlockBlob) remoteIdentity(name string) (string, string, error) {
	fullName := filepath.Join(bb.Config.prefixPath, name)
	maxResults := int32(1)
	pager := bb.Container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		MaxResults: &maxResults,
		Prefix:     &fullName,
	})

	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			log.Err("BlockBlob::remoteIdentity : failed to list %s [%s]", name, err.Error())
			return "", "", err
		}
		for _, item := range page.Segment.BlobItems {
			if item == nil || item.Name == nil || *item.Name != fullName {
				continue
			}
			sha := ""
			if item.Properties != nil && item.Properties.CustomerProvidedKeySHA256 != nil {
				sha = *item.Properties.CustomerProvidedKeySHA256
			}
			etag := ""
			if item.Properties != nil {
				etag = sanitizeEtag(item.Properties.ETag)
			}
			return sha, etag, nil
		}
	}
	return "", "", syscall.ENOENT
}

// enumerateCPKObjects lists every object under the mounted prefix with its
// remote encryption identifier and current version.
func (bb *BlockBlob) enumerateCPKObjects() ([]cpkRemoteObject, error) {
	listPrefix := bb.Config.prefixPath
	if listPrefix != "" && !strings.HasSuffix(listPrefix, "/") {
		listPrefix += "/"
	}

	out := make([]cpkRemoteObject, 0)
	pager := bb.Container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &listPrefix,
		Include: container.ListBlobsInclude{
			Metadata: true,
		},
	})

	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			log.Err("BlockBlob::enumerateCPKObjects : list failed [%s]", err.Error())
			return nil, err
		}
		for _, item := range page.Segment.BlobItems {
			if item == nil || item.Name == nil || item.Properties == nil {
				continue
			}
			name := removePrefixPath(bb.Config.prefixPath, *item.Name)
			sha := ""
			if item.Properties.CustomerProvidedKeySHA256 != nil {
				sha = *item.Properties.CustomerProvidedKeySHA256
			}
			isDir := bb.Config.isHNS && hasFolderMetadata(item.Metadata)
			out = append(out, cpkRemoteObject{
				name: name,
				sha:  sha,
				etag: sanitizeEtag(item.Properties.ETag),
				isDir: isDir,
			})
		}
	}
	return out, nil
}

func hasFolderMetadata(metadata map[string]*string) bool {
	for k, v := range metadata {
		if strings.EqualFold(k, folderKey) && v != nil && *v == "true" {
			return true
		}
	}
	return false
}

// probeCPKKey validates a candidate key with a tiny encrypted upload followed
// by best-effort cleanup. A rejected key never becomes active.
func (bb *BlockBlob) probeCPKKey(m cpkKeyMaterial) error {
	probeName := filepath.Join(bb.Config.prefixPath,
		fmt.Sprintf(".blobfuse2-cpk-probe-%d", cpkRuntimeSeq.Add(1)))
	client := bb.Container.NewBlockBlobClient(probeName)

	ctx, cancel := context.WithTimeout(context.Background(), max_context_timeout*time.Minute)
	defer cancel()

	_, err := client.UploadBuffer(ctx, []byte{'b'}, &blockblob.UploadBufferOptions{
		CPKInfo: cpkBlobInfo(m.key, m.sha),
	})
	if err != nil {
		log.Err("BlockBlob::probeCPKKey : new key rejected by service [%s]", err.Error())
		return err
	}
	_, _ = client.Delete(context.Background(), nil)
	return nil
}

// migrateObject republishes one object under newSHA:
//  1. fetch metadata/properties presenting the object's current key,
//  2. download the content presenting the object's current key,
//  3. publish atomically with the new key only if the remote version still
//     equals srcETag (If-Match), preserving metadata and HTTP settings.
//
// The caller is responsible for the post-publish remote confirmation; an
// ambiguous transport error is returned as a regular error but the migrator
// classifies it via isDefinitiveCPKPublishError before trusting the result.
func (bb *BlockBlob) migrateObject(obj cpkRemoteObject, newSHA, srcETag string) (string, error) {
	if srcETag == "" {
		return "", errors.New("migrateObject requires the source object version (etag)")
	}
	oldCPK, err := bb.cpkInfoForSHA(obj.sha)
	if err != nil {
		return "", err
	}
	newCPK, err := bb.cpkInfoForSHA(newSHA)
	if err != nil {
		return "", err
	}

	blobClient := bb.Container.NewBlobClient(filepath.Join(bb.Config.prefixPath, obj.name))
	client := bb.Container.NewBlockBlobClient(filepath.Join(bb.Config.prefixPath, obj.name))
	ctx, cancel := context.WithTimeout(context.Background(), max_context_timeout*time.Minute)
	defer cancel()

	props, err := blobClient.GetProperties(ctx, &blob.GetPropertiesOptions{
		CPKInfo: oldCPK,
	})
	if err != nil {
		return "", err
	}
	if got := sanitizeEtag(props.ETag); got != "" && got != srcETag {
		// Source version moved between enumeration and migration: fail the
		// conditional publish explicitly instead of overwriting new content.
		return "", &azcore.ResponseError{ErrorCode: string(bloberror.ConditionNotMet), StatusCode: 412}
	}

	dl, err := blobClient.DownloadStream(ctx, &blob.DownloadStreamOptions{
		CPKInfo: oldCPK,
	})
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(dl.NewRetryReader(ctx, nil))
	if err != nil {
		return "", err
	}

	contentType := getContentType(obj.name)
	if props.ContentType != nil && *props.ContentType != "" {
		contentType = *props.ContentType
	}

	ifMatch := quoteETag(srcETag)
	resp, err := client.UploadStream(ctx, streaming.NopCloser(bytes.NewReader(data)), &blockblob.UploadStreamOptions{
		BlockSize:   bb.Config.blockSize,
		Concurrency: int(bb.Config.maxConcurrency),
		Metadata:    props.Metadata,
		AccessTier:  bb.Config.defaultTier,
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType:        to.Ptr(contentType),
			BlobContentMD5:         props.ContentMD5,
			BlobContentEncoding:    props.ContentEncoding,
			BlobContentLanguage:    props.ContentLanguage,
			BlobContentDisposition: props.ContentDisposition,
			BlobCacheControl:       props.CacheControl,
		},
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: &ifMatch},
		},
		CPKInfo: newCPK,
	})
	if err != nil {
		return "", err
	}
	return sanitizeEtag(resp.ETag), nil
}
