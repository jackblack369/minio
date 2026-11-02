// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdfs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/jackblack369/minio/internal/lock"
)

const fsHashLayoutDir = ".minio-hash-v1"
const fsHashLayoutVersion = "hash-v1"

// Hash mode is an experimental, explicitly versioned new-root format. Never
// infer its presence from object names or migrate an existing legacy tree.
func initFSHashLayout(root string) (bool, error) {
	mode := os.Getenv("MINIO_FS_HASH_LAYOUT")
	if mode != "" && mode != "off" && mode != "on" {
		return false, fmt.Errorf("MINIO_FS_HASH_LAYOUT must be on or off")
	}
	enabled := mode == "on"
	marker := filepath.Join(root, minioMetaBucket, "hash-layout.json")
	data, err := os.ReadFile(marker)
	if err == nil {
		var version struct {
			Version string `json:"version"`
		}
		if json.Unmarshal(data, &version) != nil || version.Version != fsHashLayoutVersion {
			return false, fmt.Errorf("invalid hash layout marker")
		}
		if !enabled {
			return false, fmt.Errorf("hash layout root requires MINIO_FS_HASH_LAYOUT=on")
		}
		return true, nil
	}
	if !os.IsNotExist(err) {
		return false, err
	}
	if !enabled {
		return false, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, err
	}
	if len(entries) != 0 {
		return false, fmt.Errorf("hash layout requires a new empty root; legacy conversion is not supported")
	}
	if err := os.MkdirAll(filepath.Dir(marker), 0o777); err != nil {
		return false, err
	}
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_SYNC, 0o666)
	if err != nil {
		return false, err
	}
	_, writeErr := io.WriteString(f, "{\"version\":\"hash-v1\"}\n")
	closeErr := f.Close()
	if writeErr != nil {
		return false, writeErr
	}
	return true, closeErr
}

// An unsupported FS version also makes older Gateway binaries reject this
// root instead of interpreting the physical hash directories as logical keys.
func initFormatFSHash(root string) (*lock.RLockedFile, error) {
	name := filepath.Join(root, minioMetaBucket, formatConfigFile)
	if _, err := os.Stat(name); os.IsNotExist(err) {
		wlk, err := lock.LockedOpenFile(name, os.O_RDWR|os.O_CREATE, 0o666)
		if err != nil {
			return nil, err
		}
		info, statErr := wlk.Stat()
		if statErr == nil && info.Size() == 0 {
			format := newFormatFSV1()
			format.FS.Version = fsHashLayoutVersion
			_, statErr = jsonSaveCount(wlk, format)
		}
		closeErr := wlk.Close()
		if statErr != nil {
			return nil, statErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	rlk, err := lock.RLockedOpenFile(name)
	if err != nil {
		return nil, err
	}
	backend, err := formatMetaGetFormatBackendFS(rlk)
	if err != nil || backend != formatBackendFS {
		rlk.Close()
		return nil, fmt.Errorf("hash root format.json must use FS backend")
	}
	version, err := formatFSGetVersion(rlk)
	if err != nil || version != fsHashLayoutVersion {
		rlk.Close()
		return nil, fmt.Errorf("hash root format.json must use FS version %s", fsHashLayoutVersion)
	}
	return rlk, nil
}

func (fs *FSObjects) isHashBucket(bucket string) bool {
	return fs.hashLayout && bucket != minioMetaBucket
}

func (fs *FSObjects) hashObjectDir(bucket, object string) string {
	hash := getSHA256Hash([]byte(object))
	return filepath.Join(fs.fsPath, bucket, fsHashLayoutDir, hash[:2], hash)
}

func (fs *FSObjects) objectPath(bucket, object string) string {
	if fs.isHashBucket(bucket) {
		return filepath.Join(fs.hashObjectDir(bucket, object), "data")
	}
	return pathJoin(fs.fsPath, bucket, object)
}

func (fs *FSObjects) objectMetaPath(bucket, object string) string {
	if fs.isHashBucket(bucket) {
		return filepath.Join(fs.hashObjectDir(bucket, object), fs.metaJSONFile)
	}
	return pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
}

func (fs *FSObjects) objectTempPath(bucket, object, transaction string) string {
	if fs.isHashBucket(bucket) {
		return filepath.Join(fs.hashObjectDir(bucket, object), "data.tmp."+fs.fsUUID+"."+transaction)
	}
	return pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, transaction)
}

func (fs *FSObjects) dataDeleteRoot(bucket, object string) string {
	if fs.isHashBucket(bucket) {
		return fs.hashObjectDir(bucket, object)
	}
	return pathJoin(fs.fsPath, bucket)
}

func (fs *FSObjects) metadataDeleteRoot(bucket, object string) string {
	if fs.isHashBucket(bucket) {
		return filepath.Join(fs.fsPath, bucket, fsHashLayoutDir)
	}
	return pathJoin(fs.fsPath, minioMetaBucket)
}

func (fs *FSObjects) validateHashMetadata(bucket, object string, meta fsMetaV1) error {
	if !fs.isHashBucket(bucket) {
		return nil
	}
	if meta.HashKey == object {
		return nil
	}
	if meta.HashKey == "" {
		if _, err := os.Stat(fs.objectPath(bucket, object)); os.IsNotExist(err) {
			return errFileNotFound
		}
	}
	return fmt.Errorf("%w: hashed object identity does not match logical key", errCorruptedFormat)
}

func (fs *FSObjects) prepareHashMetadata(bucket, object, name string) fsMetadataPreparation {
	prepared := fs.prepareObjectMetadata(name)
	if prepared.err != nil || !fs.isHashBucket(bucket) {
		return prepared
	}
	info, err := prepared.file.Stat()
	if err != nil {
		prepared.err = err
		return prepared
	}
	if info.Size() != 0 {
		prepared.fresh = false
		var existing fsMetaV1
		_, err = existing.ReadFrom(GlobalContext, prepared.file)
		if err == nil {
			err = fs.validateHashMetadata(bucket, object, existing)
		}
		prepared.err = err
	} else if _, statErr := os.Stat(fs.objectPath(bucket, object)); statErr == nil {
		prepared.fresh = false
		prepared.err = fmt.Errorf("%w: published hash data has no identity metadata", errCorruptedFormat)
	}
	return prepared
}

func (fs *FSObjects) prepareMultipartMetadata(bucket, object, name string) fsMetadataPreparation {
	if fs.isHashBucket(bucket) {
		return fs.prepareHashMetadata(bucket, object, name)
	}
	f, err := fs.rwPool.Write(name)
	if err == nil {
		return fsMetadataPreparation{file: f}
	}
	if !errors.Is(err, errFileNotFound) {
		return fsMetadataPreparation{err: err}
	}
	f, err = fs.rwPool.Create(name)
	return fsMetadataPreparation{file: f, fresh: err == nil, err: err}
}

func (fs *FSObjects) hashBucketMutex(bucket string) *sync.RWMutex {
	v, _ := fs.hashBucketLocks.LoadOrStore(bucket, &sync.RWMutex{})
	return v.(*sync.RWMutex)
}

func (fs *FSObjects) hashBucketReadLock(bucket string) func() {
	if !fs.isHashBucket(bucket) {
		return func() {}
	}
	m := fs.hashBucketMutex(bucket)
	m.RLock()
	return m.RUnlock
}

// Metadata and temporary files are internal; only a data file with matching,
// valid key metadata is a committed object. LIST is intentionally scan+sort
// for this prototype, not a persistent index or a constant-time operation.
func (fs *FSObjects) hashObjectInfos(ctx context.Context, bucket string) ([]ObjectInfo, error) {
	root := filepath.Join(fs.fsPath, bucket, fsHashLayoutDir)
	infos := make([]ObjectInfo, 0)
	err := filepath.WalkDir(root, func(name string, entry stdfs.DirEntry, walkErr error) error {
		if os.IsNotExist(walkErr) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != fs.metaJSONFile {
			return nil
		}
		dataPath := filepath.Join(filepath.Dir(name), "data")
		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}
		rlk, err := fs.rwPool.Open(name)
		if err != nil {
			if err == errFileNotFound {
				return nil
			}
			return err
		}
		var meta fsMetaV1
		_, err = meta.ReadFrom(ctx, rlk.LockedFile)
		fs.rwPool.Close(name)
		if err != nil {
			return err
		}
		if meta.HashKey == "" || filepath.Clean(filepath.Dir(name)) != fs.hashObjectDir(bucket, meta.HashKey) {
			return fmt.Errorf("%w: invalid hash container identity", errCorruptedFormat)
		}
		info, err := fs.getObjectInfoWithLock(ctx, bucket, meta.HashKey)
		if err == errFileNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		infos = append(infos, info)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

func (fs *FSObjects) listHashObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	var result ListObjectsInfo
	if err := checkListObjsArgs(ctx, bucket, prefix, marker, fs); err != nil {
		return result, err
	}
	if maxKeys == 0 {
		return result, nil
	}
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}
	objects, err := fs.hashObjectInfos(ctx, bucket)
	if err != nil {
		return result, toObjectErr(err, bucket)
	}
	type listed struct {
		name   string
		prefix bool
		object ObjectInfo
	}
	entries := make(map[string]listed)
	for _, object := range objects {
		if !strings.HasPrefix(object.Name, prefix) {
			continue
		}
		if delimiter != "" {
			if at := strings.Index(strings.TrimPrefix(object.Name, prefix), delimiter); at >= 0 {
				name := object.Name[:len(prefix)+at+len(delimiter)]
				if name > marker {
					entries[name] = listed{name: name, prefix: true}
				}
				continue
			}
		}
		if object.Name > marker {
			entries[object.Name] = listed{name: object.Name, object: object}
		}
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	result.IsTruncated = len(names) > maxKeys
	if result.IsTruncated {
		names = names[:maxKeys]
	}
	for _, name := range names {
		entry := entries[name]
		if entry.prefix {
			result.Prefixes = append(result.Prefixes, name)
		} else {
			result.Objects = append(result.Objects, entry.object)
		}
	}
	if result.IsTruncated && len(names) != 0 {
		result.NextMarker = names[len(names)-1]
	}
	return result, nil
}

func (fs *FSObjects) walkHashObjects(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
	defer close(results)
	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	objects, err := fs.hashObjectInfos(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}
	for _, object := range objects {
		if !strings.HasPrefix(object.Name, prefix) {
			continue
		}
		select {
		case results <- object:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (fs *FSObjects) deleteHashBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	m := fs.hashBucketMutex(bucket)
	m.Lock()
	defer m.Unlock()
	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	if !opts.Force {
		objects, err := fs.hashObjectInfos(ctx, bucket)
		if err != nil {
			return toObjectErr(err, bucket)
		}
		if len(objects) != 0 {
			return BucketNotEmpty{Bucket: bucket}
		}
	}
	// Unlike the legacy flat tree an empty bucket retains empty shard and
	// object containers. Public writers hold the corresponding bucket RLock.
	if err := fsRemoveAll(ctx, pathJoin(fs.fsPath, bucket)); err != nil {
		return toObjectErr(err, bucket)
	}
	if err := fsRemoveAll(ctx, pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket)); err != nil {
		return toObjectErr(err, bucket)
	}
	deleteBucketMetadata(ctx, fs, bucket)
	NSUpdated(bucket, slashSeparator)
	return nil
}

// Existing multipart staging uses a separate internal directory. Copy the
// fully assembled file into the destination container before publication so
// its final atomic rename is also same-directory. This is not the ordinary
// 4MiB PUT hot path and deliberately retains the extra close/flush.
func (fs *FSObjects) hashMultipartLocalFile(ctx context.Context, bucket, object, assembled string) (string, error) {
	name := fs.objectTempPath(bucket, object, mustGetUUID())
	r, size, err := fsOpenFile(ctx, assembled, 0)
	if err != nil {
		return "", err
	}
	defer r.Close()
	_, err = fsCreateFile(ctx, name, r, size)
	if err != nil {
		fsRemoveFile(ctx, name)
		return "", err
	}
	if err := fsRemoveFile(ctx, assembled); err != nil {
		fsRemoveFile(ctx, name)
		return "", err
	}
	return name, nil
}
