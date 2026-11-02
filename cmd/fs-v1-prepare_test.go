// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/jackblack369/minio/internal/lock"
)

func fsParallelTestSetup(t *testing.T) *FSObjects {
	t.Helper()
	oldParallel, oldDirect, oldReverse := globalFSParallelMeta, globalFSDirectIO, globalFSReverseWriteSize
	globalFSParallelMeta, globalFSDirectIO, globalFSReverseWriteSize = true, false, 0
	t.Cleanup(func() {
		globalFSParallelMeta, globalFSDirectIO, globalFSReverseWriteSize = oldParallel, oldDirect, oldReverse
	})
	fs := initFSObjects(t.TempDir(), t).(*FSObjects)
	t.Cleanup(func() { fs.Shutdown(GlobalContext) })
	if err := fs.MakeBucketWithLocation(GlobalContext, "bucket", BucketOptions{}); err != nil {
		t.Fatal(err)
	}
	return fs
}

func fsParallelMetaPath(fs *FSObjects, object string) string {
	return pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, "bucket", object, fs.metaJSONFile)
}

func fsParallelAssertEmptyTemp(t *testing.T, fs *FSObjects) {
	t.Helper()
	entries, err := os.ReadDir(pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		// FS initialization creates this empty directory for multipart appends.
		if entry.IsDir() && entry.Name() == bgAppendsDirName {
			children, err := os.ReadDir(pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, entry.Name()))
			if err == nil && len(children) == 0 {
				continue
			}
		}
		t.Fatalf("temporary file not cleaned: %s", entry.Name())
	}
}

func fsParallelAssertObject(t *testing.T, fs *FSObjects, object string, data []byte) {
	t.Helper()
	got, err := os.ReadFile(pathJoin(fs.fsPath, "bucket", object))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("object contents differ: got=%q err=%v", got, err)
	}
	oi, err := fs.GetObjectInfo(GlobalContext, "bucket", object, ObjectOptions{})
	sum := md5.Sum(data)
	if err != nil || oi.ETag != hex.EncodeToString(sum[:]) {
		t.Fatalf("object metadata differs: etag=%q err=%v", oi.ETag, err)
	}
}

func TestFSParallelMetadataSuccessAndFailures(t *testing.T) {
	readErr := errors.New("injected request read failure")
	for _, tc := range []struct {
		name        string
		reader      func() io.Reader
		size        int64
		md5         string
		metadataErr bool
		wantSuccess bool
	}{
		{name: "success", reader: func() io.Reader { return bytes.NewReader([]byte("new data")) }, size: 8, wantSuccess: true},
		{name: "empty", reader: func() io.Reader { return bytes.NewReader(nil) }, wantSuccess: true},
		{name: "metadata failure", reader: func() io.Reader { return bytes.NewReader([]byte("new data")) }, size: 8, metadataErr: true},
		{name: "data failure", reader: func() io.Reader { return iotest.ErrReader(readErr) }, size: 8},
		{name: "both fail metadata wins", reader: func() io.Reader { return iotest.ErrReader(readErr) }, size: 8, metadataErr: true},
		{name: "incomplete body", reader: func() io.Reader { return bytes.NewReader([]byte("short")) }, size: 8},
		{name: "checksum failure", reader: func() io.Reader { return bytes.NewReader([]byte("new data")) }, size: 8, md5: "00000000000000000000000000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := fsParallelTestSetup(t)
			metaPath := fsParallelMetaPath(fs, "object")
			var wantMetaErr error
			if tc.metadataErr {
				// An fs.json directory makes both opening and creating metadata fail.
				if err := os.MkdirAll(metaPath, 0o700); err != nil {
					t.Fatal(err)
				}
				prepared := fs.prepareObjectMetadata(metaPath)
				if prepared.file != nil || prepared.err == nil {
					t.Fatal("metadata failure injection did not fail")
				}
				wantMetaErr = toObjectErr(prepared.err, "bucket", "object")
			}
			_, err := fs.PutObject(GlobalContext, "bucket", "object", mustGetPutObjReader(t, tc.reader(), tc.size, tc.md5, ""), ObjectOptions{})
			if tc.wantSuccess {
				if err != nil {
					t.Fatal(err)
				}
				data, _ := io.ReadAll(tc.reader())
				fsParallelAssertObject(t, fs, "object", data)
			} else {
				if err == nil {
					t.Fatal("failed operation unexpectedly succeeded")
				}
				if wantMetaErr != nil && (!isSameType(err, wantMetaErr) || err.Error() != wantMetaErr.Error()) {
					t.Fatalf("error = %v, want metadata error %v", err, wantMetaErr)
				}
				if _, err := os.Stat(pathJoin(fs.fsPath, "bucket", "object")); !os.IsNotExist(err) {
					t.Fatalf("failed object was published: %v", err)
				}
				if !tc.metadataErr {
					if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
						t.Fatalf("fresh metadata was not cleaned: %v", err)
					}
				}
			}
			fsParallelAssertEmptyTemp(t, fs)
		})
	}
}

func TestFSParallelMetadataFailedOverwritePreservesObject(t *testing.T) {
	fs := fsParallelTestSetup(t)
	old := []byte("original object")
	if _, err := fs.PutObject(GlobalContext, "bucket", "object", mustGetPutObjReader(t, bytes.NewReader(old), int64(len(old)), "", ""), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	metaPath := fsParallelMetaPath(fs, "object")
	oldMeta, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		reader io.Reader
		md5    string
	}{
		{name: "read failure", reader: iotest.ErrReader(errors.New("read failed"))},
		{name: "short body", reader: bytes.NewReader([]byte("x"))},
		{name: "bad checksum", reader: bytes.NewReader([]byte("replaced")), md5: "00000000000000000000000000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := fs.PutObject(GlobalContext, "bucket", "object", mustGetPutObjReader(t, tc.reader, 8, tc.md5, ""), ObjectOptions{}); err == nil {
				t.Fatal("failed overwrite unexpectedly succeeded")
			}
			fsParallelAssertObject(t, fs, "object", old)
			got, err := os.ReadFile(metaPath)
			if err != nil || !bytes.Equal(got, oldMeta) {
				t.Fatalf("existing metadata changed: err=%v", err)
			}
			fsParallelAssertEmptyTemp(t, fs)
		})
	}
}

func TestFSParallelMetadataDisabledPreservesSerialPreparation(t *testing.T) {
	fs := fsParallelTestSetup(t)
	globalFSParallelMeta = false
	if err := os.MkdirAll(fsParallelMetaPath(fs, "object"), 0o700); err != nil {
		t.Fatal(err)
	}
	reader := &fsParallelObservedReader{reader: bytes.NewReader([]byte("new data")), started: make(chan struct{})}
	if _, err := fs.PutObject(GlobalContext, "bucket", "object", mustGetPutObjReader(t, reader, 8, "", ""), ObjectOptions{}); err == nil {
		t.Fatal("expected metadata preparation error")
	}
	select {
	case <-reader.started:
		t.Fatal("disabled mode consumed data after metadata preparation failed")
	default:
	}
	fsParallelAssertEmptyTemp(t, fs)
}

type fsParallelObservedReader struct {
	reader      io.Reader
	started     chan struct{}
	release     <-chan struct{}
	panicOnRead bool
	once        sync.Once
}

func (r *fsParallelObservedReader) Read(p []byte) (int, error) {
	r.once.Do(func() {
		close(r.started)
		if r.release != nil {
			<-r.release
		}
	})
	if r.panicOnRead {
		panic("injected reader panic")
	}
	return r.reader.Read(p)
}

func fsParallelWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for operation")
	}
}

func TestFSParallelMetadataJoinsBeforePublicationAndFailure(t *testing.T) {
	for _, mode := range []string{"success", "read error", "reader panic"} {
		t.Run(mode, func(t *testing.T) {
			fs := fsParallelTestSetup(t)
			old := []byte("old data")
			if _, err := fs.PutObject(GlobalContext, "bucket", "object", mustGetPutObjReader(t, bytes.NewReader(old), 8, "", ""), ObjectOptions{}); err != nil {
				t.Fatal(err)
			}
			metaPath := fsParallelMetaPath(fs, "object")
			held, err := lock.LockedOpenFile(metaPath, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			var unlock sync.Once
			releaseLock := func() { unlock.Do(func() { held.Close() }) }
			defer releaseLock()
			reader := &fsParallelObservedReader{reader: bytes.NewReader([]byte("new data")), started: make(chan struct{}), panicOnRead: mode == "reader panic"}
			if mode == "read error" {
				reader.reader = iotest.ErrReader(errors.New("read failed"))
			}
			input := mustGetPutObjReader(t, reader, 8, "", "")
			done := make(chan struct{})
			var putErr error
			var panicValue interface{}
			go func() {
				defer close(done)
				defer func() { panicValue = recover() }()
				_, putErr = fs.PutObject(GlobalContext, "bucket", "object", input, ObjectOptions{})
			}()
			// Data consumption overlaps a metadata flock that cannot complete yet.
			fsParallelWait(t, reader.started)
			select {
			case <-done:
				t.Fatal("request returned before joining metadata preparation")
			case <-time.After(30 * time.Millisecond):
			}
			got, err := os.ReadFile(filepath.Join(fs.fsPath, "bucket", "object"))
			if err != nil || !bytes.Equal(got, old) {
				t.Fatal("object published before acquiring metadata lock")
			}
			releaseLock()
			fsParallelWait(t, done)
			if mode == "success" {
				if putErr != nil || panicValue != nil {
					t.Fatalf("put failed: %v, panic=%v", putErr, panicValue)
				}
				fsParallelAssertObject(t, fs, "object", []byte("new data"))
			} else {
				if mode == "read error" && putErr == nil || mode == "reader panic" && panicValue == nil {
					t.Fatalf("expected failure: err=%v panic=%v", putErr, panicValue)
				}
				fsParallelAssertObject(t, fs, "object", old)
			}
			probe, err := lock.TryLockedOpenFile(metaPath, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatalf("metadata lock leaked: %v", err)
			}
			probe.Close()
			fsParallelAssertEmptyTemp(t, fs)
		})
	}
}

func TestFSParallelMetadataSameKeySerializes(t *testing.T) {
	fs := fsParallelTestSetup(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	first := &fsParallelObservedReader{reader: bytes.NewReader([]byte("first")), started: make(chan struct{}), release: release}
	second := &fsParallelObservedReader{reader: bytes.NewReader([]byte("second")), started: make(chan struct{})}
	firstInput := mustGetPutObjReader(t, first, 5, "", "")
	secondInput := mustGetPutObjReader(t, second, 6, "", "")
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	var firstErr, secondErr error
	go func() {
		defer close(firstDone)
		_, firstErr = fs.PutObject(GlobalContext, "bucket", "object", firstInput, ObjectOptions{})
	}()
	fsParallelWait(t, first.started)
	go func() {
		defer close(secondDone)
		_, secondErr = fs.PutObject(GlobalContext, "bucket", "object", secondInput, ObjectOptions{})
	}()
	select {
	case <-second.started:
		t.Fatal("second PUT read its body while first PUT held the object lock")
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	fsParallelWait(t, firstDone)
	fsParallelWait(t, secondDone)
	if firstErr != nil || secondErr != nil {
		t.Fatalf("PUT errors: first=%v second=%v", firstErr, secondErr)
	}
	fsParallelAssertObject(t, fs, "object", []byte("second"))
	fsParallelAssertEmptyTemp(t, fs)
}
