package cmd

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
)

func fsHashTestSetup(t *testing.T) *FSObjects {
	t.Helper()
	t.Setenv("MINIO_FS_HASH_LAYOUT", "on")
	oldMeta, oldDirect, oldReverse, oldChecksum := globalFSParallelMeta, globalFSDirectIO, globalFSReverseWriteSize, globalFSParallelChecksum
	globalFSParallelMeta, globalFSDirectIO, globalFSReverseWriteSize, globalFSParallelChecksum = true, false, 0, false
	t.Cleanup(func() {
		globalFSParallelMeta, globalFSDirectIO, globalFSReverseWriteSize, globalFSParallelChecksum = oldMeta, oldDirect, oldReverse, oldChecksum
	})
	fs := initFSObjects(t.TempDir(), t).(*FSObjects)
	t.Cleanup(func() { _ = fs.Shutdown(GlobalContext) })
	if !fs.hashLayout {
		t.Fatal("hash mode was not enabled")
	}
	if err := fs.MakeBucketWithLocation(GlobalContext, "bucket", BucketOptions{}); err != nil {
		t.Fatal(err)
	}
	return fs
}

func fsHashPut(t *testing.T, fs *FSObjects, bucket, key string, data []byte) ObjectInfo {
	t.Helper()
	oi, err := fs.PutObject(GlobalContext, bucket, key,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""),
		ObjectOptions{UserDefined: map[string]string{"x-amz-meta-test": "preserved"}})
	if err != nil {
		t.Fatalf("PUT %q: %v", key, err)
	}
	if oi.Name != key || oi.Size != int64(len(data)) {
		t.Fatalf("PUT returned physical name/wrong length: %+v", oi)
	}
	return oi
}

func fsHashRead(t *testing.T, fs *FSObjects, bucket, key string, want []byte) ObjectInfo {
	t.Helper()
	r, err := fs.GetObjectNInfo(GlobalContext, bucket, key, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		t.Fatalf("GET %q: %v", key, err)
	}
	got, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(got, want) {
		t.Fatalf("GET %q differs: length=%d/%d read=%v close=%v", key, len(got), len(want), readErr, closeErr)
	}
	oi, err := fs.GetObjectInfo(GlobalContext, bucket, key, ObjectOptions{})
	if err != nil || oi.Name != key || oi.Size != int64(len(want)) {
		t.Fatalf("HEAD %q: %+v, err=%v", key, oi, err)
	}
	if r.ObjInfo.Name != key {
		t.Fatalf("reader leaked physical name %q", r.ObjInfo.Name)
	}
	return oi
}

func fsHashListKeys(t *testing.T, fs *FSObjects, prefix string, pageSize int) []string {
	t.Helper()
	var got []string
	marker := ""
	for pages := 0; pages < 100; pages++ {
		page, err := fs.ListObjects(GlobalContext, "bucket", prefix, marker, "", pageSize)
		if err != nil {
			t.Fatal(err)
		}
		for _, oi := range page.Objects {
			got = append(got, oi.Name)
		}
		if !page.IsTruncated {
			return got
		}
		if page.NextMarker <= marker {
			t.Fatalf("pagination did not advance: %q -> %q", marker, page.NextMarker)
		}
		marker = page.NextMarker
	}
	t.Fatal("pagination exceeded bound")
	return nil
}

func TestFSHashLayoutRoundTripAndRange(t *testing.T) {
	fs := fsHashTestSetup(t)
	keys := []string{"data", "fs.json", ".tmp", ".minio-hash-v1", "a", "a/child", "a/", "中文/空 格", strings.Repeat("long", 200), "a/b"}
	for i, key := range keys {
		t.Run(fmt.Sprintf("key-%d", i), func(t *testing.T) {
			data := bytes.Repeat([]byte{byte(i + 1)}, []int{0, 1, 4095, 4096, 4097}[i%5])
			if strings.HasSuffix(key, "/") {
				data = nil
			}
			oi := fsHashPut(t, fs, "bucket", key, data)
			sum := md5.Sum(data)
			if oi.ETag != hex.EncodeToString(sum[:]) {
				t.Fatalf("wrong ETag %q", oi.ETag)
			}
			fsHashRead(t, fs, "bucket", key, data)
			dataPath := fs.objectPath("bucket", key)
			if filepath.Dir(dataPath) != fs.hashObjectDir("bucket", key) {
				t.Fatal("data is not in its independent object directory")
			}
			entries, err := os.ReadDir(filepath.Dir(dataPath))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), "data.tmp.") {
					t.Fatalf("completed PUT left a temporary object: %s", entry.Name())
				}
			}
		})
	}
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	fsHashPut(t, fs, "bucket", "range/object", data)
	r, err := fs.GetObjectNInfo(GlobalContext, "bucket", "range/object", &HTTPRangeSpec{Start: 3, End: 15}, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, data[3:16]) {
		t.Fatalf("range differs: %q %v", got, err)
	}
}

func TestFSHashLayoutListAndDelete(t *testing.T) {
	fs := fsHashTestSetup(t)
	keys := []string{"z", "a", "a/one", "a/two", "a/sub/three", "b/one", "data", "fs.json"}
	for _, key := range keys {
		fsHashPut(t, fs, "bucket", key, []byte(key))
	}
	if err := fs.DeleteBucket(GlobalContext, "bucket", DeleteBucketOptions{}); err == nil {
		t.Fatal("nonempty hashed bucket was deleted")
	}
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if got := fsHashListKeys(t, fs, "", 3); !reflect.DeepEqual(got, want) {
		t.Fatalf("logical sorted pages: got %q want %q", got, want)
	}
	if got := fsHashListKeys(t, fs, "a/", 2); !reflect.DeepEqual(got, []string{"a/one", "a/sub/three", "a/two"}) {
		t.Fatalf("prefix list: %q", got)
	}
	page, err := fs.ListObjects(GlobalContext, "bucket", "a/", "", "/", 100)
	if err != nil || !reflect.DeepEqual(page.Prefixes, []string{"a/sub/"}) {
		t.Fatalf("delimiter prefixes: %q, err=%v", page.Prefixes, err)
	}
	var names []string
	for _, oi := range page.Objects {
		names = append(names, oi.Name)
	}
	if !reflect.DeepEqual(names, []string{"a/one", "a/two"}) {
		t.Fatalf("delimiter objects: %q", names)
	}
	var v2Names []string
	token := ""
	for i := 0; i < 10; i++ {
		v2, err := fs.ListObjectsV2(GlobalContext, "bucket", "", token, "", 3, false, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, oi := range v2.Objects {
			v2Names = append(v2Names, oi.Name)
		}
		if !v2.IsTruncated {
			break
		}
		if v2.NextContinuationToken == "" || v2.NextContinuationToken == token {
			t.Fatal("V2 token did not advance")
		}
		token = v2.NextContinuationToken
	}
	if !reflect.DeepEqual(v2Names, want) {
		t.Fatalf("V2 pages: %q", v2Names)
	}
	if _, err := fs.DeleteObject(GlobalContext, "bucket", "a", ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	fsHashRead(t, fs, "bucket", "a/one", []byte("a/one"))
	deleted, errs := fs.DeleteObjects(GlobalContext, "bucket", []ObjectToDelete{{ObjectV: ObjectV{ObjectName: "a/one"}}, {ObjectV: ObjectV{ObjectName: "fs.json"}}}, ObjectOptions{})
	if len(deleted) != 2 || len(errs) != 2 || errs[0] != nil || errs[1] != nil {
		t.Fatalf("batch delete: %+v %v", deleted, errs)
	}
	if _, err := fs.GetObjectInfo(GlobalContext, "bucket", "a/one", ObjectOptions{}); err == nil {
		t.Fatal("deleted object is still visible")
	}
	for _, key := range fsHashListKeys(t, fs, "", 100) {
		if _, err := fs.DeleteObject(GlobalContext, "bucket", key, ObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.DeleteBucket(GlobalContext, "bucket", DeleteBucketOptions{}); err != nil {
		t.Fatalf("empty hashed bucket cannot be deleted: %v", err)
	}
}

func TestFSHashLayoutFailedWritesPreservePublishedObject(t *testing.T) {
	fs := fsHashTestSetup(t)
	old := []byte("published data")
	fsHashPut(t, fs, "bucket", "object", old)
	for _, tc := range []struct {
		name   string
		reader func() io.Reader
		size   int64
		md5    string
	}{
		{"bad-md5", func() io.Reader { return bytes.NewReader([]byte("replacement")) }, 11, strings.Repeat("0", 32)},
		{"short-body", func() io.Reader { return bytes.NewReader([]byte("short")) }, 11, ""},
		{"body-error", func() io.Reader { return iotest.ErrReader(errors.New("request interrupted")) }, 11, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{"object", "unpublished/" + tc.name} {
				_, err := fs.PutObject(GlobalContext, "bucket", key, mustGetPutObjReader(t, tc.reader(), tc.size, tc.md5, ""), ObjectOptions{})
				if err == nil {
					t.Fatalf("invalid PUT %q succeeded", key)
				}
			}
			fsHashRead(t, fs, "bucket", "object", old)
			if got := fsHashListKeys(t, fs, "", 100); !reflect.DeepEqual(got, []string{"object"}) {
				t.Fatalf("failed upload became visible: %q", got)
			}
		})
	}
}

func TestFSHashLayoutCopyTagsRestartAndMarker(t *testing.T) {
	fs := fsHashTestSetup(t)
	if err := fs.MakeBucketWithLocation(GlobalContext, "copybucket", BucketOptions{}); err != nil {
		t.Fatal(err)
	}
	data := []byte("source bytes")
	oi := fsHashPut(t, fs, "bucket", "a/source", data)
	oi.PutObjReader = mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", "")
	if _, err := fs.CopyObject(GlobalContext, "bucket", "a/source", "copybucket", "b/destination", oi, ObjectOptions{}, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	fsHashRead(t, fs, "copybucket", "b/destination", data)
	if _, err := fs.PutObjectTags(GlobalContext, "copybucket", "b/destination", "purpose=test&layout=hash", ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	tags, err := fs.GetObjectTags(GlobalContext, "copybucket", "b/destination", ObjectOptions{})
	if err != nil || tags.String() != "layout=hash&purpose=test" {
		t.Fatalf("tags differ: %v %v", tags, err)
	}
	oi, err = fs.GetObjectInfo(GlobalContext, "copybucket", "b/destination", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oi.metadataOnly = true
	oi.UserDefined["x-amz-meta-replaced"] = "yes"
	if _, err := fs.CopyObject(GlobalContext, "copybucket", "b/destination", "copybucket", "b/destination", oi, ObjectOptions{}, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	fsHashRead(t, fs, "copybucket", "b/destination", data)
	root := fs.fsPath
	// An interrupted writer's private temporary name must never be listed as
	// an S3 object, including after the object layer is reopened.
	if err := os.WriteFile(filepath.Join(fs.hashObjectDir("copybucket", "b/destination"), "data.tmp.orphan"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fs.Shutdown(GlobalContext); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MINIO_FS_HASH_LAYOUT", "off")
	if wrong, err := NewFSObjectLayer(root); err == nil {
		_ = wrong.Shutdown(GlobalContext)
		t.Fatal("legacy mode accepted a hashed root")
	}
	t.Setenv("MINIO_FS_HASH_LAYOUT", "on")
	reopened, err := NewFSObjectLayer(root)
	if err != nil {
		t.Fatal(err)
	}
	fs = reopened.(*FSObjects)
	t.Cleanup(func() { _ = fs.Shutdown(GlobalContext) })
	gotInfo := fsHashRead(t, fs, "copybucket", "b/destination", data)
	if gotInfo.UserDefined["x-amz-meta-replaced"] != "yes" {
		t.Fatal("self-copy metadata did not survive restart")
	}
	list, err := fs.ListObjects(GlobalContext, "copybucket", "", "", "", 100)
	if err != nil || len(list.Objects) != 1 || list.Objects[0].Name != "b/destination" {
		t.Fatalf("restart/list exposed physical or temporary files: %+v %v", list, err)
	}
}

func TestFSHashLayoutMultipartCompleteAndAbort(t *testing.T) {
	fs := fsHashTestSetup(t)
	key := "multipart/target"
	first := bytes.Repeat([]byte{0x71}, 5<<20)
	last := bytes.Repeat([]byte{0xa3}, 4097)
	upload, err := fs.NewMultipartUpload(GlobalContext, "bucket", key, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var parts []CompletePart
	for i, data := range [][]byte{first, last} {
		part, err := fs.PutObjectPart(GlobalContext, "bucket", key, upload, i+1,
			mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, CompletePart{PartNumber: i + 1, ETag: part.ETag})
	}
	if got := fsHashListKeys(t, fs, "", 100); len(got) != 0 {
		t.Fatalf("uncompleted multipart visible: %q", got)
	}
	info, err := fs.CompleteMultipartUpload(GlobalContext, "bucket", key, upload, parts, ObjectOptions{})
	if err != nil || info.Name != key {
		t.Fatalf("complete: %+v %v", info, err)
	}
	data := append(append([]byte(nil), first...), last...)
	got := fsHashRead(t, fs, "bucket", key, data)
	if keys := fsHashListKeys(t, fs, "", 100); !reflect.DeepEqual(keys, []string{key}) {
		t.Fatalf("completed multipart missing from logical listing: %q", keys)
	}
	if !strings.HasSuffix(got.ETag, "-2") {
		t.Fatalf("multipart ETag lost: %q", got.ETag)
	}
	upload, err = fs.NewMultipartUpload(GlobalContext, "bucket", "aborted", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.PutObjectPart(GlobalContext, "bucket", "aborted", upload, 1, mustGetPutObjReader(t, bytes.NewReader(last), int64(len(last)), "", ""), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fs.AbortMultipartUpload(GlobalContext, "bucket", "aborted", upload, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.GetObjectInfo(GlobalContext, "bucket", "aborted", ObjectOptions{}); err == nil {
		t.Fatal("aborted multipart became visible")
	}
	upload, err = fs.NewMultipartUpload(GlobalContext, "bucket", "mp/key", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.PutObjectPart(GlobalContext, "bucket", "mp//key", upload, 1,
		mustGetPutObjReader(t, bytes.NewReader(last), int64(len(last)), "", ""), ObjectOptions{}); err == nil {
		t.Fatal("multipart accepted an object name rejected by existing NAS validation")
	}
	part, err := fs.PutObjectPart(GlobalContext, "bucket", "mp/key", upload, 1,
		mustGetPutObjReader(t, bytes.NewReader(last), int64(len(last)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.CompleteMultipartUpload(GlobalContext, "bucket", "mp/key", upload,
		[]CompletePart{{PartNumber: 1, ETag: part.ETag}}, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	fsHashRead(t, fs, "bucket", "mp/key", last)
}

func TestFSHashLayoutPreservesNASKeyValidation(t *testing.T) {
	fs := fsHashTestSetup(t)
	old := []byte("unchanged normal key")
	oi := fsHashPut(t, fs, "bucket", "a/b", old)
	if _, err := fs.PutObject(GlobalContext, "bucket", "a//b", mustGetPutObjReader(t, bytes.NewReader([]byte("bad")), 3, "", ""), ObjectOptions{}); !isSameType(err, ObjectNameInvalid{}) {
		t.Fatalf("PUT did not preserve existing NAS rejection: %v", err)
	}
	oi.PutObjReader = mustGetPutObjReader(t, bytes.NewReader(old), int64(len(old)), "", "")
	if _, err := fs.CopyObject(GlobalContext, "bucket", "a/b", "bucket", "a//b", oi, ObjectOptions{}, ObjectOptions{}); !isSameType(err, ObjectNameInvalid{}) {
		t.Fatalf("COPY did not preserve existing NAS rejection: %v", err)
	}
	if _, err := fs.NewMultipartUpload(GlobalContext, "bucket", "a//b", ObjectOptions{}); !isSameType(err, ObjectNameInvalid{}) {
		t.Fatalf("MPU did not preserve existing NAS rejection: %v", err)
	}
	fsHashRead(t, fs, "bucket", "a/b", old)
	if keys := fsHashListKeys(t, fs, "", 100); !reflect.DeepEqual(keys, []string{"a/b"}) {
		t.Fatalf("rejected operation polluted listing: %q", keys)
	}
}

func TestFSHashLayoutConcurrentKeysAndOverwrite(t *testing.T) {
	fs := fsHashTestSetup(t)
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("common-prefix/%03d", i)
			data := bytes.Repeat([]byte{byte(i)}, 4097)
			_, err := fs.PutObject(GlobalContext, "bucket", key, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if got := fsHashListKeys(t, fs, "common-prefix/", 7); len(got) != 64 {
		t.Fatalf("concurrent publication lost keys: %d", len(got))
	}
	for i := 0; i < 64; i++ {
		fsHashRead(t, fs, "bucket", fmt.Sprintf("common-prefix/%03d", i), bytes.Repeat([]byte{byte(i)}, 4097))
	}
	fsHashPut(t, fs, "bucket", "same-key", []byte("old"))
	wg = sync.WaitGroup{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := bytes.Repeat([]byte{byte(i)}, 8193)
			if _, err := fs.PutObject(GlobalContext, "bucket", "same-key", mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	r, err := fs.GetObjectNInfo(GlobalContext, "bucket", "same-key", nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || len(got) != 8193 || got[0] > 7 || !bytes.Equal(got, bytes.Repeat(got[:1], len(got))) {
		t.Fatalf("same-key overwrite left mixed content: length=%d err=%v", len(got), err)
	}
}

func TestFSHashLayoutRejectsLegacyAndNonemptyRoots(t *testing.T) {
	legacyRoot := t.TempDir()
	t.Setenv("MINIO_FS_HASH_LAYOUT", "off")
	legacy := initFSObjects(legacyRoot, t)
	if err := legacy.MakeBucketWithLocation(GlobalContext, "bucket", BucketOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Shutdown(GlobalContext); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MINIO_FS_HASH_LAYOUT", "on")
	if wrong, err := NewFSObjectLayer(legacyRoot); err == nil {
		_ = wrong.Shutdown(GlobalContext)
		t.Fatal("hash mode silently adopted a legacy root")
	}
	nonempty := t.TempDir()
	if err := os.WriteFile(filepath.Join(nonempty, "business-data"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if wrong, err := NewFSObjectLayer(nonempty); err == nil {
		_ = wrong.Shutdown(GlobalContext)
		t.Fatal("hash mode silently adopted a nonempty root")
	}
	got, err := os.ReadFile(filepath.Join(nonempty, "business-data"))
	if err != nil || string(got) != "keep" {
		t.Fatal("rejected root changed pre-existing data")
	}
	entries, err := os.ReadDir(nonempty)
	if err != nil || len(entries) != 1 || entries[0].Name() != "business-data" {
		t.Fatalf("rejected root was modified: %v %v", entries, err)
	}
}

func TestFSHashLayoutOptimizedBoundaryWrites(t *testing.T) {
	fs := fsHashTestSetup(t)
	globalFSDirectIO, globalFSParallelChecksum, globalFSReverseWriteSize = true, true, 2<<20
	for _, size := range []int{(4 << 20) - 1, 4 << 20, (4 << 20) + 1} {
		key := fmt.Sprintf("boundary/%d", size)
		data := bytes.Repeat([]byte{byte(size)}, size)
		fsHashPut(t, fs, "bucket", key, data)
		fsHashRead(t, fs, "bucket", key, data)
		st, err := os.Stat(fs.objectPath("bucket", key))
		if err != nil || st.Size() != int64(size) {
			t.Fatalf("padding leaked into published object: size=%d stat=%v err=%v", size, st, err)
		}
	}
}
