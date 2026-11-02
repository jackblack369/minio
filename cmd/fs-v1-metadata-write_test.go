// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jackblack369/minio/internal/lock"
)

func TestFSMetadataWriteCountAndOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fs.json")
	f, err := lock.LockedOpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	for _, tc := range []struct {
		name string
		meta map[string]string
	}{
		{name: "new metadata", meta: map[string]string{"etag": "first"}},
		{name: "long metadata", meta: map[string]string{"etag": "second", "x-amz-meta-description": strings.Repeat("metadata", 1024)}},
		{name: "shorter overwrite", meta: map[string]string{"etag": "third"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFSMetaV1()
			m.Meta = tc.meta
			want, err := json.Marshal(&m)
			if err != nil {
				t.Fatal(err)
			}
			n, err := m.WriteTo(f)
			if err != nil || n != int64(len(want)) {
				t.Fatalf("WriteTo = (%d, %v), want (%d, nil)", n, err, len(want))
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("file differs from serialized metadata: size = %d, want %d", len(got), len(want))
			}
			var decoded fsMetaV1
			if _, err := decoded.ReadFrom(GlobalContext, f); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded.Meta, tc.meta) {
				t.Fatalf("metadata = %v, want %v", decoded.Meta, tc.meta)
			}
		})
	}
}

type jsonSaveTestFile struct {
	ops         []string
	truncateErr error
	seekErr     error
	writeErr    error
	writeLimit  int
}

func (f *jsonSaveTestFile) Truncate(size int64) error {
	if size != 0 {
		panic("unexpected truncate size")
	}
	f.ops = append(f.ops, "truncate")
	return f.truncateErr
}

func (f *jsonSaveTestFile) Seek(offset int64, whence int) (int64, error) {
	if offset != 0 || whence != io.SeekStart {
		panic("unexpected seek")
	}
	f.ops = append(f.ops, "seek")
	return 0, f.seekErr
}

func (f *jsonSaveTestFile) Write(p []byte) (int, error) {
	f.ops = append(f.ops, "write")
	n := len(p)
	if f.writeLimit >= 0 && f.writeLimit < n {
		n = f.writeLimit
	}
	return n, f.writeErr
}

func TestJSONSaveCountErrors(t *testing.T) {
	failure := errors.New("injected filesystem failure")
	data := map[string]string{"etag": "content digest"}
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		file jsonSaveTestFile
		n    int64
		err  error
		ops  []string
	}{
		{name: "success", file: jsonSaveTestFile{writeLimit: -1}, n: int64(len(encoded)), ops: []string{"truncate", "seek", "write"}},
		{name: "truncate error", file: jsonSaveTestFile{truncateErr: failure}, err: failure, ops: []string{"truncate"}},
		{name: "seek error", file: jsonSaveTestFile{seekErr: failure}, err: failure, ops: []string{"truncate", "seek"}},
		{name: "write error", file: jsonSaveTestFile{writeLimit: 0, writeErr: failure}, err: failure, ops: []string{"truncate", "seek", "write"}},
		{name: "partial write error", file: jsonSaveTestFile{writeLimit: 3, writeErr: failure}, n: 3, err: failure, ops: []string{"truncate", "seek", "write"}},
		{name: "short write", file: jsonSaveTestFile{writeLimit: 3}, n: 3, err: io.ErrShortWrite, ops: []string{"truncate", "seek", "write"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := jsonSaveCount(&tc.file, data)
			if n != tc.n || !errors.Is(err, tc.err) {
				t.Fatalf("jsonSaveCount = (%d, %v), want (%d, %v)", n, err, tc.n, tc.err)
			}
			if !reflect.DeepEqual(tc.file.ops, tc.ops) {
				t.Fatalf("operations = %v, want %v", tc.file.ops, tc.ops)
			}
		})
	}

	t.Run("marshal error leaves existing file untouched", func(t *testing.T) {
		f := &jsonSaveTestFile{}
		n, err := jsonSaveCount(f, make(chan int))
		if n != 0 || err == nil || len(f.ops) != 0 {
			t.Fatalf("jsonSaveCount = (%d, %v), operations = %v", n, err, f.ops)
		}
	})
	t.Run("jsonSave propagates short write", func(t *testing.T) {
		f := &jsonSaveTestFile{writeLimit: 3}
		if err := jsonSave(f, data); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("jsonSave error = %v, want %v", err, io.ErrShortWrite)
		}
	})
}
