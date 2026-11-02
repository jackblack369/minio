//go:build linux
// +build linux

// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"testing/iotest"
	"time"

	"github.com/jackblack369/minio/internal/hash"
)

type fsChecksumTestFile struct {
	data                       []byte
	offsets                    []int64
	written                    int
	writeErr, closeErr         error
	writePanic, closePanic     interface{}
	shortWrite                 bool
	closes                     int
	closeEntered, releaseClose chan struct{}
}

func (f *fsChecksumTestFile) Write(p []byte) (int, error) {
	return f.WriteAt(p, 0)
}

func (f *fsChecksumTestFile) WriteAt(p []byte, offset int64) (int, error) {
	if f.writePanic != nil {
		panic(f.writePanic)
	}
	n := len(p)
	if f.shortWrite {
		n /= 2
	}
	if len(f.data) == 0 {
		f.data = make([]byte, fsWriteBufferSize)
	}
	copy(f.data[int(offset):], p[:n])
	f.offsets = append(f.offsets, offset)
	f.written += n
	return n, f.writeErr
}

func (f *fsChecksumTestFile) Close() error {
	f.closes++
	if f.closeEntered != nil {
		close(f.closeEntered)
	}
	if f.releaseClose != nil {
		<-f.releaseClose
	}
	if f.closePanic != nil {
		panic(f.closePanic)
	}
	return f.closeErr
}

func TestFSParallelChecksumOverlapsAndJoins(t *testing.T) {
	data := bytes.Repeat([]byte{0x73}, fsWriteBufferSize)
	f := &fsChecksumTestFile{closeEntered: make(chan struct{}), releaseClose: make(chan struct{})}
	verifyEntered, releaseVerify := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := fsWriteFileParallelChecksum(f, data, func() error {
			close(verifyEntered)
			<-releaseVerify
			return nil
		}, 1<<20)
		finished <- err
	}()
	// Both operations must start before either is released. This detects a
	// sequential verify-before-close implementation without relying on timing.
	for _, entered := range []chan struct{}{verifyEntered, f.closeEntered} {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("checksum and Close did not overlap")
		}
	}
	close(f.releaseClose)
	select {
	case <-finished:
		t.Fatal("returned before verification completed")
	default:
	}
	close(releaseVerify)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if f.closes != 1 || !bytes.Equal(f.data, data) || !reflect.DeepEqual(f.offsets, []int64{3 << 20, 2 << 20, 1 << 20, 0}) {
		t.Fatalf("closes=%d offsets=%v contentsEqual=%v", f.closes, f.offsets, bytes.Equal(f.data, data))
	}
}

func TestFSParallelChecksumErrorsAndPanics(t *testing.T) {
	digestErr, writeErr, closeErr := errors.New("digest"), errors.New("write"), errors.New("close")
	data := make([]byte, fsWriteBufferSize)
	for _, tc := range []struct {
		name        string
		file        fsChecksumTestFile
		verifyErr   error
		verifyPanic interface{}
		wantErr     error
		wantPanic   interface{}
	}{
		{name: "write", file: fsChecksumTestFile{writeErr: writeErr}, wantErr: writeErr},
		{name: "short write", file: fsChecksumTestFile{shortWrite: true}, wantErr: io.ErrShortWrite},
		{name: "close", file: fsChecksumTestFile{closeErr: closeErr}, wantErr: closeErr},
		{name: "digest before write and close", file: fsChecksumTestFile{writeErr: writeErr, closeErr: closeErr}, verifyErr: digestErr, wantErr: digestErr},
		{name: "write panic joins", file: fsChecksumTestFile{writePanic: "write panic"}, wantPanic: "write panic"},
		{name: "close panic joins", file: fsChecksumTestFile{closePanic: "close panic"}, wantPanic: "close panic"},
		{name: "checksum panic transferred", verifyPanic: "hash panic", wantPanic: "hash panic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verified := make(chan struct{})
			var gotErr error
			var recovered interface{}
			func() {
				defer func() { recovered = recover() }()
				_, gotErr = fsWriteFileParallelChecksum(&tc.file, data, func() error {
					defer close(verified)
					if tc.verifyPanic != nil {
						panic(tc.verifyPanic)
					}
					return tc.verifyErr
				}, 1<<20)
			}()
			select {
			case <-verified:
			default:
				t.Fatal("checksum worker not joined")
			}
			if !errors.Is(gotErr, tc.wantErr) || recovered != tc.wantPanic || tc.file.closes != 1 {
				t.Fatalf("err=%v panic=%v closes=%d", gotErr, recovered, tc.file.closes)
			}
		})
	}
}

func TestFSParallelChecksumAbortedVerifierFailsClosed(t *testing.T) {
	f := &fsChecksumTestFile{}
	_, err := fsWriteFileParallelChecksum(f, make([]byte, fsWriteBufferSize), func() error {
		runtime.Goexit()
		return nil
	}, 1<<20)
	if !errors.Is(err, errFSChecksumVerificationAborted) || f.closes != 1 {
		t.Fatalf("err=%v closes=%d", err, f.closes)
	}
}

func TestFSCreateFileParallelChecksumIntegration(t *testing.T) {
	oldEnabled, oldDirect, oldChunk := globalFSParallelChecksum, globalFSDirectIO, globalFSReverseWriteSize
	defer func() {
		globalFSParallelChecksum, globalFSDirectIO, globalFSReverseWriteSize = oldEnabled, oldDirect, oldChunk
	}()
	globalFSParallelChecksum, globalFSDirectIO = true, false
	data := bytes.Repeat([]byte{0xa6}, fsWriteBufferSize)
	md5sum, sha := md5.Sum(data), sha256.Sum256(data)
	for _, chunk := range []int{0, 1 << 20, 2 << 20, 4 << 20} {
		for _, bad := range []string{"", "md5", "sha256"} {
			t.Run(fmt.Sprintf("chunk=%d/bad=%s", chunk, bad), func(t *testing.T) {
				globalFSReverseWriteSize = chunk
				md5hex, shahex := hex.EncodeToString(md5sum[:]), hex.EncodeToString(sha[:])
				if bad == "md5" {
					md5hex = "00000000000000000000000000000000"
				}
				if bad == "sha256" {
					shahex = fmt.Sprintf("%064x", 0)
				}
				r, err := hash.NewReader(bytes.NewReader(data), int64(len(data)), md5hex, shahex, int64(len(data)))
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), "unpublished-data")
				n, err := fsCreateFileOptimized(path, os.O_CREATE|os.O_WRONLY, r)
				if bad != "" {
					if err == nil {
						t.Fatal("bad checksum accepted")
					}
					return
				}
				if err != nil || n != int64(len(data)) || hex.EncodeToString(r.MD5Current()) != hex.EncodeToString(md5sum[:]) {
					t.Fatalf("n=%d err=%v etag=%x", n, err, r.MD5Current())
				}
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("file differs, err=%v", err)
				}
			})
		}
	}
}

func TestFSParallelChecksumPublicationAndFallback(t *testing.T) {
	fs := fsParallelTestSetup(t)
	oldEnabled := globalFSParallelChecksum
	defer func() { globalFSParallelChecksum = oldEnabled }()
	globalFSParallelChecksum, globalFSReverseWriteSize = true, 1<<20
	oldData := bytes.Repeat([]byte{0x34}, fsWriteBufferSize)
	if _, err := fs.PutObject(GlobalContext, "bucket", "object", mustGetPutObjReader(t, bytes.NewReader(oldData), int64(len(oldData)), "", ""), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	oldMetadata, err := os.ReadFile(fsParallelMetaPath(fs, "object"))
	if err != nil {
		t.Fatal(err)
	}
	newData := bytes.Repeat([]byte{0x97}, fsWriteBufferSize)
	for _, tc := range []struct {
		name           string
		source         func() io.Reader
		md5hex, shahex string
	}{
		{name: "bad-md5", source: func() io.Reader { return bytes.NewReader(newData) }, md5hex: "00000000000000000000000000000000"},
		{name: "bad-sha", source: func() io.Reader { return bytes.NewReader(newData) }, shahex: fmt.Sprintf("%064x", 0)},
		{name: "short", source: func() io.Reader { return bytes.NewReader(newData[:len(newData)-17]) }},
		{name: "read-error", source: func() io.Reader {
			return io.MultiReader(bytes.NewReader(newData[:1<<20]), iotest.ErrReader(errors.New("request failed")))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{"object", "fresh-" + tc.name} {
				_, err := fs.PutObject(GlobalContext, "bucket", key, mustGetPutObjReader(t, tc.source(), int64(len(newData)), tc.md5hex, tc.shahex), ObjectOptions{})
				if err == nil {
					t.Fatal("invalid upload published")
				}
			}
			fsParallelAssertObject(t, fs, "object", oldData)
			got, err := os.ReadFile(fsParallelMetaPath(fs, "object"))
			if err != nil || !bytes.Equal(got, oldMetadata) {
				t.Fatalf("old metadata modified: %v", err)
			}
			if _, err := os.Stat(filepath.Join(fs.fsPath, "bucket", "fresh-"+tc.name)); !os.IsNotExist(err) {
				t.Fatalf("fresh failed upload exists: %v", err)
			}
			fsParallelAssertEmptyTemp(t, fs)
		})
	}
	for _, size := range []int{0, 4097, fsWriteBufferSize - 1, fsWriteBufferSize + 1} {
		t.Run(fmt.Sprintf("fallback-size=%d", size), func(t *testing.T) {
			data := bytes.Repeat([]byte{0x58}, size)
			key := fmt.Sprintf("fallback-%d", size)
			if _, err := fs.PutObject(GlobalContext, "bucket", key, mustGetPutObjReader(t, bytes.NewReader(data), int64(size), "", ""), ObjectOptions{}); err != nil {
				t.Fatal(err)
			}
			fsParallelAssertObject(t, fs, key, data)
			fsParallelAssertEmptyTemp(t, fs)
		})
	}
}

type fsChecksumLatencyFile struct{}

func (fsChecksumLatencyFile) Write(p []byte) (int, error)            { return len(p), nil }
func (fsChecksumLatencyFile) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }
func (fsChecksumLatencyFile) Truncate(int64) error                   { return nil }
func (fsChecksumLatencyFile) Close() error                           { time.Sleep(35 * time.Millisecond); return nil }

// This is an explicitly synthetic latency experiment, not a storage benchmark.
// It isolates whether real checksum CPU work overlaps a fixed Close wait.
func BenchmarkFSParallelChecksumSyntheticClose(b *testing.B) {
	data := bytes.Repeat([]byte{0xa6}, fsWriteBufferSize)
	for _, parallel := range []bool{false, true} {
		name := "serial"
		if parallel {
			name = "parallel"
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r, err := hash.NewReader(bytes.NewReader(data), int64(len(data)), "", "", int64(len(data)))
				if err != nil {
					b.Fatal(err)
				}
				if !parallel {
					if _, err = fsWriteFileReverse(fsChecksumLatencyFile{}, r, 1<<20); err != nil {
						b.Fatal(err)
					}
				} else {
					block := fsDirectIOBufferPool.Get().(*fsDirectIOBuffer)
					n, verify, readErr := r.ReadAllDeferred(block.data)
					if readErr != nil {
						b.Fatal(readErr)
					}
					_, err = fsWriteFileParallelChecksum(fsChecksumLatencyFile{}, block.data[:n], verify, 1<<20)
					fsDirectIOBufferPool.Put(block)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
