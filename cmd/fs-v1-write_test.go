// Copyright (c) 2026 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack.
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
	"testing"
	"testing/iotest"

	"github.com/jackblack369/minio/internal/hash"
	xioutil "github.com/jackblack369/minio/internal/ioutil"
)

type fsFragmentReader struct {
	reader io.Reader
	size   int
}

func (r fsFragmentReader) Read(p []byte) (int, error) {
	if len(p) > r.size {
		p = p[:r.size]
	}
	return r.reader.Read(p)
}

type fsWriteTestFile struct {
	bytes.Buffer
	writes         []int
	writeErr       error
	closeErr       error
	shortWrite     bool
	closed         bool
	readFromCalled bool
}

func (f *fsWriteTestFile) Write(p []byte) (int, error) {
	f.writes = append(f.writes, len(p))
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.shortWrite {
		return f.Buffer.Write(p[:len(p)/2])
	}
	return f.Buffer.Write(p)
}

func (f *fsWriteTestFile) ReadFrom(io.Reader) (int64, error) {
	f.readFromCalled = true
	return 0, errors.New("unexpected ReadFrom bypass")
}

func (f *fsWriteTestFile) Close() error {
	f.closed = true
	return f.closeErr
}

func TestFSWriteFileAggregatesShortReads(t *testing.T) {
	for _, tc := range []struct {
		size       int
		wantWrites int
	}{
		{size: 0, wantWrites: 0},
		{size: 1, wantWrites: 1},
		{size: 1 << 20, wantWrites: 1},
		{size: (4 << 20) - 1, wantWrites: 1},
		{size: 4 << 20, wantWrites: 1},
		{size: (4 << 20) + 1, wantWrites: 2},
		{size: 8 << 20, wantWrites: 2},
		{size: (8 << 20) + 1, wantWrites: 3},
	} {
		t.Run(fmt.Sprint(tc.size), func(t *testing.T) {
			data := bytes.Repeat([]byte{0xa5}, tc.size)
			dst := &fsWriteTestFile{}
			n, err := fsWriteFile(dst, fsFragmentReader{bytes.NewReader(data), 317})
			if err != nil || n != int64(tc.size) {
				t.Fatalf("write = (%d, %v), want (%d, nil)", n, err, tc.size)
			}
			if !bytes.Equal(dst.Bytes(), data) || !dst.closed || dst.readFromCalled {
				t.Fatalf("content or close invalid; closed=%v, ReadFrom bypass=%v", dst.closed, dst.readFromCalled)
			}
			if len(dst.writes) != tc.wantWrites {
				t.Fatalf("writes = %v, want %d writes", dst.writes, tc.wantWrites)
			}
			for i, length := range dst.writes {
				if i < len(dst.writes)-1 && length != fsWriteBufferSize {
					t.Fatalf("write %d = %d, want full buffer", i, length)
				}
			}
		})
	}
}

func TestFSWriteFileDataAndEOF(t *testing.T) {
	// Readers may return the final payload and EOF together, including when
	// the payload ends exactly at the aggregation boundary.
	for _, size := range []int{(4 << 20) - 1, 4 << 20, (4 << 20) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := bytes.Repeat([]byte{0x3c}, size)
			src := iotest.DataErrReader(fsFragmentReader{bytes.NewReader(data), 4093})
			dst := &fsWriteTestFile{}
			n, err := fsWriteFile(dst, src)
			if err != nil || n != int64(size) || !bytes.Equal(dst.Bytes(), data) || !dst.closed {
				t.Fatalf("write = (%d, %v), content length = %d, closed = %v", n, err, dst.Len(), dst.closed)
			}
		})
	}
}

func TestFSWriteFileErrors(t *testing.T) {
	writeErr := errors.New("write failed")
	closeErr := errors.New("close failed")
	readErr := errors.New("read failed")
	for _, tc := range []struct {
		name      string
		size      int
		file      fsWriteTestFile
		readErr   error
		wantErr   error
		wantBytes int64
	}{
		{name: "flush", size: 17, file: fsWriteTestFile{writeErr: writeErr}, wantErr: writeErr},
		{name: "full buffer write", size: fsWriteBufferSize + 17, file: fsWriteTestFile{writeErr: writeErr}, wantErr: writeErr},
		{name: "short write", size: 18, file: fsWriteTestFile{shortWrite: true}, wantErr: io.ErrShortWrite, wantBytes: 9},
		{name: "full buffer short write", size: fsWriteBufferSize + 17, file: fsWriteTestFile{shortWrite: true}, wantErr: io.ErrShortWrite, wantBytes: fsWriteBufferSize / 2},
		{name: "close", size: 17, file: fsWriteTestFile{closeErr: closeErr}, wantErr: closeErr, wantBytes: 17},
		{name: "write before close", size: 17, file: fsWriteTestFile{writeErr: writeErr, closeErr: closeErr}, wantErr: writeErr},
		{name: "read before flush", size: 17, readErr: readErr, wantErr: readErr},
		{name: "read before close", size: fsWriteBufferSize + 17, file: fsWriteTestFile{closeErr: closeErr}, readErr: readErr, wantErr: readErr, wantBytes: fsWriteBufferSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var src io.Reader = bytes.NewReader(make([]byte, tc.size))
			if tc.readErr != nil {
				src = io.MultiReader(src, iotest.ErrReader(tc.readErr))
			}
			n, err := fsWriteFile(&tc.file, src)
			if !errors.Is(err, tc.wantErr) || n != tc.wantBytes {
				t.Fatalf("write = (%d, %v), want (%d, %v)", n, err, tc.wantBytes, tc.wantErr)
			}
			if !tc.file.closed {
				t.Fatal("destination was not closed")
			}
		})
	}
}

func TestFSWriteFileChecksums(t *testing.T) {
	// An exact buffer boundary must still cause a final Read through EOF:
	// hash.Reader checks both digests only when it sees EOF.
	data := bytes.Repeat([]byte{0x5a}, 4<<20)
	md5Sum := md5.Sum(data)
	shaSum := sha256.Sum256(data)
	md5Hex := hex.EncodeToString(md5Sum[:])
	shaHex := hex.EncodeToString(shaSum[:])
	for _, tc := range []struct {
		name   string
		md5    string
		sha    string
		badMD5 bool
		badSHA bool
	}{
		{name: "valid", md5: md5Hex, sha: shaHex},
		{name: "bad md5", md5: "00000000000000000000000000000000", sha: shaHex, badMD5: true},
		{name: "bad sha256", md5: md5Hex, sha: hex.EncodeToString(make([]byte, sha256.Size)), badSHA: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, err := hash.NewReader(fsFragmentReader{bytes.NewReader(data), 4093}, int64(len(data)), tc.md5, tc.sha, int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			dst := &fsWriteTestFile{}
			_, err = fsWriteFile(dst, src)
			var md5Err hash.BadDigest
			var shaErr hash.SHA256Mismatch
			if tc.badMD5 {
				if !errors.As(err, &md5Err) {
					t.Fatalf("want MD5 mismatch, got %v", err)
				}
			} else if tc.badSHA {
				if !errors.As(err, &shaErr) {
					t.Fatalf("want SHA256 mismatch, got %v", err)
				}
			} else if err != nil || !bytes.Equal(dst.Bytes(), data) {
				t.Fatalf("valid checksums failed: %v", err)
			}
			if !dst.closed {
				t.Fatal("destination was not closed")
			}
		})
	}
}

func BenchmarkFSWriteFile4MiB(b *testing.B) {
	data := bytes.Repeat([]byte{0x5a}, 4<<20)
	for _, aggregated := range []bool{false, true} {
		name := "baseline"
		if aggregated {
			name = "aggregated"
		}
		b.Run(name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "object")
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
				if err != nil {
					b.Fatal(err)
				}
				src, err := hash.NewReader(fsFragmentReader{bytes.NewReader(data), 32 << 10}, int64(len(data)), "", "", int64(len(data)))
				if err != nil {
					dst.Close()
					b.Fatal(err)
				}
				if aggregated {
					_, err = fsWriteFile(dst, src)
				} else {
					_, err = xioutil.Copy(dst, src)
					if closeErr := dst.Close(); err == nil {
						err = closeErr
					}
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
