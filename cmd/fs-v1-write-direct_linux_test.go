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
	"reflect"
	"testing"
	"testing/iotest"
	"unsafe"

	"github.com/jackblack369/minio/internal/hash"
)

// Keep the original write payloads as well as the truncated contents: the
// former let the tests inspect alignment and padding before truncate hides it.
type fsDirectWriteTestFile struct {
	data        []byte
	writes      [][]byte
	addresses   []uintptr
	truncates   []int64
	events      []string
	writeErr    error
	truncateErr error
	closeErr    error
	shortWrite  bool
	closes      int
}

func (f *fsDirectWriteTestFile) Write(p []byte) (int, error) {
	f.events = append(f.events, "write")
	f.writes = append(f.writes, append([]byte(nil), p...))
	var address uintptr
	if len(p) > 0 {
		address = uintptr(unsafe.Pointer(&p[0]))
	}
	f.addresses = append(f.addresses, address)
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	n := len(p)
	if f.shortWrite {
		n /= 2
	}
	f.data = append(f.data, p[:n]...)
	return n, nil
}

func (f *fsDirectWriteTestFile) Truncate(size int64) error {
	f.events = append(f.events, "truncate")
	f.truncates = append(f.truncates, size)
	if f.truncateErr != nil {
		return f.truncateErr
	}
	if size < 0 || size > int64(len(f.data)) {
		return fmt.Errorf("invalid truncate size %d for %d bytes", size, len(f.data))
	}
	f.data = f.data[:size]
	return nil
}

func (f *fsDirectWriteTestFile) Close() error {
	f.events = append(f.events, "close")
	f.closes++
	return f.closeErr
}

func TestFSWriteFileDirectIOAlignmentAndLength(t *testing.T) {
	const alignment = 4096
	const blockSize = 4 << 20
	for _, size := range []int{0, 1, alignment - 1, alignment, alignment + 1, 1 << 20, blockSize - 1, blockSize, blockSize + 1, 2*blockSize + 17} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			// Nonzero data in the preceding full buffer catches stale bytes in
			// tail padding when the aggregation buffer is reused.
			data := bytes.Repeat([]byte{0xa5}, size)
			dst := &fsDirectWriteTestFile{}
			n, err := fsWriteFileDirectIO(dst, fsFragmentReader{bytes.NewReader(data), 317})
			if err != nil || n != int64(size) {
				t.Fatalf("write = (%d, %v), want (%d, nil)", n, err, size)
			}
			if !bytes.Equal(dst.data, data) {
				t.Fatalf("logical contents differ: got %d bytes, want %d", len(dst.data), size)
			}
			wantWrites := (size + blockSize - 1) / blockSize
			if len(dst.writes) != wantWrites {
				t.Fatalf("write calls = %d, want %d", len(dst.writes), wantWrites)
			}
			remaining := size
			for i, payload := range dst.writes {
				if dst.addresses[i]%alignment != 0 || len(payload)%alignment != 0 {
					t.Fatalf("write %d is unaligned: address=%#x length=%d", i, dst.addresses[i], len(payload))
				}
				logical := remaining
				if logical > blockSize {
					logical = blockSize
				}
				wantLength := (logical + alignment - 1) / alignment * alignment
				if len(payload) != wantLength {
					t.Fatalf("write %d length = %d, want %d", i, len(payload), wantLength)
				}
				for j, b := range payload[logical:] {
					if b != 0 {
						t.Fatalf("write %d padding byte %d = %#x, want zero", i, j, b)
					}
				}
				remaining -= logical
			}
			if size%alignment != 0 {
				if !reflect.DeepEqual(dst.truncates, []int64{int64(size)}) {
					t.Fatalf("truncate calls = %v, want [%d]", dst.truncates, size)
				}
				if len(dst.events) < 2 || dst.events[len(dst.events)-2] != "truncate" {
					t.Fatalf("truncate must precede close: %v", dst.events)
				}
			} else if len(dst.truncates) != 0 {
				t.Fatalf("aligned object must not need truncate: %v", dst.truncates)
			}
			if dst.closes != 1 || dst.events[len(dst.events)-1] != "close" {
				t.Fatalf("close calls = %d, events = %v", dst.closes, dst.events)
			}
		})
	}
}

func TestFSWriteFileDirectIODataAndEOF(t *testing.T) {
	for _, size := range []int{4095, 4096, (4 << 20) - 1, 4 << 20, (4 << 20) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := bytes.Repeat([]byte{0x3c}, size)
			src := iotest.DataErrReader(fsFragmentReader{bytes.NewReader(data), 4093})
			dst := &fsDirectWriteTestFile{}
			n, err := fsWriteFileDirectIO(dst, src)
			if err != nil || n != int64(size) || !bytes.Equal(dst.data, data) || dst.closes != 1 {
				t.Fatalf("write = (%d, %v), contents length = %d, closes = %d", n, err, len(dst.data), dst.closes)
			}
		})
	}
}

func TestFSWriteFileDirectIOErrors(t *testing.T) {
	writeErr := errors.New("write failed")
	readErr := errors.New("read failed")
	truncateErr := errors.New("truncate failed")
	closeErr := errors.New("close failed")
	for _, tc := range []struct {
		name    string
		size    int
		file    fsDirectWriteTestFile
		readErr error
		wantErr error
	}{
		{name: "tail write", size: 17, file: fsDirectWriteTestFile{writeErr: writeErr}, wantErr: writeErr},
		{name: "full write", size: (4 << 20) + 17, file: fsDirectWriteTestFile{writeErr: writeErr}, wantErr: writeErr},
		{name: "tail short write", size: 3000, file: fsDirectWriteTestFile{shortWrite: true}, wantErr: io.ErrShortWrite},
		{name: "full short write", size: 4 << 20, file: fsDirectWriteTestFile{shortWrite: true}, wantErr: io.ErrShortWrite},
		{name: "read", size: 17, readErr: readErr, wantErr: readErr},
		{name: "read after full write", size: (4 << 20) + 17, readErr: readErr, wantErr: readErr},
		{name: "truncate", size: 17, file: fsDirectWriteTestFile{truncateErr: truncateErr}, wantErr: truncateErr},
		{name: "close", size: 17, file: fsDirectWriteTestFile{closeErr: closeErr}, wantErr: closeErr},
		{name: "empty close", file: fsDirectWriteTestFile{closeErr: closeErr}, wantErr: closeErr},
		{name: "write before close", size: 17, file: fsDirectWriteTestFile{writeErr: writeErr, closeErr: closeErr}, wantErr: writeErr},
		{name: "read before close", size: 17, file: fsDirectWriteTestFile{closeErr: closeErr}, readErr: readErr, wantErr: readErr},
		{name: "truncate before close", size: 17, file: fsDirectWriteTestFile{truncateErr: truncateErr, closeErr: closeErr}, wantErr: truncateErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var src io.Reader = bytes.NewReader(bytes.Repeat([]byte{0x69}, tc.size))
			if tc.readErr != nil {
				src = io.MultiReader(src, iotest.ErrReader(tc.readErr))
			}
			_, err := fsWriteFileDirectIO(&tc.file, src)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if tc.file.closes != 1 {
				t.Fatalf("close calls = %d, want 1", tc.file.closes)
			}
			if (tc.readErr != nil || tc.file.writeErr != nil || tc.file.shortWrite) && len(tc.file.truncates) != 0 {
				t.Fatalf("failed copy must not finalize length: %v", tc.file.truncates)
			}
		})
	}
}

// Unlike io.MultiReader, this reader deliberately returns the final error with
// n > 0. io.ReadFull would hide that error when n fills the aggregation buffer.
type fsDirectDataErrorReader struct {
	data []byte
	err  error
}

func (r *fsDirectDataErrorReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		// Errors are reported once. A caller that drops a full buffer's
		// simultaneous error cannot recover it by reading again.
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

type fsDirectNoProgressReader struct{}

func (fsDirectNoProgressReader) Read([]byte) (int, error) { return 0, nil }

func TestFSWriteFileDirectIONoProgress(t *testing.T) {
	dst := &fsDirectWriteTestFile{}
	n, err := fsWriteFileDirectIO(dst, fsDirectNoProgressReader{})
	if !errors.Is(err, io.ErrNoProgress) || n != 0 {
		t.Fatalf("write = (%d, %v), want (0, ErrNoProgress)", n, err)
	}
	if len(dst.writes) != 0 || len(dst.truncates) != 0 || dst.closes != 1 {
		t.Fatalf("no-progress reader touched destination: writes=%d truncates=%v closes=%d", len(dst.writes), dst.truncates, dst.closes)
	}
}

func TestFSWriteFileDirectIODataAndError(t *testing.T) {
	readErr := errors.New("checksum failed with final data")
	for _, size := range []int{17, (4 << 20) - 1, 4 << 20, (4 << 20) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			src := &fsDirectDataErrorReader{data: bytes.Repeat([]byte{0xa5}, size), err: readErr}
			dst := &fsDirectWriteTestFile{closeErr: errors.New("secondary close failure")}
			_, err := fsWriteFileDirectIO(dst, src)
			if !errors.Is(err, readErr) {
				t.Fatalf("data+error was lost: %v", err)
			}
			if dst.closes != 1 || len(dst.truncates) != 0 {
				t.Fatalf("failed copy was finalized incorrectly: closes=%d, truncates=%v", dst.closes, dst.truncates)
			}
		})
	}
}

func TestFSWriteFileDirectIOChecksums(t *testing.T) {
	// Exercise EOF on a separate read and alongside the last data. With the
	// exact 4 MiB case, a swallowed digest error would let PutObject publish a
	// corrupt object because the returned byte count matches Content-Length.
	for _, finalDataAndEOF := range []bool{false, true} {
		for _, size := range []int{(4 << 20) - 1, 4 << 20, (4 << 20) + 1} {
			data := bytes.Repeat([]byte{0x5a}, size)
			md5Sum := md5.Sum(data)
			shaSum := sha256.Sum256(data)
			md5Hex := hex.EncodeToString(md5Sum[:])
			shaHex := hex.EncodeToString(shaSum[:])
			for _, digest := range []string{"valid", "bad-md5", "bad-sha256"} {
				t.Run(fmt.Sprintf("%s/%d/data-and-eof=%t", digest, size, finalDataAndEOF), func(t *testing.T) {
					var input io.Reader = bytes.NewReader(data)
					if finalDataAndEOF {
						input = &fsDirectDataErrorReader{data: data, err: io.EOF}
					}
					wantMD5, wantSHA := md5Hex, shaHex
					if digest == "bad-md5" {
						wantMD5 = hex.EncodeToString(make([]byte, md5.Size))
					} else if digest == "bad-sha256" {
						wantSHA = hex.EncodeToString(make([]byte, sha256.Size))
					}
					src, err := hash.NewReader(input, int64(size), wantMD5, wantSHA, int64(size))
					if err != nil {
						t.Fatal(err)
					}
					dst := &fsDirectWriteTestFile{}
					n, err := fsWriteFileDirectIO(dst, src)
					var md5Err hash.BadDigest
					var shaErr hash.SHA256Mismatch
					switch digest {
					case "bad-md5":
						if !errors.As(err, &md5Err) {
							t.Fatalf("want MD5 mismatch, got %v", err)
						}
					case "bad-sha256":
						if !errors.As(err, &shaErr) {
							t.Fatalf("want SHA256 mismatch, got %v", err)
						}
					default:
						if err != nil || n != int64(size) || !bytes.Equal(dst.data, data) {
							t.Fatalf("valid data rejected: n=%d err=%v length=%d", n, err, len(dst.data))
						}
					}
					if dst.closes != 1 {
						t.Fatalf("close calls = %d, want 1", dst.closes)
					}
					if digest != "valid" && len(dst.truncates) != 0 {
						t.Fatalf("checksum failure must not finalize length: %v", dst.truncates)
					}
				})
			}
		}
	}
}
