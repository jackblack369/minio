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
	"sort"
	"testing"
	"testing/iotest"
	"unsafe"

	"github.com/jackblack369/minio/internal/hash"
)

type fsReverseTestWrite struct {
	offset  int64
	data    []byte
	address uintptr
}

// Apply accepted bytes at their actual offsets. Appending payloads would hide
// incorrect offsets, holes, overlapping writes, and cross-buffer corruption.
type fsReverseTestFile struct {
	data        []byte
	writes      []fsReverseTestWrite
	truncates   []int64
	events      []string
	failAt      int // one-based WriteAt call; zero disables injection
	writeN      int
	writeErr    error
	truncateErr error
	closeErr    error
	closes      int
}

func (f *fsReverseTestFile) WriteAt(p []byte, offset int64) (int, error) {
	f.events = append(f.events, "write")
	f.writes = append(f.writes, fsReverseTestWrite{
		offset: offset, data: append([]byte(nil), p...), address: uintptr(unsafe.Pointer(&p[0])),
	})
	n, err := len(p), error(nil)
	if len(f.writes) == f.failAt {
		n, err = f.writeN, f.writeErr
	}
	if offset < 0 || n < 0 || n > len(p) {
		return 0, fmt.Errorf("invalid test write: offset=%d n=%d length=%d", offset, n, len(p))
	}
	if n > 0 {
		end := int(offset) + n
		if end > len(f.data) {
			f.data = append(f.data, make([]byte, end-len(f.data))...)
		}
		copy(f.data[int(offset):end], p[:n])
	}
	return n, err
}

func (f *fsReverseTestFile) Truncate(size int64) error {
	f.events = append(f.events, "truncate")
	f.truncates = append(f.truncates, size)
	if f.truncateErr != nil {
		return f.truncateErr
	}
	if size < 0 || size > int64(len(f.data)) {
		return fmt.Errorf("invalid test truncate: %d for %d bytes", size, len(f.data))
	}
	f.data = f.data[:size]
	return nil
}

func (f *fsReverseTestFile) Close() error {
	f.events = append(f.events, "close")
	f.closes++
	return f.closeErr
}

func fsReverseTestData(size int) []byte {
	data := make([]byte, size)
	// A deterministic stream with different contents in every fragment and
	// every aggregation buffer catches reordering that constant data misses.
	state := uint32(0x93c465a7)
	for i := range data {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		data[i] = byte(state)
	}
	return data
}

func fsCheckReverseLayout(t *testing.T, dst *fsReverseTestFile, data []byte, chunkSize int) {
	t.Helper()
	const alignment, groupSize = 4096, 4 << 20
	if !bytes.Equal(dst.data, data) {
		t.Fatalf("logical contents differ: got %d bytes, want %d", len(dst.data), len(data))
	}
	var previousGroup int64 = -1
	var previousOffset int64
	for i, write := range dst.writes {
		group := write.offset / groupSize
		if group < previousGroup || (group == previousGroup && write.offset >= previousOffset) {
			t.Fatalf("write %d is not descending within its group: offset=%d previous=%d", i, write.offset, previousOffset)
		}
		if write.offset%int64(chunkSize) != 0 || len(write.data) == 0 || len(write.data) > chunkSize ||
			write.offset%alignment != 0 || len(write.data)%alignment != 0 || write.address%alignment != 0 {
			t.Fatalf("invalid fragment %d: offset=%d length=%d address=%#x", i, write.offset, len(write.data), write.address)
		}
		if (write.offset+int64(len(write.data))-1)/groupSize != group {
			t.Fatalf("write %d crosses an aggregation boundary", i)
		}
		for j, b := range write.data {
			pos := int(write.offset) + j
			if pos >= len(data) && b != 0 {
				t.Fatalf("padding byte %d = %#x, want zero", pos, b)
			}
		}
		previousGroup, previousOffset = group, write.offset
	}
	// Check the union of physical writes independently of generation order.
	// Every aligned byte must be written exactly once, including tail padding.
	ordered := append([]fsReverseTestWrite(nil), dst.writes...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].offset < ordered[j].offset })
	var end int64
	for _, write := range ordered {
		if write.offset != end {
			t.Fatalf("hole or overlap: previous end=%d next offset=%d", end, write.offset)
		}
		end += int64(len(write.data))
	}
	wantEnd := int64((len(data) + alignment - 1) / alignment * alignment)
	if end != wantEnd {
		t.Fatalf("physical length=%d, want %d", end, wantEnd)
	}
	if len(data)%alignment != 0 {
		if !reflect.DeepEqual(dst.truncates, []int64{int64(len(data))}) || len(dst.events) < 2 || dst.events[len(dst.events)-2] != "truncate" {
			t.Fatalf("tail must be truncated once before close: truncates=%v events=%v", dst.truncates, dst.events)
		}
	} else if len(dst.truncates) != 0 {
		t.Fatalf("aligned data must not be truncated: %v", dst.truncates)
	}
	if dst.closes != 1 || len(dst.events) == 0 || dst.events[len(dst.events)-1] != "close" {
		t.Fatalf("close calls=%d, events=%v", dst.closes, dst.events)
	}
}

func TestFSWriteFileReverseLayout(t *testing.T) {
	for _, chunkSize := range []int{4096, 512 << 10, 4 << 20} {
		for _, size := range []int{0, 1, 4095, 4096, 4097, (512 << 10) - 1, 512 << 10, (512 << 10) + 1, (4 << 20) - 1, 4 << 20, (4 << 20) + 1, (8 << 20) + 17} {
			t.Run(fmt.Sprintf("chunk=%d/size=%d", chunkSize, size), func(t *testing.T) {
				data := fsReverseTestData(size)
				dst := &fsReverseTestFile{}
				n, err := fsWriteFileReverse(dst, fsFragmentReader{bytes.NewReader(data), 317}, chunkSize)
				if err != nil || n != int64(size) {
					t.Fatalf("write=(%d, %v), want (%d, nil)", n, err, size)
				}
				fsCheckReverseLayout(t, dst, data, chunkSize)
			})
		}
	}
}

func TestFSWriteFileReverseRealFile(t *testing.T) {
	data := fsReverseTestData((8 << 20) + 17)
	path := filepath.Join(t.TempDir(), "object")
	dst, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	n, err := fsWriteFileReverse(dst, bytes.NewReader(data), 512<<10)
	if err != nil || n != int64(len(data)) {
		t.Fatalf("write=(%d, %v)", n, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("readback: length=%d err=%v", len(got), err)
	}
	if _, err := dst.WriteAt([]byte{1}, 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("destination must be closed: %v", err)
	}
}

func TestFSWriteFileReverseDataAndEOF(t *testing.T) {
	for _, size := range []int{17, (4 << 20) - 1, 4 << 20, (4 << 20) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := fsReverseTestData(size)
			dst := &fsReverseTestFile{}
			n, err := fsWriteFileReverse(dst, &fsDirectDataErrorReader{data: data, err: io.EOF}, 512<<10)
			if err != nil || n != int64(size) {
				t.Fatalf("data+EOF write=(%d, %v), want (%d, nil)", n, err, size)
			}
			fsCheckReverseLayout(t, dst, data, 512<<10)
		})
	}
}

func TestFSWriteFileReverseWriteAndFinalizeErrors(t *testing.T) {
	writeErr, truncateErr, closeErr := errors.New("write failed"), errors.New("truncate failed"), errors.New("close failed")
	for _, tc := range []struct {
		name       string
		size       int
		file       fsReverseTestFile
		wantBytes  int64
		wantWrites int
		wantErr    error
	}{
		{name: "write error", size: 4 << 20, file: fsReverseTestFile{failAt: 1, writeErr: writeErr, closeErr: closeErr}, wantWrites: 1, wantErr: writeErr},
		{name: "partial write and error", size: 4 << 20, file: fsReverseTestFile{failAt: 2, writeN: 123, writeErr: writeErr}, wantBytes: (512 << 10) + 123, wantWrites: 2, wantErr: writeErr},
		{name: "zero short write", size: 4 << 20, file: fsReverseTestFile{failAt: 1}, wantWrites: 1, wantErr: io.ErrShortWrite},
		{name: "short write before close error", size: 4 << 20, file: fsReverseTestFile{failAt: 2, writeN: 256 << 10, closeErr: closeErr}, wantBytes: 768 << 10, wantWrites: 2, wantErr: io.ErrShortWrite},
		{name: "short padding counts logical bytes only", size: 17, file: fsReverseTestFile{failAt: 1, writeN: 2048}, wantBytes: 17, wantWrites: 1, wantErr: io.ErrShortWrite},
		{name: "tail partial error after full group", size: (4 << 20) + 17, file: fsReverseTestFile{failAt: 9, writeN: 6, writeErr: writeErr, closeErr: closeErr}, wantBytes: (4 << 20) + 6, wantWrites: 9, wantErr: writeErr},
		{name: "truncate before close error", size: 17, file: fsReverseTestFile{truncateErr: truncateErr, closeErr: closeErr}, wantBytes: 17, wantWrites: 1, wantErr: truncateErr},
		{name: "close after truncate", size: 17, file: fsReverseTestFile{closeErr: closeErr}, wantBytes: 17, wantWrites: 1, wantErr: closeErr},
		{name: "close aligned file", size: 4 << 20, file: fsReverseTestFile{closeErr: closeErr}, wantBytes: 4 << 20, wantWrites: 8, wantErr: closeErr},
		{name: "close empty file", file: fsReverseTestFile{closeErr: closeErr}, wantErr: closeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := fsWriteFileReverse(&tc.file, bytes.NewReader(fsReverseTestData(tc.size)), 512<<10)
			if n != tc.wantBytes || !errors.Is(err, tc.wantErr) {
				t.Fatalf("write=(%d, %v), want (%d, %v)", n, err, tc.wantBytes, tc.wantErr)
			}
			if len(tc.file.writes) != tc.wantWrites || tc.file.closes != 1 || tc.file.events[len(tc.file.events)-1] != "close" {
				t.Fatalf("writes=%d want=%d closes=%d events=%v", len(tc.file.writes), tc.wantWrites, tc.file.closes, tc.file.events)
			}
			if tc.file.failAt != 0 && len(tc.file.truncates) != 0 {
				t.Fatalf("failed write must not finalize length: %v", tc.file.truncates)
			}
			if tc.file.truncateErr != nil && !reflect.DeepEqual(tc.file.truncates, []int64{int64(tc.size)}) {
				t.Fatalf("truncate was not attempted with actual length: %v", tc.file.truncates)
			}
		})
	}
}

func TestFSWriteFileReverseReadErrors(t *testing.T) {
	readErr := errors.New("final data failed verification")
	for _, sameRead := range []bool{false, true} {
		for _, size := range []int{17, (4 << 20) - 1, 4 << 20, (4 << 20) + 17} {
			t.Run(fmt.Sprintf("size=%d/data-and-error=%t", size, sameRead), func(t *testing.T) {
				data := fsReverseTestData(size)
				var src io.Reader = io.MultiReader(bytes.NewReader(data), iotest.ErrReader(readErr))
				wantBytes := int64(size / (4 << 20) * (4 << 20))
				if sameRead {
					src = &fsDirectDataErrorReader{data: data, err: readErr}
					// A full final buffer's simultaneous error must not be lost.
					wantBytes = int64((size - 1) / (4 << 20) * (4 << 20))
				}
				dst := &fsReverseTestFile{closeErr: errors.New("secondary close failure")}
				n, err := fsWriteFileReverse(dst, src, 512<<10)
				if n != wantBytes || !errors.Is(err, readErr) {
					t.Fatalf("write=(%d, %v), want (%d, read error)", n, err, wantBytes)
				}
				if len(dst.writes) != int(wantBytes/(512<<10)) || len(dst.truncates) != 0 || dst.closes != 1 {
					t.Fatalf("failed read finalized data: writes=%d truncates=%v closes=%d", len(dst.writes), dst.truncates, dst.closes)
				}
			})
		}
	}
}

func TestFSWriteFileReverseChecksums(t *testing.T) {
	for _, sameRead := range []bool{false, true} {
		for _, size := range []int{(4 << 20) - 1, 4 << 20, (4 << 20) + 1} {
			data := fsReverseTestData(size)
			md5Sum, shaSum := md5.Sum(data), sha256.Sum256(data)
			for _, digest := range []string{"valid", "bad-md5", "bad-sha256"} {
				t.Run(fmt.Sprintf("%s/size=%d/data-and-eof=%t", digest, size, sameRead), func(t *testing.T) {
					var input io.Reader = bytes.NewReader(data)
					if sameRead {
						input = &fsDirectDataErrorReader{data: data, err: io.EOF}
					}
					wantMD5, wantSHA := hex.EncodeToString(md5Sum[:]), hex.EncodeToString(shaSum[:])
					if digest == "bad-md5" {
						wantMD5 = hex.EncodeToString(make([]byte, md5.Size))
					} else if digest == "bad-sha256" {
						wantSHA = hex.EncodeToString(make([]byte, sha256.Size))
					}
					src, err := hash.NewReader(input, int64(size), wantMD5, wantSHA, int64(size))
					if err != nil {
						t.Fatal(err)
					}
					dst := &fsReverseTestFile{}
					if digest != "valid" {
						dst.closeErr = errors.New("secondary close failure")
					}
					n, err := fsWriteFileReverse(dst, src, 512<<10)
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
						if err != nil || n != int64(size) {
							t.Fatalf("valid data rejected: n=%d err=%v", n, err)
						}
						fsCheckReverseLayout(t, dst, data, 512<<10)
					}
					if dst.closes != 1 || (digest != "valid" && len(dst.truncates) != 0) {
						t.Fatalf("invalid finalization: closes=%d truncates=%v", dst.closes, dst.truncates)
					}
				})
			}
		}
	}
}

func TestFSWriteFileReverseInvalidChunkSize(t *testing.T) {
	for _, chunkSize := range []int{-1, 0, 1, 2048, 4095, 4097, 12288, (4 << 20) + 4096, 8 << 20} {
		t.Run(fmt.Sprint(chunkSize), func(t *testing.T) {
			dst := &fsReverseTestFile{closeErr: errors.New("secondary close failure")}
			// A valid implementation rejects the size before reading anything.
			src := bytes.NewReader([]byte{1})
			n, err := fsWriteFileReverse(dst, src, chunkSize)
			if n != 0 || !errors.Is(err, errInvalidArgument) || src.Len() != 1 {
				t.Fatalf("invalid chunk write=(%d, %v), unread bytes=%d", n, err, src.Len())
			}
			if len(dst.writes) != 0 || len(dst.truncates) != 0 || dst.closes != 1 {
				t.Fatalf("invalid chunk touched destination: writes=%d truncates=%v closes=%d", len(dst.writes), dst.truncates, dst.closes)
			}
		})
	}
}

func TestFSWriteFileReverseNoProgress(t *testing.T) {
	dst := &fsReverseTestFile{}
	n, err := fsWriteFileReverse(dst, fsDirectNoProgressReader{}, 512<<10)
	if n != 0 || !errors.Is(err, io.ErrNoProgress) || len(dst.writes) != 0 || len(dst.truncates) != 0 || dst.closes != 1 {
		t.Fatalf("no-progress write=(%d, %v), writes=%d truncates=%v closes=%d", n, err, len(dst.writes), dst.truncates, dst.closes)
	}
}

func TestFSWriteFileReverseConcurrentBuffers(t *testing.T) {
	for _, chunkSize := range []int{4096, 256 << 10, 512 << 10, 4 << 20} {
		t.Run(fmt.Sprint(chunkSize), func(t *testing.T) {
			t.Parallel()
			data := fsReverseTestData((4 << 20) + chunkSize + 17)
			for i := range data {
				data[i] ^= byte(chunkSize >> 12)
			}
			dst := &fsReverseTestFile{}
			n, err := fsWriteFileReverse(dst, fsFragmentReader{bytes.NewReader(data), 4093}, chunkSize)
			if err != nil || n != int64(len(data)) {
				t.Fatalf("write=(%d, %v)", n, err)
			}
			fsCheckReverseLayout(t, dst, data, chunkSize)
		})
	}
}
