//go:build linux
// +build linux

// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/jackblack369/minio/internal/disk"
	"github.com/jackblack369/minio/internal/hash"
)

const fsDirectIOAlignment = 4096

type fsDirectIOBuffer struct{ data []byte }

var fsDirectIOBufferPool = sync.Pool{
	New: func() interface{} {
		return &fsDirectIOBuffer{data: disk.AlignedBlock(fsWriteBufferSize)}
	},
}

func fsCreateFileOptimized(filePath string, flags int, src io.Reader) (int64, error) {
	var f *os.File
	var err error
	if globalFSDirectIO {
		f, err = disk.OpenFileDirectIO(filePath, flags, 0o666)
	} else {
		f, err = os.OpenFile(filePath, flags, 0o666)
	}
	if err != nil {
		// An explicitly requested mode must not silently fall back to buffered I/O.
		// The generic mapping treats EINVAL as a missing path, which is misleading
		// when the filesystem is rejecting O_DIRECT on an otherwise valid path.
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.ENOSYS) {
			return 0, errUnsupportedDisk
		}
		return 0, osErrToFileErr(err)
	}
	owned := true
	defer func() {
		if owned {
			// Covers a reader panic before a writer takes file ownership.
			_ = f.Close()
		}
	}()
	if globalFSParallelChecksum {
		if reader, ok := src.(*hash.Reader); ok && reader.Size() == fsWriteBufferSize {
			written, checksumErr, handled := func() (int64, error, bool) {
				block := fsDirectIOBufferPool.Get().(*fsDirectIOBuffer)
				// Keep the buffer owned until both checksum and all file work
				// have completed, including failure and panic unwinding.
				defer fsDirectIOBufferPool.Put(block)
				n, finalize, readErr := reader.ReadAllDeferred(block.data)
				if errors.Is(readErr, hash.ErrDeferredChecksumUnsupported) {
					return 0, nil, false
				}
				if readErr != nil {
					return 0, readErr, true
				}
				owned = false
				nw, writeErr := fsWriteFileParallelChecksum(f, block.data[:n], finalize, globalFSReverseWriteSize)
				return nw, writeErr, true
			}()
			if handled {
				return written, checksumErr
			}
		}
	}
	owned = false
	if globalFSReverseWriteSize != 0 {
		return fsWriteFileReverse(f, src, globalFSReverseWriteSize)
	}
	return fsWriteFileDirectIO(f, src)
}

type fsDirectIOFile interface {
	io.WriteCloser
	Truncate(int64) error
}

// fsWriteFileDirectIO owns dst. Padding exists only in the unpublished temporary
// file: truncate it to the logical length before Close and the caller's rename.
// O_DIRECT does not replace O_SYNC or the filesystem's close/flush semantics.
func fsWriteFileDirectIO(dst fsDirectIOFile, src io.Reader) (written int64, err error) {
	defer func() {
		if closeErr := dst.Close(); err == nil {
			err = closeErr
		}
	}()
	block := fsDirectIOBufferPool.Get().(*fsDirectIOBuffer)
	defer fsDirectIOBufferPool.Put(block)
	buf := block.data
	needsTruncate := false
	for {
		n, readErr := fsReadDirectIOBlock(src, buf)
		if readErr != nil && readErr != io.EOF {
			return written, readErr
		}
		if n > 0 {
			aligned := (n + fsDirectIOAlignment - 1) / fsDirectIOAlignment * fsDirectIOAlignment
			// A pooled buffer can contain bytes from another object. Never leak them.
			clear(buf[n:aligned])
			nw, writeErr := dst.Write(buf[:aligned])
			written += int64(min(nw, n))
			if writeErr != nil {
				return written, writeErr
			}
			if nw != aligned {
				return written, io.ErrShortWrite
			}
			needsTruncate = needsTruncate || aligned != n
		}
		if readErr == io.EOF {
			break
		}
	}
	if needsTruncate {
		err = dst.Truncate(written)
	}
	return written, err
}

// Unlike io.ReadFull, preserve a checksum error returned alongside the final
// bytes even when those bytes exactly fill buf. Continue through EOF so an
// exact-sized object cannot skip the hash reader's final verification.
func fsReadDirectIOBlock(src io.Reader, buf []byte) (int, error) {
	n, emptyReads := 0, 0
	for n < len(buf) {
		nr, err := src.Read(buf[n:])
		n += nr
		if err != nil {
			return n, err
		}
		if nr == 0 {
			emptyReads++
			if emptyReads >= 100 {
				return n, io.ErrNoProgress
			}
		} else {
			emptyReads = 0
		}
	}
	return n, nil
}
