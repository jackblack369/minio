//go:build linux
// +build linux

// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import "io"

type fsReverseWriteFile interface {
	io.WriterAt
	io.Closer
	Truncate(int64) error
}

// fsWriteFileReverse owns dst. Each bounded 4MiB group is written at descending
// offsets to allow append-only filesystem slice writers to flush several slices
// concurrently. All writes and Close still finish before the caller publishes
// the object. Never submit the fragments concurrently: the filesystem may sort
// a batch back into ascending order and merge it into one slice.
func fsWriteFileReverse(dst fsReverseWriteFile, src io.Reader, chunkSize int) (written int64, err error) {
	defer func() {
		if closeErr := dst.Close(); err == nil {
			err = closeErr
		}
	}()
	if chunkSize < fsDirectIOAlignment || chunkSize > fsWriteBufferSize || chunkSize&(chunkSize-1) != 0 {
		return 0, errInvalidArgument
	}
	block := fsDirectIOBufferPool.Get().(*fsDirectIOBuffer)
	defer fsDirectIOBufferPool.Put(block)
	buf := block.data
	var base int64
	needsTruncate := false
	for {
		n, readErr := fsReadDirectIOBlock(src, buf)
		if readErr != nil && readErr != io.EOF {
			return written, readErr
		}
		if n > 0 {
			aligned := (n + fsDirectIOAlignment - 1) / fsDirectIOAlignment * fsDirectIOAlignment
			clear(buf[n:aligned])
			for offset := (aligned - 1) / chunkSize * chunkSize; offset >= 0; offset -= chunkSize {
				end := min(offset+chunkSize, aligned)
				nw, writeErr := dst.WriteAt(buf[offset:end], base+int64(offset))
				written += int64(min(nw, n-offset))
				if writeErr != nil {
					return written, writeErr
				}
				if nw != end-offset {
					return written, io.ErrShortWrite
				}
			}
			base += int64(n)
			needsTruncate = needsTruncate || aligned != n
		}
		if readErr == io.EOF {
			break
		}
	}
	if needsTruncate {
		err = dst.Truncate(base)
	}
	return written, err
}
