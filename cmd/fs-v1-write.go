// Copyright (c) 2026 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"bufio"
	"io"
	"sync"
)

const fsWriteBufferSize = 4 << 20

var fsWriteBufferPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, fsWriteBufferSize)
	},
}

// fsWriteFile owns and closes dst. Aggregate short HTTP reads before writing to
// the filesystem, while reading through EOF so checksum readers can verify the
// complete object before it is published.
func fsWriteFile(dst io.WriteCloser, src io.Reader) (written int64, err error) {
	defer func() {
		if closeErr := dst.Close(); err == nil {
			err = closeErr
		}
	}()

	buffer := fsWriteBufferPool.Get().(*bufio.Writer)
	// Hide ReadFrom: otherwise bufio.Writer delegates to os.File.ReadFrom,
	// which falls back to unbuffered 32 KiB writes for hash.Reader sources.
	buffer.Reset(struct{ io.Writer }{dst})
	defer func() {
		buffer.Reset(nil)
		fsWriteBufferPool.Put(buffer)
	}()

	n, err := buffer.ReadFrom(src)
	if err == nil {
		err = buffer.Flush()
	}
	return n - int64(buffer.Buffered()), err
}
