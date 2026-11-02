//go:build linux
// +build linux

// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"errors"
	"io"
)

type fsParallelChecksumFile interface {
	io.Writer
	io.WriterAt
	io.Closer
}

type fsChecksumResult struct {
	err       error
	panic     interface{}
	completed bool
}

var errFSChecksumVerificationAborted = errors.New("checksum verification did not complete")

// fsWriteFileParallelChecksum owns dst and borrows immutable data. It does not
// publish a file: the caller may rename only after this function succeeds.
// Every exit joins verification and closes dst before returning buffer ownership.
func fsWriteFileParallelChecksum(dst fsParallelChecksumFile, data []byte, verify func() error, chunkSize int) (written int64, err error) {
	if len(data) != fsWriteBufferSize || verify == nil || (chunkSize != 0 &&
		(chunkSize < fsDirectIOAlignment || chunkSize > fsWriteBufferSize || chunkSize&(chunkSize-1) != 0)) {
		_ = dst.Close()
		return 0, errInvalidArgument
	}

	verified := make(chan fsChecksumResult, 1)
	go func() {
		var result fsChecksumResult
		defer func() {
			result.panic = recover()
			if !result.completed && result.panic == nil {
				// Fail closed even for Goexit or legacy panic(nil) behavior.
				result.err = errFSChecksumVerificationAborted
			}
			verified <- result
		}()
		result.err = verify()
		result.completed = true
	}()

	// Registered before Close so verification is joined even if Write or Close
	// panics. If verification and file work both fail, prefer verification.
	defer func() {
		result := <-verified
		if result.panic != nil {
			panic(result.panic)
		}
		if result.err != nil {
			err = result.err
		}
	}()
	defer func() {
		if closeErr := dst.Close(); err == nil {
			err = closeErr
		}
	}()

	if chunkSize == 0 {
		n, writeErr := dst.Write(data)
		if writeErr == nil && n != len(data) {
			writeErr = io.ErrShortWrite
		}
		return int64(n), writeErr
	}
	for offset := len(data) - chunkSize; offset >= 0; offset -= chunkSize {
		n, writeErr := dst.WriteAt(data[offset:offset+chunkSize], int64(offset))
		written += int64(n)
		if writeErr != nil {
			return written, writeErr
		}
		if n != chunkSize {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}
