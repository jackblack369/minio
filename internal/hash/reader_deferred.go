// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package hash

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/jackblack369/minio/internal/etag"
)

// ErrDeferredChecksumUnsupported means ReadAllDeferred did not consume or
// change the reader. The caller may use the normal Read path instead.
var ErrDeferredChecksumUnsupported = errors.New("hash: deferred checksum unsupported for this reader")

// ErrDeferredChecksumPending prevents ordinary reads from bypassing a pending
// verification. ETag returns nil until verification finishes.
var ErrDeferredChecksumPending = errors.New("hash: deferred checksum verification pending")

const maxDeferredChecksumSize = 4 << 20

type deferredChecksum struct {
	once sync.Once
	done atomic.Bool
	tag  etag.ETag
	err  error
}

// ReadAllDeferred reads this reader's entire declared body into buf without
// calculating its MD5 or SHA256. It is supported only before the first read,
// for known lengths up to 4MiB, and when NewReader owns the ETag calculation.
// The original length limiter and source reader remain in use, including any
// authentication or framing performed by that source.
//
// On success the caller must invoke finalize and check its result before
// publishing data or accepting ETag. The caller owns buf and must keep buf[:n]
// immutable until finalize returns. It may concurrently write those bytes to
// an unpublished temporary file. No extra payload copy is allocated here.
//
// finalize is safe to call more than once, including concurrently; it performs
// the checksum work once. ReadAllDeferred itself must have sole ownership of
// the reader. After it returns, ETag may be read concurrently with finalize
// (nil while pending), but the caller must join finalize before returning buf
// to a pool. These rules do not make general concurrent Read calls supported.
//
// ErrDeferredChecksumUnsupported is the only error that allows fallback to
// normal reading. Other errors may follow source consumption; a short body
// returns io.ErrUnexpectedEOF unless its EOF checksum verification fails first.
// That failure path verifies synchronously to preserve normal error priority.
// An error returned with final bytes is kept.
func (r *Reader) ReadAllDeferred(buf []byte) (n int, finalize func() error, err error) {
	if r.deferredSrc == nil || r.deferred != nil || r.readStarted || r.bytesRead != 0 ||
		r.size < 0 || r.size > maxDeferredChecksumSize || int64(len(buf)) < r.size {
		return 0, nil, ErrDeferredChecksumUnsupported
	}

	state := &deferredChecksum{}
	r.deferred = state
	finishReadError := func(readErr error) (int, func() error, error) {
		if verification, ok := readErr.(etag.VerifyError); ok {
			readErr = BadDigest{ExpectedMD5: verification.Expected.String(), CalculatedMD5: verification.Computed.String()}
		}
		state.err = readErr
		state.done.Store(true)
		return n, nil, readErr
	}
	// Read through the length limiter's EOF even when the buffer is exactly
	// full. A source error returned with its final bytes must not disappear.
	var probe [1]byte
	for emptyReads := 0; ; {
		target := buf[n:int(r.size)]
		if len(target) == 0 {
			target = probe[:]
		}
		nr, readErr := r.deferredSrc.Read(target)
		if nr > 0 {
			n += nr
			r.bytesRead += int64(nr)
			emptyReads = 0
		} else if readErr == nil {
			emptyReads++
			if emptyReads >= 100 {
				return finishReadError(io.ErrNoProgress)
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				return finishReadError(readErr)
			}
			if int64(n) != r.size {
				// Ordinary Read verifies at EOF before the caller diagnoses an
				// incomplete body. Preserve digest error priority on this failure
				// path; no parallel write will be started for a short body.
				state.err = r.finalizeDeferredChecksum(buf[:n], state)
				if state.err != nil {
					return finishReadError(state.err)
				}
				return finishReadError(io.ErrUnexpectedEOF)
			}
			break
		}
	}

	return n, func() error {
		state.once.Do(func() {
			// Publish all result fields with the release store only after both
			// checksum calculation and verification have finished.
			defer state.done.Store(true)
			state.err = r.finalizeDeferredChecksum(buf[:n], state)
		})
		return state.err
	}, nil
}

func (r *Reader) finalizeDeferredChecksum(buf []byte, state *deferredChecksum) error {
	sum := md5.Sum(buf)
	state.tag = etag.ETag(sum[:])
	if len(r.checksum) != 0 && !etag.Equal(r.checksum, state.tag) {
		return BadDigest{ExpectedMD5: r.checksum.String(), CalculatedMD5: state.tag.String()}
	}
	if len(r.contentSHA256) != 0 {
		sha := newSHA256()
		_, _ = sha.Write(buf)
		actual := sha.Sum(nil)
		if !bytes.Equal(r.contentSHA256, actual) {
			return SHA256Mismatch{ExpectedSHA256: hex.EncodeToString(r.contentSHA256), CalculatedSHA256: hex.EncodeToString(actual)}
		}
	}
	return nil
}
