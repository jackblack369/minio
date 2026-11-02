// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package hash

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackblack369/minio/internal/etag"
)

func deferredDigests(data []byte) (string, string) {
	md5sum, shasum := md5.Sum(data), sha256.Sum256(data)
	return hex.EncodeToString(md5sum[:]), hex.EncodeToString(shasum[:])
}

func TestReadAllDeferredVerification(t *testing.T) {
	for _, size := range []int{0, 1, 4095, 4096, 4097, maxDeferredChecksumSize} {
		payload := bytes.Repeat([]byte{0x97}, size)
		md5hex, shahex := deferredDigests(payload)
		for _, check := range []struct {
			name, md5, sha string
			want           error
		}{
			{name: "etag only"},
			{name: "both valid", md5: md5hex, sha: shahex},
			{name: "bad md5", md5: "00", sha: shahex, want: BadDigest{}},
			{name: "bad sha", md5: md5hex, sha: "00", want: SHA256Mismatch{}},
			{name: "both bad md5 first", md5: "00", sha: "00", want: BadDigest{}},
		} {
			t.Run(check.name+"/"+strconv.Itoa(size), func(t *testing.T) {
				r, err := NewReader(bytes.NewReader(payload), int64(size), check.md5, check.sha, int64(size))
				if err != nil {
					t.Fatal(err)
				}
				buf := bytes.Repeat([]byte{0x11}, size+32)
				n, finalize, err := r.ReadAllDeferred(buf)
				if err != nil || n != size || finalize == nil {
					t.Fatalf("read: n=%d finalizer=%v err=%v", n, finalize != nil, err)
				}
				if !bytes.Equal(buf[:n], payload) || !bytes.Equal(buf[n:], bytes.Repeat([]byte{0x11}, 32)) {
					t.Fatal("body differs or bytes outside body changed")
				}
				if r.ETag() != nil || r.MD5Current() != nil {
					t.Fatal("pending checksum was exposed as valid ETag")
				}
				if _, err := r.Read(make([]byte, 1)); !errors.Is(err, ErrDeferredChecksumPending) {
					t.Fatalf("pending ordinary read: %v", err)
				}
				err = finalize()
				switch check.want.(type) {
				case nil:
					if err != nil {
						t.Fatal(err)
					}
				case BadDigest:
					var digest BadDigest
					if !errors.As(err, &digest) || digest.CalculatedMD5 != md5hex {
						t.Fatalf("want BadDigest, got %v", err)
					}
				case SHA256Mismatch:
					var mismatch SHA256Mismatch
					if !errors.As(err, &mismatch) || mismatch.CalculatedSHA256 != shahex {
						t.Fatalf("want SHA256Mismatch, got %v", err)
					}
				}
				if again := finalize(); !errors.Is(again, err) {
					t.Fatalf("non-idempotent verification: %v / %v", err, again)
				}
				if got := hex.EncodeToString(r.MD5Current()); got != md5hex {
					t.Fatalf("ETag mismatch %s / %s", got, md5hex)
				}
				_, readErr := r.Read(make([]byte, 1))
				if err == nil && readErr != io.EOF || err != nil && !errors.Is(readErr, err) {
					t.Fatalf("post-finalize read: %v", readErr)
				}
			})
		}
	}
}

type deferredCountingReader struct {
	reader io.Reader
	calls  int
}

func (r *deferredCountingReader) Read(p []byte) (int, error) {
	r.calls++
	return r.reader.Read(p)
}

type deferredForeignTagger struct{ *deferredCountingReader }

func (r deferredForeignTagger) ETag() etag.ETag { return nil }

func TestReadAllDeferredUnsupportedDoesNotConsume(t *testing.T) {
	for _, name := range []string{"unknown length", "oversize", "small buffer", "foreign tagger", "wrapped tagger", "already read", "empty read", "second deferred call", "unknown then merged", "known then merged"} {
		t.Run(name, func(t *testing.T) {
			counter := &deferredCountingReader{reader: bytes.NewBufferString("abcd")}
			var source io.Reader = counter
			var size int64 = 4
			buf := make([]byte, 4)
			switch name {
			case "unknown length", "unknown then merged":
				size = -1
			case "oversize":
				size = maxDeferredChecksumSize + 1
			case "small buffer":
				buf = buf[:3]
			case "foreign tagger":
				source = deferredForeignTagger{counter}
			case "wrapped tagger":
				source = etag.Wrap(counter, deferredForeignTagger{counter})
			}
			r, err := NewReader(source, size, "", "", size)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "already read":
				if _, err := r.Read(make([]byte, 1)); err != nil {
					t.Fatal(err)
				}
			case "empty read":
				if _, err := r.Read(nil); err != nil {
					t.Fatal(err)
				}
			case "second deferred call":
				_, finalize, err := r.ReadAllDeferred(buf)
				if err != nil {
					t.Fatal(err)
				}
				defer finalize()
			case "unknown then merged", "known then merged":
				r, err = NewReader(r, 4, "", "", 4)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := counter.calls
			n, finalize, err := r.ReadAllDeferred(buf)
			if n != 0 || finalize != nil || !errors.Is(err, ErrDeferredChecksumUnsupported) {
				t.Fatalf("unexpected result: n=%d finalize=%v err=%v", n, finalize != nil, err)
			}
			if before != counter.calls {
				t.Fatal("unsupported reader was consumed")
			}
		})
	}
}

func TestReadAllDeferredRetainsEarlierMergedVerification(t *testing.T) {
	r, _ := NewReader(bytes.NewBufferString("abcd"), 4, "00", "", 4)
	r, err := NewReader(r, 4, "", "", 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.ReadAllDeferred(make([]byte, 4)); !errors.Is(err, ErrDeferredChecksumUnsupported) {
		t.Fatalf("merged verification must fall back: %v", err)
	}
	_, err = io.Copy(io.Discard, r)
	var digest BadDigest
	if !errors.As(err, &digest) {
		t.Fatalf("fallback lost original MD5 verification: %v", err)
	}
}

func TestReadAllDeferredShortBodyDigestPriority(t *testing.T) {
	for _, tc := range []struct{ md5, sha string }{{md5: "00", sha: "00"}, {sha: "00"}} {
		r, _ := NewReader(bytes.NewBufferString("abc"), 4, tc.md5, tc.sha, 4)
		_, _, got := r.ReadAllDeferred(make([]byte, 4))
		normal, _ := NewReader(bytes.NewBufferString("abc"), 4, tc.md5, tc.sha, 4)
		_, want := io.Copy(io.Discard, normal)
		if got == nil || want == nil || got.Error() != want.Error() {
			t.Fatalf("short-body priority changed: deferred=%v normal=%v", got, want)
		}
	}
}

type deferredFinalErrorReader struct {
	err  error
	done bool
}

func (r *deferredFinalErrorReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	return copy(p, "abcd"), r.err
}

type deferredEmptyReader struct{}

func (deferredEmptyReader) Read([]byte) (int, error) { return 0, nil }

func TestReadAllDeferredSourceBoundaries(t *testing.T) {
	readErr := errors.New("source failed with final bytes")
	for _, tc := range []struct {
		name   string
		source io.Reader
		wantN  int
		want   error
	}{
		{name: "short body", source: bytes.NewBufferString("abc"), wantN: 3, want: io.ErrUnexpectedEOF},
		{name: "final bytes with error", source: &deferredFinalErrorReader{err: readErr}, wantN: 4, want: readErr},
		{name: "final bytes with eof", source: &deferredFinalErrorReader{err: io.EOF}, wantN: 4},
		{name: "exact body", source: bytes.NewBufferString("abcd"), wantN: 4},
		{name: "no progress", source: deferredEmptyReader{}, want: io.ErrNoProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(tc.source, 4, "", "", 4)
			if err != nil {
				t.Fatal(err)
			}
			n, finalize, err := r.ReadAllDeferred(make([]byte, 4))
			if n != tc.wantN || !errors.Is(err, tc.want) {
				t.Fatalf("n=%d err=%v", n, err)
			}
			if err != nil {
				if finalize != nil {
					t.Fatal("failed source returned usable finalizer")
				}
				return
			}
			if err := finalize(); err != nil {
				t.Fatal(err)
			}
		})
	}
	// The HTTP/content limiter belongs to this reader. A subsequent request
	// or trailing data in the source must not be consumed to check EOF.
	source := bytes.NewBufferString("abcd-next-request")
	r, _ := NewReader(source, 4, "", "", 4)
	_, finalize, err := r.ReadAllDeferred(make([]byte, 4))
	if err != nil || finalize() != nil || source.String() != "-next-request" {
		t.Fatalf("length limiter not retained: err=%v tail=%q", err, source.String())
	}
}

func TestReadAllDeferredConcurrentFinalize(t *testing.T) {
	payload := bytes.Repeat([]byte("independent-immutable-buffer"), 65536)
	md5hex, shahex := deferredDigests(payload)
	r, err := NewReader(bytes.NewReader(payload), int64(len(payload)), md5hex, shahex, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(payload))
	_, finalize, err := r.ReadAllDeferred(buf)
	if err != nil {
		t.Fatal(err)
	}
	var done atomic.Bool
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := finalize(); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		for !done.Load() {
			if tag := r.ETag(); tag != nil && tag.String() != md5hex {
				t.Error("partially published ETag")
			}
		}
	}()
	if err := finalize(); err != nil {
		t.Fatal(err)
	}
	done.Store(true)
	group.Wait()
	if !bytes.Equal(buf, payload) || hex.EncodeToString(r.MD5Current()) != md5hex {
		t.Fatal("payload changed or ETag differs after finalization")
	}
	if _, err := NewReader(r, int64(len(payload)), "", "", int64(len(payload))); err == nil {
		t.Fatal("consumed deferred reader was merged")
	}
}
