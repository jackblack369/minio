// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"sync"

	"github.com/jackblack369/minio/internal/lock"
)

type fsMetadataPreparation struct {
	file  *lock.LockedFile
	fresh bool
	err   error
}

// prepareObjectMetadata acquires the existing cross-process lock without
// changing the metadata contents. The caller owns the returned locked file.
func (fs *FSObjects) prepareObjectMetadata(path string) fsMetadataPreparation {
	f, err := fs.rwPool.Write(path)
	if err == nil {
		return fsMetadataPreparation{file: f}
	}
	f, err = fs.rwPool.Create(path)
	return fsMetadataPreparation{file: f, fresh: err == nil, err: err}
}

// fsStartMetadataPreparation returns an await function usable both on the
// normal path and during deferred cleanup. Await never releases the file lock.
func fsStartMetadataPreparation(prepare func() fsMetadataPreparation) func() fsMetadataPreparation {
	ready := make(chan fsMetadataPreparation, 1)
	go func() {
		ready <- prepare()
	}()
	var once sync.Once
	var result fsMetadataPreparation
	return func() fsMetadataPreparation {
		once.Do(func() { result = <-ready })
		return result
	}
}
