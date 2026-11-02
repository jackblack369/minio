//go:build !linux
// +build !linux

// Copyright (c) 2026 MinIO, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import "io"

// Keep the opt-in mode explicit rather than presenting buffered I/O as direct.
func fsCreateFileOptimized(filePath string, flags int, src io.Reader) (int64, error) {
	return 0, errUnsupportedDisk
}
