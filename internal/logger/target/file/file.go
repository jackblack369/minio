// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package file

import (
	"fmt"
	"log/slog"

	"github.com/jackblack369/minio/internal/logger/message/log"
	"github.com/jackblack369/minio/internal/logger/target/types"
	"github.com/natefinch/lumberjack"
	"github.com/robfig/cron/v3"
)

// Config holds the configuration for file logging
type Config struct {
	Enable     bool   `json:"enable"`
	FilePath   string `json:"file_path"`
	RetainDays int    `json:"retain_days"`
	MaxSizeMB  int    `json:"max_size_mb"`
	MaxBackups int    `json:"max_backups"`
	Compress   bool   `json:"compress"`
	JSONFormat bool   `json:"json_format"`
}

// Target implements loggerTarget to send log entries to a file with rotation
type Target struct {
	config     Config
	logger     *slog.Logger
	lumberjack *lumberjack.Logger
	cron       *cron.Cron
}

// New creates a new file target
func New(config Config) *Target {
	return &Target{
		config: config,
	}
}

// Validate validates the file target configuration
func (t *Target) Validate() error {
	if !t.config.Enable {
		return fmt.Errorf("file target is not enabled")
	}
	if t.config.FilePath == "" {
		return fmt.Errorf("file path is required")
	}
	if t.config.MaxSizeMB <= 0 {
		t.config.MaxSizeMB = 100 // default 100MB
	}
	if t.config.MaxBackups <= 0 {
		t.config.MaxBackups = 10 // default 10 backups
	}
	if t.config.RetainDays <= 0 {
		t.config.RetainDays = 30 // default 30 days
	}
	return nil
}

// Init initializes the file target
func (t *Target) Init() error {
	if err := t.Validate(); err != nil {
		return err
	}

	// Create lumberjack logger for file rotation
	t.lumberjack = &lumberjack.Logger{
		Filename:   t.config.FilePath,
		MaxSize:    t.config.MaxSizeMB,
		MaxBackups: t.config.MaxBackups,
		MaxAge:     t.config.RetainDays,
		Compress:   t.config.Compress,
		LocalTime:  true,
	}

	// Create slog handler
	var handler slog.Handler
	if t.config.JSONFormat {
		handler = slog.NewJSONHandler(t.lumberjack, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	} else {
		handler = slog.NewTextHandler(t.lumberjack, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	}

	t.logger = slog.New(handler)

	// Schedule daily rotation
	t.scheduleDailyRotate()

	return nil
}

// scheduleDailyRotate schedules daily log rotation
func (t *Target) scheduleDailyRotate() {
	t.cron = cron.New()
	t.cron.AddFunc("0 0 * * *", func() {
		_ = t.lumberjack.Rotate()
	})
	t.cron.Start()
}

// String returns the name of the target
func (t *Target) String() string {
	return "file"
}

// Endpoint returns the file path
func (t *Target) Endpoint() string {
	return t.config.FilePath
}

// Type returns the target type
func (t *Target) Type() types.TargetType {
	return types.TargetFile
}

// Cancel cancels the file target
func (t *Target) Cancel() {
	if t.cron != nil {
		t.cron.Stop()
	}
	if t.lumberjack != nil {
		t.lumberjack.Close()
	}
}

// Send sends a log entry to the file
func (t *Target) Send(entry interface{}, logKind string) error {
	logEntry, ok := entry.(log.Entry)
	if !ok {
		return fmt.Errorf("unexpected log entry structure %#v", entry)
	}

	// Convert MinIO log entry to slog format
	attrs := []slog.Attr{
		slog.String("deployment_id", logEntry.DeploymentID),
		slog.String("level", logEntry.Level),
		slog.String("log_kind", logEntry.LogKind),
		slog.String("remote_host", logEntry.RemoteHost),
		slog.String("host", logEntry.Host),
		slog.String("request_id", logEntry.RequestID),
		slog.String("user_agent", logEntry.UserAgent),
		slog.Time("time", logEntry.Time),
	}

	// Add API information if available
	if logEntry.API != nil {
		attrs = append(attrs, slog.Group("api",
			slog.String("name", logEntry.API.Name),
			slog.String("bucket", logEntry.API.Args.Bucket),
			slog.String("object", logEntry.API.Args.Object),
			slog.String("version_id", logEntry.API.Args.VersionID),
		))
	}

	// Add trace information if available
	if logEntry.Trace != nil {
		attrs = append(attrs, slog.Group("trace",
			slog.String("message", logEntry.Trace.Message),
			slog.Any("source", logEntry.Trace.Source),
			slog.Any("variables", logEntry.Trace.Variables),
		))
	}

	// Log the entry
	t.logger.LogAttrs(nil, slog.LevelInfo, "MinIO Log Entry", attrs...)

	return nil
}

// GetCurrentLogFile returns the current log file path
func (t *Target) GetCurrentLogFile() string {
	if t.lumberjack != nil {
		return t.lumberjack.Filename
	}
	return ""
}

// Rotate manually rotates the log file
func (t *Target) Rotate() error {
	if t.lumberjack != nil {
		return t.lumberjack.Rotate()
	}
	return nil
}
