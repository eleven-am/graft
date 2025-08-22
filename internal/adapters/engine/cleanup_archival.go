package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ArchiveStorage interface {
	Store(ctx context.Context, key string, data []byte) error
	Retrieve(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

type WorkflowArchive struct {
	WorkflowID  string                 `json:"workflow_id"`
	ArchivedAt  time.Time              `json:"archived_at"`
	State       interface{}            `json:"state,omitempty"`
	QueueItems  []ports.QueueItem      `json:"queue_items,omitempty"`
	Claims      []interface{}          `json:"claims,omitempty"`
	AuditLogs   []interface{}          `json:"audit_logs,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	Version     string                 `json:"version"`
	Compression string                 `json:"compression,omitempty"`
}

type ArchivalManager struct {
	storage     ArchiveStorage
	logger      *slog.Logger
	compression bool
}

func NewArchivalManager(storage ArchiveStorage, logger *slog.Logger, compression bool) *ArchivalManager {
	return &ArchivalManager{
		storage:     storage,
		logger:      logger.With("component", "archival_manager"),
		compression: compression,
	}
}

func (am *ArchivalManager) ArchiveWorkflow(ctx context.Context, workflowID string, data WorkflowArchive) error {
	am.logger.Info("archiving workflow",
		"workflow_id", workflowID,
		"compression", am.compression)

	data.WorkflowID = workflowID
	data.ArchivedAt = time.Now()
	data.Version = "1.0"

	if am.compression {
		data.Compression = "gzip"
	}

	serialized, err := json.Marshal(data)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to serialize workflow archive",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	if am.compression {
		compressed, err := am.compressData(serialized)
		if err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to compress archive data",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"error":       err.Error(),
				},
			}
		}
		serialized = compressed
	}

	archiveKey := fmt.Sprintf("workflow-archive:%s:%d", workflowID, data.ArchivedAt.Unix())
	if err := am.storage.Store(ctx, archiveKey, serialized); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to store workflow archive",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"archive_key": archiveKey,
				"error":       err.Error(),
			},
		}
	}

	am.logger.Info("workflow archived successfully",
		"workflow_id", workflowID,
		"archive_key", archiveKey,
		"size_bytes", len(serialized))

	return nil
}

func (am *ArchivalManager) RetrieveWorkflow(ctx context.Context, workflowID string, timestamp *time.Time) (*WorkflowArchive, error) {
	var archiveKey string

	if timestamp != nil {
		archiveKey = fmt.Sprintf("workflow-archive:%s:%d", workflowID, timestamp.Unix())
	} else {
		keys, err := am.storage.List(ctx, fmt.Sprintf("workflow-archive:%s:", workflowID))
		if err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to list archives for workflow",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"error":       err.Error(),
				},
			}
		}
		if len(keys) == 0 {
			return nil, domain.Error{
				Type:    domain.ErrorTypeNotFound,
				Message: "no archives found for workflow",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
				},
			}
		}
		archiveKey = keys[len(keys)-1]
	}

	data, err := am.storage.Retrieve(ctx, archiveKey)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to retrieve archive",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"archive_key": archiveKey,
				"error":       err.Error(),
			},
		}
	}

	var archive WorkflowArchive
	if err := am.deserializeArchive(data, &archive); err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to deserialize archive",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"archive_key": archiveKey,
				"error":       err.Error(),
			},
		}
	}

	am.logger.Info("workflow archive retrieved",
		"workflow_id", workflowID,
		"archive_key", archiveKey,
		"archived_at", archive.ArchivedAt)

	return &archive, nil
}

func (am *ArchivalManager) ListWorkflowArchives(ctx context.Context, workflowID string) ([]string, error) {
	prefix := fmt.Sprintf("workflow-archive:%s:", workflowID)
	return am.storage.List(ctx, prefix)
}

func (am *ArchivalManager) DeleteArchive(ctx context.Context, workflowID string, timestamp time.Time) error {
	archiveKey := fmt.Sprintf("workflow-archive:%s:%d", workflowID, timestamp.Unix())

	if err := am.storage.Delete(ctx, archiveKey); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to delete archive",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"archive_key": archiveKey,
				"error":       err.Error(),
			},
		}
	}

	am.logger.Info("workflow archive deleted",
		"workflow_id", workflowID,
		"archive_key", archiveKey)

	return nil
}

func (am *ArchivalManager) RestoreWorkflow(ctx context.Context, workflowID string, timestamp *time.Time, storagePort ports.StoragePort) error {
	archive, err := am.RetrieveWorkflow(ctx, workflowID, timestamp)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to retrieve archive for restoration",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	am.logger.Info("restoring workflow from archive",
		"workflow_id", workflowID,
		"archived_at", archive.ArchivedAt)

	if archive.State != nil {
		stateKey := fmt.Sprintf("workflow:state:%s", workflowID)
		stateData, err := json.Marshal(archive.State)
		if err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to marshal state for restoration",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"error":       err.Error(),
				},
			}
		}

		if err := storagePort.Put(ctx, stateKey, stateData); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to restore workflow state",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"error":       err.Error(),
				},
			}
		}
	}

	for _, claim := range archive.Claims {
		claimData, err := json.Marshal(claim)
		if err != nil {
			continue
		}

		claimKey := fmt.Sprintf("claim:%s", workflowID)
		if err := storagePort.Put(ctx, claimKey, claimData); err != nil {
			am.logger.Error("failed to restore claim",
				"workflow_id", workflowID,
				"error", err)
		}
	}

	for _, auditLog := range archive.AuditLogs {
		auditData, err := json.Marshal(auditLog)
		if err != nil {
			continue
		}

		auditKey := fmt.Sprintf("audit:%s:%d", workflowID, time.Now().UnixNano())
		if err := storagePort.Put(ctx, auditKey, auditData); err != nil {
			am.logger.Error("failed to restore audit log",
				"workflow_id", workflowID,
				"error", err)
		}
	}

	am.logger.Info("workflow restoration completed",
		"workflow_id", workflowID,
		"restored_from", archive.ArchivedAt)

	return nil
}

func (am *ArchivalManager) GetArchiveStats(ctx context.Context) (ArchiveStats, error) {
	archives, err := am.storage.List(ctx, "workflow-archive:")
	if err != nil {
		return ArchiveStats{}, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list archives",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	stats := ArchiveStats{
		TotalArchives: len(archives),
		LastUpdated:   time.Now(),
	}

	workflowCounts := make(map[string]int)
	for _, archiveKey := range archives {
		if workflowID := extractWorkflowIDFromKey(archiveKey); workflowID != "" {
			workflowCounts[workflowID]++
		}
	}

	stats.UniqueWorkflows = len(workflowCounts)

	var totalSize int64
	for _, archiveKey := range archives {
		if data, err := am.storage.Retrieve(ctx, archiveKey); err == nil {
			totalSize += int64(len(data))
		}
	}

	stats.TotalSizeBytes = totalSize

	return stats, nil
}

func (am *ArchivalManager) compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)

	if _, err := gzWriter.Write(data); err != nil {
		return nil, err
	}

	if err := gzWriter.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (am *ArchivalManager) decompressData(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	return io.ReadAll(gzReader)
}

func (am *ArchivalManager) deserializeArchive(data []byte, archive *WorkflowArchive) error {
	if err := json.Unmarshal(data, archive); err == nil {
		return nil
	}

	decompressed, err := am.decompressData(data)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to decompress archive data",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return json.Unmarshal(decompressed, archive)
}

func extractWorkflowIDFromKey(key string) string {
	prefix := "workflow-archive:"
	if len(key) <= len(prefix) {
		return ""
	}

	remaining := key[len(prefix):]
	for i, char := range remaining {
		if char == ':' {
			return remaining[:i]
		}
	}

	return remaining
}

type ArchiveStats struct {
	TotalArchives   int       `json:"total_archives"`
	UniqueWorkflows int       `json:"unique_workflows"`
	TotalSizeBytes  int64     `json:"total_size_bytes"`
	LastUpdated     time.Time `json:"last_updated"`
}
