package raftimpl

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"

	"github.com/eleven-am/graft/internal/domain"
)

type FSMSnapshot struct {
	db     *badger.DB
	logger *slog.Logger
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	s.logger.Info("persisting snapshot")

	err := func() error {
		data := make(map[string][]byte)
		
		err := s.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 100
			
			it := txn.NewIterator(opts)
			defer it.Close()
			
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := string(item.KeyCopy(nil))
				
				value, err := item.ValueCopy(nil)
				if err != nil {
					return domain.Error{
						Type:    domain.ErrorTypeInternal,
						Message: "failed to copy value during snapshot",
						Details: map[string]interface{}{
							"key":   key,
							"error": err.Error(),
						},
					}
				}
				
				data[key] = value
			}
			
			return nil
		})
		
		if err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to read database during snapshot",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}

		snapshotData := struct {
			Version int               `json:"version"`
			Data    map[string][]byte `json:"data"`
		}{
			Version: 1,
			Data:    data,
		}

		jsonData, err := json.Marshal(snapshotData)
		if err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to marshal snapshot data",
				Details: map[string]interface{}{
					"error":     err.Error(),
					"keys_count": len(data),
				},
			}
		}

		var compressedBuf bytes.Buffer
		gzipWriter := gzip.NewWriter(&compressedBuf)
		
		if _, err := gzipWriter.Write(jsonData); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to compress snapshot data",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
		
		if err := gzipWriter.Close(); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to close gzip writer",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}

		if _, err := io.Copy(sink, &compressedBuf); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to write snapshot to sink",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}

		s.logger.Info("snapshot persisted successfully",
			"keys_count", len(data),
			"compressed_size", compressedBuf.Len())

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		s.logger.Error("failed to persist snapshot", "error", err)
		return err
	}

	return nil
}

func (s *FSMSnapshot) Release() {
	s.logger.Debug("releasing snapshot")
}