package storage

import "github.com/eleven-am/graft/internal/ports"

// WriteMany is a convenience wrapper for BatchWrite to encourage batched writes
func WriteMany(s ports.StoragePort, ops []ports.WriteOp) error {
	return s.BatchWrite(ops)
}
