package bootstrap

import (
	"context"
)

// OrchestratorAPI exposes the bootstrap orchestration surface for testing/mocking.
type OrchestratorAPI interface {
	Start(ctx context.Context, grpcPort int) (*Result, error)
	Stop()
}
