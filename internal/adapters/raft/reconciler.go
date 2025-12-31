package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"log/slog"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ReconcilerState string

const (
	ReconcilerStateIdle        ReconcilerState = "idle"
	ReconcilerStatePending     ReconcilerState = "pending"
	ReconcilerStateReconciling ReconcilerState = "reconciling"
	ReconcilerStateSucceeded   ReconcilerState = "succeeded"
	ReconcilerStateFailed      ReconcilerState = "failed"
)

var (
	ErrReconciliationFailed = errors.New("reconciliation failed: could not rejoin cluster")
	ErrNoTransport          = errors.New("reconciliation failed: no transport available")
	ErrNoPeersAvailable     = errors.New("reconciliation failed: no peers available")
	ErrLeaderNotFound       = errors.New("reconciliation failed: leader not found")
	ErrAddVoterRejected     = errors.New("reconciliation failed: add voter request rejected")
)

type Reconciler struct {
	mu sync.RWMutex

	runtime   *Runtime
	transport ports.TransportPort
	peers     []domain.RaftPeerSpec
	nodeID    string
	nodeAddr  string
	logger    *slog.Logger

	state     ReconcilerState
	lastError error
	attempts  int
}

type ReconcilerConfig struct {
	Runtime   *Runtime
	Transport ports.TransportPort
	Peers     []domain.RaftPeerSpec
	NodeID    string
	NodeAddr  string
	Logger    *slog.Logger
}

func NewReconciler(cfg ReconcilerConfig) *Reconciler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Reconciler{
		runtime:   cfg.Runtime,
		transport: cfg.Transport,
		peers:     cfg.Peers,
		nodeID:    cfg.NodeID,
		nodeAddr:  cfg.NodeAddr,
		logger:    logger.With("component", "raft.reconciler"),
		state:     ReconcilerStatePending,
	}
}

func (r *Reconciler) State() ReconcilerState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Reconciler) LastError() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastError
}

func (r *Reconciler) Reconcile(ctx context.Context) error {
	r.mu.Lock()
	if r.state == ReconcilerStateSucceeded {
		r.mu.Unlock()
		return nil
	}
	r.state = ReconcilerStateReconciling
	r.mu.Unlock()

	if r.transport == nil {
		r.setFailed(ErrNoTransport)
		return ErrNoTransport
	}

	if len(r.peers) == 0 {
		r.setFailed(ErrNoPeersAvailable)
		return ErrNoPeersAvailable
	}

	r.logger.Info("starting cluster reconciliation",
		"node_id", r.nodeID,
		"node_addr", r.nodeAddr,
		"peer_count", len(r.peers))

	leaderAddr, err := r.findLeader(ctx)
	if err != nil {
		r.setFailed(err)
		return err
	}

	r.logger.Info("leader found, requesting to rejoin cluster",
		"leader_addr", leaderAddr,
		"node_id", r.nodeID)

	if err := r.requestAddVoter(ctx, leaderAddr); err != nil {
		r.setFailed(err)
		return err
	}

	r.mu.Lock()
	r.state = ReconcilerStateSucceeded
	r.mu.Unlock()

	if r.runtime != nil {
		r.runtime.SetReconciliationState("succeeded")
	}

	r.logger.Info("reconciliation succeeded, node rejoined cluster",
		"node_id", r.nodeID)

	return nil
}

func (r *Reconciler) findLeader(ctx context.Context) (string, error) {
	for _, peer := range r.peers {
		r.mu.Lock()
		r.attempts++
		r.mu.Unlock()

		peerGRPCAddr := r.getGRPCAddress(peer)

		r.logger.Debug("querying peer for leader info",
			"peer_id", peer.ID,
			"peer_addr", peer.Address,
			"peer_grpc_addr", peerGRPCAddr)

		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		leaderID, leaderAddr, err := r.transport.GetLeaderInfo(queryCtx, peerGRPCAddr)
		cancel()

		if err != nil {
			r.logger.Debug("failed to get leader info from peer",
				"peer_id", peer.ID,
				"peer_grpc_addr", peerGRPCAddr,
				"error", err)
			continue
		}

		if leaderAddr != "" {
			r.logger.Debug("found leader via peer",
				"peer_id", peer.ID,
				"leader_id", leaderID,
				"leader_addr", leaderAddr)
			return leaderAddr, nil
		}

		if peerGRPCAddr != "" {
			r.logger.Debug("peer has no leader info, trying peer directly as potential leader",
				"peer_grpc_addr", peerGRPCAddr)
			return peerGRPCAddr, nil
		}
	}

	return "", ErrLeaderNotFound
}

func (r *Reconciler) getGRPCAddress(peer domain.RaftPeerSpec) string {
	if peer.Metadata != nil {
		if grpcPort, ok := peer.Metadata["grpc_port"]; ok && grpcPort != "" {
			host, _, err := net.SplitHostPort(peer.Address)
			if err != nil {
				host = peer.Address
			}
			return fmt.Sprintf("%s:%s", host, grpcPort)
		}
	}
	return peer.Address
}

func (r *Reconciler) requestAddVoter(ctx context.Context, leaderAddr string) error {
	addCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := r.transport.RequestAddVoter(addCtx, leaderAddr, r.nodeID, r.nodeAddr)
	if err != nil {
		r.logger.Warn("add voter request failed",
			"leader_addr", leaderAddr,
			"node_id", r.nodeID,
			"error", err)
		return errors.Join(ErrAddVoterRejected, err)
	}

	return nil
}

func (r *Reconciler) setFailed(err error) {
	r.mu.Lock()
	r.state = ReconcilerStateFailed
	r.lastError = err
	r.mu.Unlock()

	if r.runtime != nil {
		r.runtime.SetReconciliationState("failed")
	}

	r.logger.Error("reconciliation failed",
		"node_id", r.nodeID,
		"attempts", r.attempts,
		"error", err)
}

func (r *Reconciler) ReconcileWithRetry(ctx context.Context, maxRetries int, retryInterval time.Duration) error {
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := r.Reconcile(ctx); err == nil {
			return nil
		}

		if attempt < maxRetries-1 {
			r.logger.Info("reconciliation failed, retrying",
				"attempt", attempt+1,
				"max_retries", maxRetries,
				"retry_in", retryInterval)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryInterval):
				r.mu.Lock()
				r.state = ReconcilerStatePending
				r.mu.Unlock()
			}
		}
	}

	return r.LastError()
}
