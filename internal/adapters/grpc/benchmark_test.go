package grpc

import (
	"context"
	cryptorand "crypto/rand"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

const (
	smallPayloadSize  = 100
	mediumPayloadSize = 10000
	largePayloadSize  = 1000000
)

func BenchmarkClusterService_JoinCluster(b *testing.B) {
	_, addr, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	client := setupBenchmarkClient(b, addr)
	defer client.Close()

	ctx := context.Background()
	req := &ports.JoinRequest{
		NodeID:  "bench-node",
		Address: "127.0.0.1:8000",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.JoinCluster(ctx, addr, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterService_LeaveCluster(b *testing.B) {
	_, addr, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	client := setupBenchmarkClient(b, addr)
	defer client.Close()

	ctx := context.Background()
	req := &ports.LeaveRequest{
		NodeID: "bench-node",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.LeaveCluster(ctx, addr, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRaftService_RequestVote(b *testing.B) {
	_, addr, cleanup := setupBenchmarkRaftServer(b)
	defer cleanup()

	client := setupBenchmarkClient(b, addr)
	defer client.Close()

	ctx := context.Background()
	req := &pb.RequestVoteRequest{
		Term:          2,
		CandidateId:   "candidate",
		LastLogIndex:  10,
		LastLogTerm:   1,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.RequestVote(ctx, addr, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRaftService_AppendEntries_Small(b *testing.B) {
	benchmarkAppendEntries(b, smallPayloadSize)
}

func BenchmarkRaftService_AppendEntries_Medium(b *testing.B) {
	benchmarkAppendEntries(b, mediumPayloadSize)
}

func BenchmarkRaftService_AppendEntries_Large(b *testing.B) {
	benchmarkAppendEntries(b, largePayloadSize)
}

func benchmarkAppendEntries(b *testing.B, payloadSize int) {
	_, addr, cleanup := setupBenchmarkRaftServer(b)
	defer cleanup()

	client := setupBenchmarkClient(b, addr)
	defer client.Close()

	ctx := context.Background()
	
	entries := make([]*pb.LogEntry, 1)
	entries[0] = &pb.LogEntry{
		Index: 1,
		Term:  1,
		Data:  generatePayload(payloadSize),
	}

	req := &pb.AppendEntriesRequest{
		Term:          1,
		LeaderId:      "leader",
		PrevLogIndex:  0,
		PrevLogTerm:   0,
		Entries:       entries,
		LeaderCommit:  0,
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))

	for i := 0; i < b.N; i++ {
		_, err := client.AppendEntries(ctx, addr, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWorkflowService_GetWorkflowState(b *testing.B) {
	_, addr, cleanup := setupBenchmarkWorkflowServer(b)
	defer cleanup()

	client := setupBenchmarkClient(b, addr)
	defer client.Close()

	ctx := context.Background()
	req := &pb.GetWorkflowStateRequest{
		WorkflowId: "workflow-1",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.GetWorkflowState(ctx, addr, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func setupBenchmarkServer(b *testing.B) (*GRPCServer, string, func()) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)
	
	clusterHandler := &mockClusterHandler{}
	server.SetHandlers(clusterHandler, nil, nil)

	ctx := context.Background()
	err := server.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}

	return server, server.GetAddress(), func() {
		server.Stop()
	}
}

func setupBenchmarkRaftServer(b *testing.B) (*GRPCServer, string, func()) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)
	
	raftHandler := &mockRaftHandler{}
	server.SetHandlers(nil, raftHandler, nil)

	ctx := context.Background()
	err := server.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}

	return server, server.GetAddress(), func() {
		server.Stop()
	}
}

func setupBenchmarkWorkflowServer(b *testing.B) (*GRPCServer, string, func()) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)
	
	workflowHandler := &mockWorkflowHandler{}
	server.SetHandlers(nil, nil, workflowHandler)

	ctx := context.Background()
	err := server.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}

	return server, server.GetAddress(), func() {
		server.Stop()
	}
}

func setupBenchmarkClient(b *testing.B, addr string) *GRPCClient {
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 5 * time.Second,
		RequestTimeout: 2 * time.Second,
		RetryBackoff:   10 * time.Millisecond,
	}

	return NewGRPCClient(logger, config)
}

func BenchmarkConcurrentLoad_JoinCluster(b *testing.B) {
	_, addr, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	ctx := context.Background()
	req := &ports.JoinRequest{
		NodeID:  "bench-node",
		Address: "127.0.0.1:8000",
	}

	b.ResetTimer()
	b.ReportAllocs()
	
	b.RunParallel(func(pb *testing.PB) {
		client := setupBenchmarkClient(b, addr)
		defer client.Close()
		
		for pb.Next() {
			_, err := client.JoinCluster(ctx, addr, req)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkConcurrentLoad_AppendEntries(b *testing.B) {
	_, addr, cleanup := setupBenchmarkRaftServer(b)
	defer cleanup()

	ctx := context.Background()
	
	entries := make([]*pb.LogEntry, 1)
	entries[0] = &pb.LogEntry{
		Index: 1,
		Term:  1,
		Data:  generatePayload(smallPayloadSize),
	}

	req := &pb.AppendEntriesRequest{
		Term:          1,
		LeaderId:      "leader",
		PrevLogIndex:  0,
		PrevLogTerm:   0,
		Entries:       entries,
		LeaderCommit:  0,
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(smallPayloadSize))
	
	b.RunParallel(func(pb *testing.PB) {
		client := setupBenchmarkClient(b, addr)
		defer client.Close()
		
		for pb.Next() {
			_, err := client.AppendEntries(ctx, addr, req)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func generatePayload(size int) []byte {
	data := make([]byte, size)
	_, _ = cryptorand.Read(data)
	return data
}