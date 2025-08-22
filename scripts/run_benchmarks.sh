#!/bin/bash

# Performance Benchmark Script for gRPC Transport
set -e

cd "$(dirname "$0")/.."

echo "Running gRPC Transport Performance Benchmarks..."
echo "=============================================="

# Clean any stale test processes
pkill -f "go test" || true
sleep 1

echo
echo "1. JoinCluster Performance:"
echo "-------------------------"
go test ./internal/adapters/transport/grpc -bench=BenchmarkClusterService_JoinCluster -benchmem -count=3 -run=^$ 2>/dev/null | grep -E "(BenchmarkClusterService_JoinCluster|ns/op|B/op|allocs/op)"

echo
echo "2. LeaveCluster Performance:"
echo "----------------------------"
go test ./internal/adapters/transport/grpc -bench=BenchmarkClusterService_LeaveCluster -benchmem -count=3 -run=^$ 2>/dev/null | grep -E "(BenchmarkClusterService_LeaveCluster|ns/op|B/op|allocs/op)"

echo
echo "3. RequestVote Performance:"
echo "---------------------------"
go test ./internal/adapters/transport/grpc -bench=BenchmarkRaftService_RequestVote -benchmem -count=3 -run=^$ 2>/dev/null | grep -E "(BenchmarkRaftService_RequestVote|ns/op|B/op|allocs/op)"

echo
echo "4. AppendEntries Small Payload (100 bytes):"
echo "--------------------------------------------"
go test ./internal/adapters/transport/grpc -bench=BenchmarkRaftService_AppendEntries_Small -benchmem -count=3 -run=^$ 2>/dev/null | grep -E "(BenchmarkRaftService_AppendEntries_Small|ns/op|B/op|allocs/op)"

echo
echo "5. WorkflowService Performance:"
echo "-------------------------------"
go test ./internal/adapters/transport/grpc -bench=BenchmarkWorkflowService_GetWorkflowState -benchmem -count=3 -run=^$ 2>/dev/null | grep -E "(BenchmarkWorkflowService_GetWorkflowState|ns/op|B/op|allocs/op)"

echo
echo "=============================================="
echo "Benchmark completed successfully!"