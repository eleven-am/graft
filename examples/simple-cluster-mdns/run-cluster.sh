#!/bin/bash

set -e

echo "🚀 Starting 3-node Graft cluster with mDNS discovery..."

mkdir -p ./data/node-1 ./data/node-2 ./data/node-3

echo "📦 Building binary..."
go build -o simple-cluster-mdns main.go

echo "🔵 Starting node-1..."
GRAFT_NODE_ID=node-1 \
GRAFT_RAFT_ADDR=127.0.0.1:7001 \
GRAFT_GRPC_PORT=9001 \
GRAFT_DATA_DIR=./data/node-1 \
./simple-cluster-mdns > ./data/node-1/output.log 2>&1 &
NODE1_PID=$!
echo "  ✓ node-1 started (PID: $NODE1_PID)"

sleep 2

echo "🟢 Starting node-2..."
GRAFT_NODE_ID=node-2 \
GRAFT_RAFT_ADDR=127.0.0.1:7002 \
GRAFT_GRPC_PORT=9002 \
GRAFT_DATA_DIR=./data/node-2 \
./simple-cluster-mdns > ./data/node-2/output.log 2>&1 &
NODE2_PID=$!
echo "  ✓ node-2 started (PID: $NODE2_PID)"

sleep 2

echo "🟡 Starting node-3..."
GRAFT_NODE_ID=node-3 \
GRAFT_RAFT_ADDR=127.0.0.1:7003 \
GRAFT_GRPC_PORT=9003 \
GRAFT_DATA_DIR=./data/node-3 \
./simple-cluster-mdns > ./data/node-3/output.log 2>&1 &
NODE3_PID=$!
echo "  ✓ node-3 started (PID: $NODE3_PID)"

echo ""
echo "✅ All nodes started!"
echo ""
echo "Process IDs:"
echo "  node-1: $NODE1_PID"
echo "  node-2: $NODE2_PID"
echo "  node-3: $NODE3_PID"
echo ""
echo "Logs:"
echo "  tail -f ./data/node-1/output.log"
echo "  tail -f ./data/node-2/output.log"
echo "  tail -f ./data/node-3/output.log"
echo ""
echo "Note: mDNS discovery may take 10-15 seconds for all nodes to discover each other"
echo ""
echo "To stop the cluster: ./stop-cluster.sh"
echo ""
