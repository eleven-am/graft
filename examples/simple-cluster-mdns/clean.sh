#!/bin/bash

echo "🧹 Cleaning up cluster data..."

./stop-cluster.sh 2>/dev/null || true

if [ -d "./data" ]; then
    rm -rf ./data
    echo "  ✓ Removed data directory"
fi

if [ -f "./simple-cluster-mdns" ]; then
    rm -f ./simple-cluster-mdns
    echo "  ✓ Removed binary"
fi

echo ""
echo "✅ Cleanup complete"
echo ""
