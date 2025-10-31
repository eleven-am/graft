#!/bin/bash

echo "🧹 Cleaning up cluster data..."

./stop-cluster.sh 2>/dev/null || true

if [ -d "./data" ]; then
    rm -rf ./data
    echo "  ✓ Removed data directory"
fi

if [ -f "./simple-cluster-static" ]; then
    rm -f ./simple-cluster-static
    echo "  ✓ Removed binary"
fi

echo ""
echo "✅ Cleanup complete"
echo ""
