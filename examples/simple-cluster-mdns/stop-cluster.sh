#!/bin/bash

echo "🛑 Stopping Graft cluster..."

PIDS=$(pgrep -f "simple-cluster-mdns")

if [ -z "$PIDS" ]; then
    echo "  ℹ️  No running nodes found"
else
    echo "  Found running processes: $PIDS"
    echo "$PIDS" | xargs kill
    sleep 2

    REMAINING=$(pgrep -f "simple-cluster-mdns")
    if [ -n "$REMAINING" ]; then
        echo "  Force killing remaining processes..."
        echo "$REMAINING" | xargs kill -9
    fi

    echo "  ✓ All nodes stopped"
fi

echo ""
echo "✅ Cluster stopped"
echo ""
echo "To clean data: ./clean.sh"
echo "To restart: ./run-cluster.sh"
echo ""
