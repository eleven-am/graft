# Simple 3-Node Cluster - Static Discovery

This example demonstrates a 3-node Graft cluster using **static peer discovery**. Static discovery uses a pre-configured list of peer addresses, making it ideal for production environments where node addresses are known in advance.

## Features

- **3-Node Cluster**: Three Graft nodes forming a Raft consensus cluster
- **Static Discovery**: Pre-configured peer list for guaranteed discovery
- **Leader Election**: Automatic leader election via Raft
- **Automatic Healing**: Nodes automatically rejoin after failure
- **State Persistence**: Cluster survives full restart with persisted state
- **Status Monitoring**: Real-time cluster status logging

## Prerequisites

- Go 1.24 or later
- Available ports: 7001-7003 (Raft), 9001-9003 (gRPC)

## Quick Start

### Option 1: Using Helper Scripts

```bash
chmod +x run-cluster.sh stop-cluster.sh clean.sh

./run-cluster.sh
```

### Option 2: Manual Start (3 Terminals)

**Terminal 1 - Node 1:**
```bash
GRAFT_NODE_ID=node-1 \
GRAFT_RAFT_ADDR=127.0.0.1:7001 \
GRAFT_GRPC_PORT=9001 \
GRAFT_DATA_DIR=./data/node-1 \
go run main.go
```

**Terminal 2 - Node 2:**
```bash
GRAFT_NODE_ID=node-2 \
GRAFT_RAFT_ADDR=127.0.0.1:7002 \
GRAFT_GRPC_PORT=9002 \
GRAFT_DATA_DIR=./data/node-2 \
go run main.go
```

**Terminal 3 - Node 3:**
```bash
GRAFT_NODE_ID=node-3 \
GRAFT_RAFT_ADDR=127.0.0.1:7003 \
GRAFT_GRPC_PORT=9003 \
GRAFT_DATA_DIR=./data/node-3 \
go run main.go
```

## What to Expect

After starting all 3 nodes, you should see:

1. **Cluster Formation**: Nodes discover each other via static peer list
2. **Leader Election**: One node becomes leader (check logs for `is_leader=true`)
3. **Follower Status**: Other nodes become followers
4. **Status Updates**: Every 5 seconds, each node logs cluster status

Example log output:
```
INFO cluster status is_leader=true leader_id=node-1 peers=2 state=Leader
```

## Testing Scenarios

### Scenario 1: Leader Failure

1. Identify the current leader from logs (`is_leader=true`)
2. Kill the leader process (Ctrl+C)
3. Watch remaining nodes elect a new leader (typically within 5-10 seconds)
4. Restart the old leader - it should rejoin as a follower

### Scenario 2: Node Healing

1. Kill any node (Ctrl+C)
2. Observe: Cluster continues with remaining nodes
3. Restart the killed node
4. Observe: Node rejoins and catches up with cluster state

### Scenario 3: Full Cluster Restart

1. Kill all 3 nodes (Ctrl+C on all terminals)
2. Restart all 3 nodes using same commands
3. Observe: Cluster reforms and recovers persisted state
4. Leader election occurs again

### Scenario 4: Network Partition Simulation

1. Kill 2 nodes simultaneously
2. Observe: Single remaining node cannot elect itself (needs quorum of 2)
3. Restart one of the killed nodes
4. Observe: Quorum restored, leader election succeeds

## Stopping the Cluster

### Using Helper Script
```bash
./stop-cluster.sh
```

### Manual Stop
Press `Ctrl+C` in each terminal window

## Cleaning Data

To completely reset the cluster and remove all persisted state:

```bash
./clean.sh
```

Or manually:
```bash
rm -rf ./data
rm -f ./simple-cluster-static
```

## Configuration Details

### Static Peer List

All nodes are configured with the same peer list in `main.go`:

```go
WithStaticPeers(
    graft.StaticPeer{
        ID:      "node-1",
        Address: "127.0.0.1",
        Port:    7001, // Raft port
        Metadata: map[string]string{
            "grpc_port": "9001",
        },
    },
    graft.StaticPeer{
        ID:      "node-2",
        Address: "127.0.0.1",
        Port:    7002,
        Metadata: map[string]string{
            "grpc_port": "9002",
        },
    },
    graft.StaticPeer{
        ID:      "node-3",
        Address: "127.0.0.1",
        Port:    7003,
        Metadata: map[string]string{
            "grpc_port": "9003",
        },
    },
)
```

### Port Mapping

| Node | Raft Port | gRPC Port | Data Dir |
|------|-----------|-----------|----------|
| node-1 | 7001 | 9001 | ./data/node-1 |
| node-2 | 7002 | 9002 | ./data/node-2 |
| node-3 | 7003 | 9003 | ./data/node-3 |

## When to Use Static Discovery

**Best for:**
- Production environments
- Fixed infrastructure
- Cloud deployments with known addresses
- High reliability requirements

**Advantages:**
- Predictable and deterministic
- No dependency on mDNS or DNS-SD
- Works across network boundaries
- Explicit configuration

**Disadvantages:**
- Requires manual configuration updates
- Less flexible for dynamic environments
- Node addresses must be known in advance

## Troubleshooting

### Port Already in Use
```
Error: bind: address already in use
```
**Solution**: Change port numbers or kill processes using those ports

### Nodes Not Forming Cluster
**Check:**
- All nodes using same peer list
- Network connectivity between nodes
- Firewall rules allowing connections
- No port conflicts

### Leader Not Elected
**Possible causes:**
- Less than 2 nodes running (quorum requirement)
- Network partition
- Raft timeouts too short

## Next Steps

- Try the **mDNS discovery** example for automatic local discovery
- Explore workflow execution across the cluster
- Add more nodes to the static peer list
- Deploy to multiple machines
