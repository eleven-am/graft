# Simple 3-Node Cluster - mDNS Discovery

This example demonstrates a 3-node Graft cluster using **mDNS (multicast DNS) discovery**. mDNS automatically discovers peers on the local network without manual configuration, making it ideal for development and local testing.

## Features

- **3-Node Cluster**: Three Graft nodes forming a Raft consensus cluster
- **mDNS Discovery**: Automatic peer discovery on local network
- **Leader Election**: Automatic leader election via Raft
- **Automatic Healing**: Nodes automatically rejoin after failure
- **State Persistence**: Cluster survives full restart with persisted state
- **Status Monitoring**: Real-time cluster status logging
- **Zero Configuration**: No peer list needed - nodes find each other automatically

## Prerequisites

- Go 1.24 or later
- Available ports: 7001-7003 (Raft), 9001-9003 (gRPC)
- Local network with multicast support

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

1. **mDNS Advertising**: Each node advertises itself via mDNS
2. **Peer Discovery**: Nodes discover each other automatically
3. **Cluster Formation**: Nodes form a Raft cluster
4. **Leader Election**: One node becomes leader (check logs for `is_leader=true`)
5. **Follower Status**: Other nodes become followers
6. **Status Updates**: Every 5 seconds, each node logs cluster status

Example log output:
```
INFO cluster status is_leader=true leader_id=node-2 peers=2 state=Leader
```

## Testing Scenarios

### Scenario 1: Leader Failure

1. Identify the current leader from logs (`is_leader=true`)
2. Kill the leader process (Ctrl+C)
3. Watch remaining nodes elect a new leader (typically within 5-10 seconds)
4. Restart the old leader - it should auto-discover cluster and rejoin as follower

### Scenario 2: Node Healing

1. Kill any node (Ctrl+C)
2. Observe: Cluster continues with remaining nodes
3. Restart the killed node
4. Observe: Node auto-discovers cluster, rejoins, and catches up

### Scenario 3: Full Cluster Restart

1. Kill all 3 nodes (Ctrl+C on all terminals)
2. Restart all 3 nodes using same commands
3. Observe: Nodes discover each other via mDNS
4. Observe: Cluster reforms and recovers persisted state

### Scenario 4: Dynamic Node Addition

1. Start nodes 1 and 2
2. Wait for cluster formation
3. Start node 3
4. Observe: Node 3 discovers existing cluster and joins automatically

### Scenario 5: Network Partition Simulation

1. Kill 2 nodes simultaneously
2. Observe: Single remaining node cannot elect itself (needs quorum of 2)
3. Restart one of the killed nodes
4. Observe: Nodes discover each other, quorum restored, leader elected

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
rm -f ./simple-cluster-mdns
```

## Configuration Details

### mDNS Settings

Configured in `main.go`:

```go
WithMDNS("graft-example", "local", "")
```

- **Service**: `graft-example` - All nodes must use same service name
- **Domain**: `local` - Standard mDNS domain for local network
- **Host**: `""` - Auto-detected

### Port Mapping

| Node | Raft Port | gRPC Port | Data Dir |
|------|-----------|-----------|----------|
| node-1 | 7001 | 9001 | ./data/node-1 |
| node-2 | 7002 | 9002 | ./data/node-2 |
| node-3 | 7003 | 9003 | ./data/node-3 |

### Environment Variables

Customize behavior via environment variables:

```bash
GRAFT_NODE_ID=node-X          # Unique node identifier
GRAFT_RAFT_ADDR=127.0.0.1:700X # Raft bind address
GRAFT_GRPC_PORT=900X           # gRPC port
GRAFT_DATA_DIR=./data/node-X   # Persistent data directory
GRAFT_MDNS_SERVICE=graft-example  # mDNS service name (optional)
GRAFT_MDNS_DOMAIN=local        # mDNS domain (optional)
```

## When to Use mDNS Discovery

**Best for:**
- Development environments
- Local testing
- Small deployments on same LAN
- Dynamic node additions
- Rapid prototyping

**Advantages:**
- Zero configuration needed
- Automatic peer discovery
- Easy to add/remove nodes
- Perfect for development

**Disadvantages:**
- Local network only (multicast)
- May not work across VLANs/subnets
- Requires multicast support
- Discovery can be slower than static

## Troubleshooting

### Nodes Not Discovering Each Other

**Check:**
- All nodes using same mDNS service name (`graft-example`)
- Multicast is enabled on network interface
- No firewall blocking multicast traffic (224.0.0.251:5353)
- Nodes are on same subnet/VLAN

**Test multicast:**
```bash
ping 224.0.0.251
```

### Port Already in Use
```
Error: bind: address already in use
```
**Solution**: Change port numbers or kill processes using those ports

### Discovery Too Slow

**Possible causes:**
- Network congestion
- mDNS cache issues
- Firewall interference

**Solution:**
- Wait 10-15 seconds for discovery
- Check network connectivity
- Restart nodes

### Leader Not Elected

**Possible causes:**
- Less than 2 nodes running (quorum requirement)
- Nodes haven't discovered each other yet
- Network partition

**Solution:**
- Wait for discovery to complete
- Check logs for peer count
- Ensure quorum available

## How mDNS Discovery Works

1. **Advertising**: Each node broadcasts its presence via mDNS
2. **Listening**: Nodes listen for mDNS announcements from peers
3. **Registration**: Discovered peers are registered in Raft
4. **Formation**: Once quorum reached, cluster forms
5. **Maintenance**: Continuous discovery for new/rejoining nodes

## Network Requirements

- **Multicast Support**: Network must support IP multicast
- **Same Subnet**: All nodes should be on same local network
- **Firewall Rules**: Allow UDP port 5353 (mDNS)
- **No VLAN Isolation**: Multicast must traverse VLANs if needed

## Next Steps

- Try the **static discovery** example for production-like setup
- Explore workflow execution across the cluster
- Add a 4th node and watch it auto-discover
- Test with nodes on different machines (same network)
