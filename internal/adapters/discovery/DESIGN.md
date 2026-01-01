# Discovery System Design

## Overview

Discovery is **seeder-only**. It finds initial peers at startup. After joining the cluster, Raft is the source of truth.

## Seeder Interface

```go
type Peer struct {
    ID      string
    Address string
    Port    int
}

type Seeder interface {
    Discover(ctx context.Context) ([]Peer, error)
    Name() string
}
```

## Available Seeders

| Seeder | Use Case |
|--------|----------|
| Static | Configured peer list |
| DNS | K8s headless service |
| MDNS | Zero-config local network |

## Bootstrap Decision Logic

### Who Initiates?

**Lowest ID** among visible peers initiates the cluster. No ordinal-0 special case.

### Quorum Gate

Before initiating, a node must see enough peers:

```go
quorum := (expectedNodes / 2) + 1

if len(peers) >= quorum - 1 {
    if iAmLowestID(peers, myID) {
        initiate()
    }
}
```

| Expected Nodes | Quorum | Peers Needed to Initiate |
|----------------|--------|--------------------------|
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

### Why Quorum, Not Full Cluster?

If we required seeing all N-1 peers:
- One dead node = cluster never forms
- Deadlock on partial failures

With quorum-based:
- 2 of 3 nodes can form cluster
- 3rd joins later (or never)
- Liveness preserved

### Race Condition Prevention

Two nodes starting simultaneously with MDNS:

1. Both query MDNS
2. MDNS takes time to propagate
3. Without quorum gate: both might initiate (split-brain)
4. With quorum gate: neither initiates until they see each other
5. Once they see each other, lowest ID wins

### Fallback for Single Node

If configured as single-node deployment (`expectedNodes = 1`):
- No peers needed
- Initiate immediately

## Request Proxying

Nodes proxy requests to leader when not leader themselves:

```
New Node → Any Peer → (not leader?) → proxy to leader → response
```

This means joining nodes don't need to know who the leader is. They contact any discovered peer.

## What Discovery Does NOT Do

- Track who's alive (Raft does this)
- Decide membership (Raft does this)
- Remove peers (Raft does this)
- Continuous polling (fire once)
- Ordinal assignment (no ordinals)
