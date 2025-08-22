# mDNS Discovery Example

Zero-configuration service discovery using multicast DNS for automatic node detection on local networks.

## Overview

This example demonstrates:
- Automatic node discovery via mDNS/Bonjour
- Zero-configuration cluster formation  
- Dynamic cluster membership
- Service advertisement and resolution
- Local network mesh topology

## Use Cases

- Development environments
- IoT and edge computing scenarios
- Local testing and demos
- Small office/home networks
- Plug-and-play cluster setup

## Configuration

mDNS discovery uses service advertisements:

```go
config.Discovery.Strategy = graft.StrategyMDNS
config.Discovery.MDNSServiceName = "graft-mdns-example"
config.Discovery.MDNSDomain = "local."
```

## Running

### Single Instance
```bash
go run main.go
```

### Multiple Instances (Auto-Discovery)
```bash
# Terminal 1
NODE_ID=mdns-node-1 SERVICE_PORT=8081 TRANSPORT_PORT=9091 go run main.go

# Terminal 2 (will automatically discover node-1)
NODE_ID=mdns-node-2 SERVICE_PORT=8082 TRANSPORT_PORT=9092 go run main.go

# Terminal 3 (will automatically discover both)  
NODE_ID=mdns-node-3 SERVICE_PORT=8083 TRANSPORT_PORT=9093 go run main.go
```

## How It Works

1. **Service Advertisement**: Each node advertises itself via mDNS
2. **Discovery**: Nodes continuously scan for other instances
3. **Cluster Formation**: Discovered nodes join the cluster automatically
4. **Dynamic Membership**: Nodes can join/leave without configuration changes

## mDNS Service Details

- **Service Type**: `_graft-mdns-example._tcp.local.`
- **Discovery Domain**: `local.` 
- **Advertisement Interval**: Every 5 seconds
- **TTL**: 60 seconds

## Expected Behavior

1. First node starts and advertises itself
2. Additional nodes discover the first node automatically
3. Cluster forms without manual configuration
4. New nodes are discovered and integrated dynamically
5. Failed nodes are removed automatically

## Network Requirements

- All nodes must be on the same local network segment
- Multicast traffic must be allowed (UDP port 5353)
- No firewall blocking between nodes
- mDNS/Bonjour support in the operating system

## Platform Support

- **macOS**: Built-in Bonjour support
- **Linux**: Requires Avahi daemon
- **Windows**: Bonjour service (Apple Bonjour Print Services)
- **Docker**: Requires host networking mode

## Advantages

- Zero configuration required
- Automatic cluster formation
- Dynamic node discovery
- No central coordination needed
- Works across different platforms

## Limitations

- Limited to local network segments
- Requires multicast support
- Not suitable for WAN deployments
- May have discovery delays
- Platform-dependent implementation

## Troubleshooting

### No Nodes Discovered

Check mDNS service:
```bash
# macOS/Linux
dns-sd -B _graft-mdns-example._tcp local.

# Alternative
avahi-browse -t _graft-mdns-example._tcp
```

### Firewall Issues
```bash
# Allow mDNS traffic
sudo ufw allow 5353/udp
```

### Docker Networking
```yaml
# Use host networking
network_mode: host
```

## Testing Discovery

Monitor mDNS traffic:
```bash
# tcpdump
sudo tcpdump -i en0 -n port 5353

# Wireshark filter
udp.port == 5353
```

## Next Steps

- Compare with static discovery approach
- Try Kubernetes service discovery
- Implement custom discovery mechanisms
- Add discovery health monitoring