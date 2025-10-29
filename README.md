# Graft

Graft is a Go library that gives your service a built-in workflow engine and distributed job runner. Drop it into an existing binary, register the work you care about, and let Raft handle coordination and failover.

![Go Version](https://img.shields.io/github/go-mod/go-version/eleven-am/graft) ![License](https://img.shields.io/github/license/eleven-am/graft) ![Go Report Card](https://goreportcard.com/badge/github.com/eleven-am/graft)

## Why Graft?

- **Keep everything in your service** – no external orchestrator or queue to operate.
- **Stay alive on failure** – Raft replicates queue state and lease ownership across the cluster.
- **Typed workflows** – write normal Go structs and methods, get compile-time checks.
- **Connectors and work items** – long running listeners and one-shot jobs run under the same manager.

Use it when you need scheduled jobs, fan-out/fan-in flows, or long-running steps that must survive restarts.

## Install

```bash
go get github.com/eleven-am/graft
```

## Basic Workflow

The example below starts a single node, registers one workflow node, kicks off a run, and prints the result. Everything is in-process; no extra services are required.

```go
package main

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "time"

    "github.com/eleven-am/graft"
)

type GreeterConfig struct {
    Message string `json:"message"`
}

type GreeterState struct {
    Name string `json:"name"`
}

type GreeterNode struct{}

func (GreeterNode) GetName() string { return "greeter" }

func (GreeterNode) Execute(ctx context.Context, state GreeterState, cfg GreeterConfig) (graft.NodeResult, error) {
    out := map[string]any{
        "greeting": fmt.Sprintf("%s, %s!", cfg.Message, state.Name),
        "at":       time.Now().UTC(),
    }
    return graft.NodeResult{Data: out}, nil
}

func main() {
    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

    mgr := graft.New("node-1", "127.0.0.1:7000", "./data", logger)
    if mgr == nil {
        panic("graft: manager not created")
    }

    if err := mgr.RegisterNode(&GreeterNode{}); err != nil {
        panic(err)
    }

    if err := mgr.Start(context.Background(), 8080); err != nil {
        panic(err)
    }
    defer mgr.Stop(context.Background())

    trigger := graft.WorkflowTrigger{
        WorkflowID: "wf-001",
        InitialState: GreeterState{
            Name: "Ada",
        },
        InitialNodes: []graft.NodeConfig{{
            Name:   "greeter",
            Config: GreeterConfig{Message: "Hello"},
        }},
    }

    if err := mgr.StartWorkflow(trigger); err != nil {
        panic(err)
    }

    state, err := mgr.GetWorkflowState("wf-001")
    if err != nil {
        panic(err)
    }

    fmt.Printf("workflow status: %s\n", state.Status)
    fmt.Printf("payload: %s\n", state.CurrentState)
}
```

## Connectors at a Glance

Connectors are long-running routines (cron jobs, stream listeners, webhook receivers). Register the implementation once; start as many instances as you need by giving each config a unique `GetID()`.

```go
type CronConfig struct {
    ID       string        `json:"id"`
    Interval time.Duration `json:"interval"`
}

func (c CronConfig) GetID() string { return c.ID }

type CronConnector struct {
    cfg *CronConfig
}

func (c *CronConnector) GetName() string { return "cron" }

func (c *CronConnector) GetConfig() ports.ConnectorConfig { return c.cfg }

func NewCronConnector(configJSON []byte) (ports.ConnectorPort, error) {
    var cfg CronConfig
    if err := json.Unmarshal(configJSON, &cfg); err != nil {
        return nil, err
    }
    if strings.TrimSpace(cfg.ID) == "" {
        return nil, fmt.Errorf("cron config must include an id")
    }
    return &CronConnector{cfg: &cfg}, nil
}

func (c *CronConnector) Start(ctx context.Context) error {
    ticker := time.NewTicker(c.cfg.Interval)
    go func(cfg *CronConfig) {
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case t := <-ticker.C:
                fmt.Printf("[%s] cron tick %s\n", cfg.ID, t.Format(time.RFC3339))
            }
        }
    }(c.cfg)
    return nil
}

func (c *CronConnector) Stop(context.Context) error { return nil }

// ... after creating the manager
mgr.RegisterConnector("cron", NewCronConnector)
cfg := CronConfig{ID: "emails", Interval: 12 * time.Hour}
if err := mgr.StartConnector("cron", cfg); err != nil {
    panic(err)
}
```

Each node watches connector configs. Only one node holds the lease at a time; if it stops sending heartbeats the lease is removed and another node takes over.

## Running More Than One Node

Start the same binary in multiple terminals. Give each process a unique node ID, raft bind address, gRPC port, and data directory before calling `graft.New`. A minimal pattern looks like this:

```go
nodeID := os.Getenv("GRAFT_NODE_ID")                                  // e.g. "node-1"
raftAddr := os.Getenv("GRAFT_RAFT_ADDR")                              // e.g. "127.0.0.1:7100"
grpcPort, _ := strconv.Atoi(os.Getenv("GRAFT_GRPC_PORT")) // handle errors in production
dataDir := os.Getenv("GRAFT_DATA_DIR")

manager := graft.New(nodeID, raftAddr, dataDir, logger)
```

Run each instance with different values:

```
# Terminal 1
GRAFT_NODE_ID=node-1 \
GRAFT_RAFT_ADDR=127.0.0.1:7100 \
GRAFT_GRPC_PORT=9001 \
GRAFT_DATA_DIR=./tmp/node-1 \
go run ./path/to/main.go

# Terminal 2
GRAFT_NODE_ID=node-2 \
GRAFT_RAFT_ADDR=127.0.0.1:7101 \
GRAFT_GRPC_PORT=9002 \
GRAFT_DATA_DIR=./tmp/node-2 \
go run ./path/to/main.go
```

Use the discovery helper that suits your environment (static lists, mDNS, or a custom provider). After Raft elects a leader the queue, lease store, and connectors replicate automatically.

## Project Layout

- `examples/` – runnable demos (document pipeline, cron, API integration)
- `docs/` – architecture notes, API reference, deployment tips
- `internal/core` – manager, workflows, connector runtime
- `internal/adapters` – storage, queue, raft, engine implementations
- `tools/` – utilities used by CI or development

## Learn More

- [Architecture](./docs/ARCHITECTURE.md)
- [API reference](./docs/API.md)
- [Deployment guide](./docs/DEPLOYMENT.md)
- [Examples](./examples)

## Contributing

Contributions are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md), open an issue if you plan larger work, and run `go test ./...` before sending a PR.

## License

MIT License. See [LICENSE](LICENSE).
