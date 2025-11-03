package dockermdns_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	dockerTestEnv   = "GRAFT_DOCKER_TESTS"
	composeFileName = "docker-compose.yml"
	projectName     = "graft-mdns-test"
)

func TestDockerMDNSClusterFormsSingleLeader(t *testing.T) {
	if os.Getenv(dockerTestEnv) != "1" {
		t.Skipf("set %s=1 to enable docker integration tests", dockerTestEnv)
	}

	exampleDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to resolve working directory: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	composeArgs := func(args ...string) []string {
		base := []string{"compose", "-p", projectName, "-f", composeFileName}
		return append(base, args...)
	}

	runDocker := func(ctx context.Context, args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, "docker", args...)
		cmd.Dir = exampleDir
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		if err != nil {
			return stdout.String() + stderr.String(), err
		}
		return stdout.String(), nil
	}

	// Ensure clean slate before starting
	_, _ = runDocker(ctx, composeArgs("down", "--volumes", "--remove-orphans")...)

	// Start cluster
	if output, err := runDocker(ctx, composeArgs("up", "-d", "--build")...); err != nil {
		t.Fatalf("docker compose up failed: %v\n%s", err, output)
	}

	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		if output, err := runDocker(stopCtx, composeArgs("down", "--volumes", "--remove-orphans")...); err != nil {
			t.Fatalf("docker compose down failed: %v\n%s", err, output)
		}
	}()

	deadline := time.Now().Add(120 * time.Second)
	var logs string
	for time.Now().Before(deadline) {
		out, err := runDocker(ctx, composeArgs("logs")...)
		if err != nil {
			t.Fatalf("docker compose logs failed: %v\n%s", err, out)
		}
		logs = out

		if clusterReady(logs) {
			return
		}

		if strings.Contains(logs, "context deadline exceeded") {
			t.Fatalf("cluster failed to start:\n%s", logs)
		}

		time.Sleep(3 * time.Second)
	}

	t.Fatalf("cluster did not become ready before timeout.\nLogs:\n%s", logs)
}

func clusterReady(logs string) bool {
	required := []string{
		`node is ready" node_id=node-1`,
		`node is ready" node_id=node-2`,
		`node is ready" node_id=node-3`,
		`cluster status" node_id=node-1 status=running is_leader=true peers=2`,
		`cluster status" node_id=node-2 status=running is_leader=false peers=2`,
		`cluster status" node_id=node-3 status=running is_leader=false peers=2`,
	}
	for _, needle := range required {
		if !strings.Contains(logs, needle) {
			return false
		}
	}
	return true
}
