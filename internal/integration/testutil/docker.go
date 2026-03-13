//go:build integration

package testutil

import (
	"os/exec"
	"runtime"
)

// DockerAvailable returns true if the Docker daemon is reachable.
func DockerAvailable() bool {
	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

// DockerReason returns a human-readable reason why Docker might not be available.
func DockerReason() string {
	if runtime.GOOS == "darwin" {
		return "Docker Desktop may not be running"
	}
	return "Docker daemon not reachable (is Docker installed and running?)"
}
