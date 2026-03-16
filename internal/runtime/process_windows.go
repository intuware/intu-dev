//go:build windows

package runtime

import "os/exec"

func setProcessGroup(_ *exec.Cmd) {}

func killProcessGroup(_ int) {}
