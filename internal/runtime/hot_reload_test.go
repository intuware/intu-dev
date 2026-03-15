package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewHotReloader(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	if hr.engine != engine {
		t.Error("engine not set correctly")
	}
	if hr.channelsDir != channelsDir {
		t.Errorf("channelsDir = %q, want %q", hr.channelsDir, channelsDir)
	}
	if hr.watcher == nil {
		t.Error("watcher should not be nil")
	}
	if hr.debounce == nil {
		t.Error("debounce map should be initialized")
	}
}

func TestHotReloader_ChannelIDFromDir(t *testing.T) {
	channelDir := t.TempDir()
	channelYAML := `id: test-channel-id
enabled: true
listener:
  type: http
  http:
    port: 0
`
	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte(channelYAML), 0o644)

	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	id := hr.channelIDFromDir(channelDir)
	if id != "test-channel-id" {
		t.Errorf("channelIDFromDir() = %q, want 'test-channel-id'", id)
	}
}

func TestHotReloader_ChannelIDFromDir_NoConfig(t *testing.T) {
	emptyDir := t.TempDir()

	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	id := hr.channelIDFromDir(emptyDir)
	if id != "" {
		t.Errorf("channelIDFromDir() = %q, want empty for dir without channel.yaml", id)
	}
}

func TestHotReloader_ChannelIDFromDir_InvalidYAML(t *testing.T) {
	channelDir := t.TempDir()
	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte("{{invalid yaml"), 0o644)

	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	id := hr.channelIDFromDir(channelDir)
	if id != "" {
		t.Errorf("channelIDFromDir() = %q, want empty for invalid YAML", id)
	}
}

func TestHotReloader_ShouldDebounce(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	t.Run("first call returns false", func(t *testing.T) {
		if hr.shouldDebounce("ch-1") {
			t.Error("first call should return false (not debounced)")
		}
	})

	t.Run("immediate second call returns true", func(t *testing.T) {
		if !hr.shouldDebounce("ch-1") {
			t.Error("immediate second call should return true (debounced)")
		}
	})

	t.Run("different channel not debounced", func(t *testing.T) {
		if hr.shouldDebounce("ch-2") {
			t.Error("different channel should not be debounced")
		}
	})

	t.Run("after debounce window returns false", func(t *testing.T) {
		hr.debounceMu.Lock()
		hr.debounce["ch-expired"] = time.Now().Add(-3 * time.Second)
		hr.debounceMu.Unlock()

		if hr.shouldDebounce("ch-expired") {
			t.Error("should not debounce after debounce window has passed")
		}
	})
}

func TestHotReloader_ShouldDebounce_ConcurrentChannels(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	for i := 0; i < 10; i++ {
		chID := "concurrent-ch"
		if i == 0 {
			if hr.shouldDebounce(chID) {
				t.Error("first call should not be debounced")
			}
		} else {
			if !hr.shouldDebounce(chID) {
				t.Errorf("call %d should be debounced", i)
			}
		}
	}
}

func TestHotReloader_StartStop(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	channelDir := filepath.Join(channelsDir, "test-channel")
	os.MkdirAll(channelDir, 0o755)
	channelYAML := `id: test-channel
enabled: true
listener:
  type: http
  http:
    port: 0
`
	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte(channelYAML), 0o644)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}

	ctx := context.Background()
	if err := hr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	hr.Stop()
}

func TestHotReloader_StartWithEmptyChannelsDir(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}

	ctx := context.Background()
	if err := hr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	hr.Stop()
}

func TestHotReloader_StartWithMultipleChannels(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")

	for _, name := range []string{"ch-a", "ch-b"} {
		chDir := filepath.Join(channelsDir, name)
		os.MkdirAll(chDir, 0o755)
		yaml := "id: " + name + "\nenabled: true\nlistener:\n  type: http\n  http:\n    port: 0\n"
		os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte(yaml), 0o644)
	}

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}

	ctx := context.Background()
	if err := hr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	hr.Stop()
}

func TestHotReloader_StopWithoutStart(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}

	hr.Stop()
}

func TestHotReloader_WatchChannelDir(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	channelDir := filepath.Join(channelsDir, "test-channel")
	os.MkdirAll(channelDir, 0o755)
	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte("id: test\nenabled: true\nlistener:\n  type: http\n  http:\n    port: 0\n"), 0o644)
	os.WriteFile(filepath.Join(channelDir, "transformer.ts"), []byte("export function transform() {}"), 0o644)
	os.WriteFile(filepath.Join(channelDir, "README.md"), []byte("# readme"), 0o644)

	subDir := filepath.Join(channelDir, "sub")
	os.MkdirAll(subDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	hr.watchChannelDir(channelDir)
}
