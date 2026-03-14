package runtime

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

func TestPluginRegistry_EmptyHasNoPlugins(t *testing.T) {
	reg := NewPluginRegistry()
	if reg.HasPlugins(PhaseBeforeValidation) {
		t.Fatal("expected no plugins for empty registry")
	}
}

func TestPluginRegistry_Execute(t *testing.T) {
	reg := NewPluginRegistry()

	called := false
	reg.Register(&testPlugin{
		name:  "test-plugin",
		phase: PhaseBeforeValidation,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			called = true
			msg.Metadata["enriched"] = true
			return msg, nil
		},
	})

	if !reg.HasPlugins(PhaseBeforeValidation) {
		t.Fatal("expected plugins for before_validation")
	}
	if reg.HasPlugins(PhaseAfterTransform) {
		t.Fatal("expected no plugins for after_transform")
	}

	msg := message.New("ch-1", []byte("hello"))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	result, err := reg.Execute(context.Background(), PhaseBeforeValidation, msg, logger)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !called {
		t.Fatal("plugin was not called")
	}
	if result.Metadata["enriched"] != true {
		t.Fatal("plugin did not modify message")
	}
}

func TestPluginRegistry_MultiplePlugins(t *testing.T) {
	reg := NewPluginRegistry()

	var order []string
	reg.Register(&testPlugin{
		name:  "first",
		phase: PhaseBeforeTransform,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			order = append(order, "first")
			return msg, nil
		},
	})
	reg.Register(&testPlugin{
		name:  "second",
		phase: PhaseBeforeTransform,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			order = append(order, "second")
			return msg, nil
		},
	})

	msg := message.New("ch-1", []byte("test"))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	_, err := reg.Execute(context.Background(), PhaseBeforeTransform, msg, logger)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("expected order [first, second], got %v", order)
	}
}

func TestNewScriptPlugin_InvalidPhase(t *testing.T) {
	_, err := NewScriptPlugin(config.PluginConfig{
		Name:       "bad",
		Phase:      "invalid_phase",
		Entrypoint: "test.ts",
	}, "/tmp", "/tmp", nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err == nil {
		t.Fatal("expected error for invalid phase")
	}
}

func TestNewScriptPlugin_ValidPhases(t *testing.T) {
	phases := []string{
		"before_validation", "after_validation",
		"before_transform", "after_transform",
		"before_destination", "after_destination",
	}
	for _, p := range phases {
		sp, err := NewScriptPlugin(config.PluginConfig{
			Name:       "test",
			Phase:      p,
			Entrypoint: "test.ts",
		}, "/tmp", "/tmp", nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))
		if err != nil {
			t.Fatalf("unexpected error for phase %s: %v", p, err)
		}
		if sp.Name() != "test" {
			t.Fatalf("expected name=test, got %s", sp.Name())
		}
		if string(sp.Phase()) != p {
			t.Fatalf("expected phase=%s, got %s", p, sp.Phase())
		}
	}
}

type testPlugin struct {
	name  string
	phase Phase
	fn    func(ctx context.Context, msg *message.Message) (*message.Message, error)
}

func (tp *testPlugin) Name() string  { return tp.name }
func (tp *testPlugin) Phase() Phase  { return tp.phase }
func (tp *testPlugin) Process(ctx context.Context, msg *message.Message) (*message.Message, error) {
	return tp.fn(ctx, msg)
}
