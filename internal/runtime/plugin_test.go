package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

func TestPhaseConstants(t *testing.T) {
	phases := map[Phase]string{
		PhaseBeforeValidation:  "before_validation",
		PhaseAfterValidation:   "after_validation",
		PhaseBeforeTransform:   "before_transform",
		PhaseAfterTransform:    "after_transform",
		PhaseBeforeDestination: "before_destination",
		PhaseAfterDestination:  "after_destination",
	}

	for phase, expected := range phases {
		if string(phase) != expected {
			t.Errorf("Phase constant %v = %q, want %q", phase, string(phase), expected)
		}
	}
}

func TestNewPluginRegistry(t *testing.T) {
	reg := NewPluginRegistry()
	if reg == nil {
		t.Fatal("expected non-nil registry")
	}
	if reg.plugins == nil {
		t.Fatal("plugins map should be initialized")
	}
	if len(reg.plugins) != 0 {
		t.Errorf("expected empty plugins map, got %d entries", len(reg.plugins))
	}
}

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

func TestPluginRegistry_HasPlugins_AllPhases(t *testing.T) {
	reg := NewPluginRegistry()

	allPhases := []Phase{
		PhaseBeforeValidation, PhaseAfterValidation,
		PhaseBeforeTransform, PhaseAfterTransform,
		PhaseBeforeDestination, PhaseAfterDestination,
	}

	for _, phase := range allPhases {
		if reg.HasPlugins(phase) {
			t.Errorf("expected no plugins for phase %s in empty registry", phase)
		}
	}

	reg.Register(&testPlugin{
		name:  "bv-plugin",
		phase: PhaseBeforeValidation,
		fn:    func(ctx context.Context, msg *message.Message) (*message.Message, error) { return msg, nil },
	})

	if !reg.HasPlugins(PhaseBeforeValidation) {
		t.Error("expected HasPlugins=true for before_validation")
	}
	for _, phase := range allPhases[1:] {
		if reg.HasPlugins(phase) {
			t.Errorf("expected HasPlugins=false for %s", phase)
		}
	}
}

func TestPluginRegistry_RegisterMultiplePhases(t *testing.T) {
	reg := NewPluginRegistry()

	reg.Register(&testPlugin{
		name:  "before-val",
		phase: PhaseBeforeValidation,
		fn:    func(ctx context.Context, msg *message.Message) (*message.Message, error) { return msg, nil },
	})
	reg.Register(&testPlugin{
		name:  "after-val",
		phase: PhaseAfterValidation,
		fn:    func(ctx context.Context, msg *message.Message) (*message.Message, error) { return msg, nil },
	})
	reg.Register(&testPlugin{
		name:  "before-dest",
		phase: PhaseBeforeDestination,
		fn:    func(ctx context.Context, msg *message.Message) (*message.Message, error) { return msg, nil },
	})

	if !reg.HasPlugins(PhaseBeforeValidation) {
		t.Error("expected plugins for before_validation")
	}
	if !reg.HasPlugins(PhaseAfterValidation) {
		t.Error("expected plugins for after_validation")
	}
	if !reg.HasPlugins(PhaseBeforeDestination) {
		t.Error("expected plugins for before_destination")
	}
	if reg.HasPlugins(PhaseBeforeTransform) {
		t.Error("expected no plugins for before_transform")
	}
	if reg.HasPlugins(PhaseAfterTransform) {
		t.Error("expected no plugins for after_transform")
	}
	if reg.HasPlugins(PhaseAfterDestination) {
		t.Error("expected no plugins for after_destination")
	}
}

func TestPluginRegistry_ExecuteOrdering(t *testing.T) {
	reg := NewPluginRegistry()

	var order []string
	for i := 0; i < 5; i++ {
		idx := i
		reg.Register(&testPlugin{
			name:  fmt.Sprintf("plugin-%d", idx),
			phase: PhaseAfterTransform,
			fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
				order = append(order, fmt.Sprintf("plugin-%d", idx))
				return msg, nil
			},
		})
	}

	msg := message.New("ch-1", []byte("test"))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	_, err := reg.Execute(context.Background(), PhaseAfterTransform, msg, logger)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if len(order) != 5 {
		t.Fatalf("expected 5 plugins executed, got %d", len(order))
	}
	for i, name := range order {
		expected := fmt.Sprintf("plugin-%d", i)
		if name != expected {
			t.Errorf("order[%d] = %q, want %q", i, name, expected)
		}
	}
}

func TestPluginRegistry_ExecuteEmptyPhase(t *testing.T) {
	reg := NewPluginRegistry()
	msg := message.New("ch-1", []byte("test"))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	result, err := reg.Execute(context.Background(), PhaseAfterDestination, msg, logger)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result != msg {
		t.Error("executing empty phase should return original message")
	}
}

func TestPluginRegistry_ExecuteWithError(t *testing.T) {
	reg := NewPluginRegistry()

	reg.Register(&testPlugin{
		name:  "good-plugin",
		phase: PhaseBeforeTransform,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			return msg, nil
		},
	})
	reg.Register(&testPlugin{
		name:  "bad-plugin",
		phase: PhaseBeforeTransform,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			return nil, fmt.Errorf("plugin error")
		},
	})
	reg.Register(&testPlugin{
		name:  "never-reached",
		phase: PhaseBeforeTransform,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			t.Error("this plugin should not be reached")
			return msg, nil
		},
	})

	msg := message.New("ch-1", []byte("test"))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	_, err := reg.Execute(context.Background(), PhaseBeforeTransform, msg, logger)
	if err == nil {
		t.Fatal("expected error from failing plugin")
	}
}

func TestPluginRegistry_ExecuteModifiesMessage(t *testing.T) {
	reg := NewPluginRegistry()

	reg.Register(&testPlugin{
		name:  "modifier-1",
		phase: PhaseBeforeValidation,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			if msg.Metadata == nil {
				msg.Metadata = make(map[string]any)
			}
			msg.Metadata["step1"] = true
			return msg, nil
		},
	})
	reg.Register(&testPlugin{
		name:  "modifier-2",
		phase: PhaseBeforeValidation,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			msg.Metadata["step2"] = true
			if msg.Metadata["step1"] != true {
				t.Error("step1 should have been set by previous plugin")
			}
			return msg, nil
		},
	})

	msg := message.New("ch-1", []byte("test"))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	result, err := reg.Execute(context.Background(), PhaseBeforeValidation, msg, logger)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Metadata["step1"] != true || result.Metadata["step2"] != true {
		t.Errorf("metadata = %v, want step1=true and step2=true", result.Metadata)
	}
}

func TestPluginRegistry_RegisterAndRetrieve(t *testing.T) {
	reg := NewPluginRegistry()

	p1 := &testPlugin{name: "p1", phase: PhaseBeforeValidation}
	p2 := &testPlugin{name: "p2", phase: PhaseBeforeValidation}
	p3 := &testPlugin{name: "p3", phase: PhaseAfterTransform}

	reg.Register(p1)
	reg.Register(p2)
	reg.Register(p3)

	if !reg.HasPlugins(PhaseBeforeValidation) {
		t.Error("expected plugins for before_validation")
	}
	if !reg.HasPlugins(PhaseAfterTransform) {
		t.Error("expected plugins for after_transform")
	}

	bvPlugins := reg.plugins[PhaseBeforeValidation]
	if len(bvPlugins) != 2 {
		t.Fatalf("expected 2 before_validation plugins, got %d", len(bvPlugins))
	}
	if bvPlugins[0].Name() != "p1" {
		t.Errorf("first plugin = %q, want p1", bvPlugins[0].Name())
	}
	if bvPlugins[1].Name() != "p2" {
		t.Errorf("second plugin = %q, want p2", bvPlugins[1].Name())
	}

	atPlugins := reg.plugins[PhaseAfterTransform]
	if len(atPlugins) != 1 {
		t.Fatalf("expected 1 after_transform plugin, got %d", len(atPlugins))
	}
}

func TestPluginRegistry_ExecuteNilResult(t *testing.T) {
	reg := NewPluginRegistry()

	reg.Register(&testPlugin{
		name:  "nil-result",
		phase: PhaseAfterDestination,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			return nil, nil
		},
	})
	reg.Register(&testPlugin{
		name:  "after-nil",
		phase: PhaseAfterDestination,
		fn: func(ctx context.Context, msg *message.Message) (*message.Message, error) {
			return msg, nil
		},
	})

	msg := message.New("ch-1", []byte("test"))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	result, err := reg.Execute(context.Background(), PhaseAfterDestination, msg, logger)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result != msg {
		t.Error("when plugin returns nil, pipeline should continue with previous message")
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

func TestNewScriptPlugin_InvalidPhases(t *testing.T) {
	invalidPhases := []string{
		"",
		"invalid",
		"before",
		"after",
		"validation",
		"Before_Validation",
		"BEFORE_VALIDATION",
		"pre_validation",
		"post_transform",
	}

	for _, p := range invalidPhases {
		_, err := NewScriptPlugin(config.PluginConfig{
			Name:       "bad",
			Phase:      p,
			Entrypoint: "handler.ts",
		}, "/tmp", "/tmp", nil, slog.Default())
		if err == nil {
			t.Errorf("expected error for invalid phase %q, got nil", p)
		}
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

func TestNewScriptPlugin_AllValidPhases(t *testing.T) {
	phases := []string{
		"before_validation", "after_validation",
		"before_transform", "after_transform",
		"before_destination", "after_destination",
	}

	for _, p := range phases {
		sp, err := NewScriptPlugin(config.PluginConfig{
			Name:       "test-" + p,
			Phase:      p,
			Entrypoint: "handler.ts",
		}, "/project/channels/ch", "/project", nil, slog.Default())
		if err != nil {
			t.Fatalf("unexpected error for phase %s: %v", p, err)
		}
		if sp.Name() != "test-"+p {
			t.Errorf("Name() = %q for phase %s", sp.Name(), p)
		}
		if string(sp.Phase()) != p {
			t.Errorf("Phase() = %q, want %q", sp.Phase(), p)
		}
		if sp.entrypoint != "handler.ts" {
			t.Errorf("entrypoint = %q, want handler.ts", sp.entrypoint)
		}
		if sp.channelDir != "/project/channels/ch" {
			t.Errorf("channelDir = %q", sp.channelDir)
		}
		if sp.projectDir != "/project" {
			t.Errorf("projectDir = %q", sp.projectDir)
		}
	}
}

func TestNewScriptPlugin_NilRunner(t *testing.T) {
	sp, err := NewScriptPlugin(config.PluginConfig{
		Name:       "no-runner",
		Phase:      "before_validation",
		Entrypoint: "handler.ts",
	}, "/tmp/ch", "/tmp", nil, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sp.runner != nil {
		t.Error("runner should be nil")
	}
}

func TestScriptPlugin_ResolveScriptPath_TSFile(t *testing.T) {
	sp := &ScriptPlugin{
		channelDir: "/project/channels/my-channel",
		projectDir: "/project",
	}

	path := sp.resolveScriptPath("handler.ts")
	expected := filepath.Join("/project", "dist", "channels/my-channel", "handler.js")
	if path != expected {
		t.Errorf("resolveScriptPath(handler.ts) = %q, want %q", path, expected)
	}
}

func TestScriptPlugin_ResolveScriptPath_JSFile(t *testing.T) {
	sp := &ScriptPlugin{
		channelDir: "/project/channels/my-channel",
		projectDir: "/project",
	}

	path := sp.resolveScriptPath("handler.js")
	expected := filepath.Join("/project/channels/my-channel", "handler.js")
	if path != expected {
		t.Errorf("resolveScriptPath(handler.js) = %q, want %q", path, expected)
	}
}

func TestScriptPlugin_ResolveScriptPath_NestedChannel(t *testing.T) {
	sp := &ScriptPlugin{
		channelDir: "/project/channels/group/sub-channel",
		projectDir: "/project",
	}

	path := sp.resolveScriptPath("plugin.ts")
	expected := filepath.Join("/project", "dist", "channels/group/sub-channel", "plugin.js")
	if path != expected {
		t.Errorf("resolveScriptPath(nested ts) = %q, want %q", path, expected)
	}
}

func TestScriptPlugin_ResolveScriptPath_OtherExtension(t *testing.T) {
	sp := &ScriptPlugin{
		channelDir: "/project/channels/ch",
		projectDir: "/project",
	}

	path := sp.resolveScriptPath("config.json")
	expected := filepath.Join("/project/channels/ch", "config.json")
	if path != expected {
		t.Errorf("resolveScriptPath(config.json) = %q, want %q", path, expected)
	}
}

func TestScriptPlugin_NameAndPhase(t *testing.T) {
	sp, err := NewScriptPlugin(config.PluginConfig{
		Name:       "my-plugin",
		Phase:      "after_destination",
		Entrypoint: "post.ts",
	}, "/ch", "/proj", nil, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	if sp.Name() != "my-plugin" {
		t.Errorf("Name() = %q, want my-plugin", sp.Name())
	}
	if sp.Phase() != PhaseAfterDestination {
		t.Errorf("Phase() = %q, want after_destination", sp.Phase())
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
