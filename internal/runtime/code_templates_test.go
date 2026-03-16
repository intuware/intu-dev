package runtime

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestNewCodeTemplateLoader(t *testing.T) {
	ctl := NewCodeTemplateLoader("/tmp/project", slog.Default())
	if ctl == nil {
		t.Fatal("expected non-nil CodeTemplateLoader")
	}
	if ctl.projectDir != "/tmp/project" {
		t.Fatalf("expected projectDir /tmp/project, got %s", ctl.projectDir)
	}
	if len(ctl.libraries) != 0 {
		t.Fatalf("expected empty libraries map, got %d", len(ctl.libraries))
	}
}

func TestLoadLibraryMissingDir(t *testing.T) {
	ctl := NewCodeTemplateLoader(t.TempDir(), slog.Default())
	err := ctl.LoadLibrary("missing-lib", "nonexistent-dir")
	if err == nil {
		t.Fatal("expected error for missing directory")
	}
}

func TestLoadLibraryValidDirWithFiles(t *testing.T) {
	projectDir := t.TempDir()
	libDir := "code-templates"
	absLibDir := filepath.Join(projectDir, libDir)
	if err := os.MkdirAll(absLibDir, 0755); err != nil {
		t.Fatalf("failed to create lib dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(absLibDir, "helper.ts"), []byte("export function helper() {}"), 0644); err != nil {
		t.Fatalf("failed to write helper.ts: %v", err)
	}
	if err := os.WriteFile(filepath.Join(absLibDir, "utils.js"), []byte("function utils() {}"), 0644); err != nil {
		t.Fatalf("failed to write utils.js: %v", err)
	}
	if err := os.WriteFile(filepath.Join(absLibDir, "readme.md"), []byte("# readme"), 0644); err != nil {
		t.Fatalf("failed to write readme.md: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(absLibDir, "subdir"), 0755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}

	ctl := NewCodeTemplateLoader(projectDir, slog.Default())
	if err := ctl.LoadLibrary("mylib", libDir); err != nil {
		t.Fatalf("LoadLibrary failed: %v", err)
	}

	lib, ok := ctl.GetLibrary("mylib")
	if !ok {
		t.Fatal("expected to find loaded library")
	}
	if len(lib.Functions) != 2 {
		t.Fatalf("expected 2 functions (helper, utils), got %d", len(lib.Functions))
	}
	if _, ok := lib.Functions["helper"]; !ok {
		t.Fatal("expected 'helper' function entry")
	}
	if _, ok := lib.Functions["utils"]; !ok {
		t.Fatal("expected 'utils' function entry")
	}
}

func TestLoadLibraryDistDir(t *testing.T) {
	projectDir := t.TempDir()
	libDir := "code-templates"
	absLibDir := filepath.Join(projectDir, libDir)
	distDir := filepath.Join(projectDir, "dist", libDir)

	if err := os.MkdirAll(absLibDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(distDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(absLibDir, "transform.ts"), []byte("export function transform() {}"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(distDir, "transform.js"), []byte("function transform() {}"), 0644); err != nil {
		t.Fatal(err)
	}

	ctl := NewCodeTemplateLoader(projectDir, slog.Default())
	if err := ctl.LoadLibrary("compiled-lib", libDir); err != nil {
		t.Fatalf("LoadLibrary failed: %v", err)
	}

	lib, ok := ctl.GetLibrary("compiled-lib")
	if !ok {
		t.Fatal("expected to find library")
	}

	path, ok := lib.Functions["transform"]
	if !ok {
		t.Fatal("expected 'transform' function")
	}
	expectedPath := filepath.Join(distDir, "transform.js")
	if path != expectedPath {
		t.Fatalf("expected compiled JS path %q, got %q", expectedPath, path)
	}
}

func TestLoadLibraryFallbackToSource(t *testing.T) {
	projectDir := t.TempDir()
	libDir := "code-templates"
	absLibDir := filepath.Join(projectDir, libDir)

	if err := os.MkdirAll(absLibDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(absLibDir, "raw.ts"), []byte("export function raw() {}"), 0644); err != nil {
		t.Fatal(err)
	}

	ctl := NewCodeTemplateLoader(projectDir, slog.Default())
	if err := ctl.LoadLibrary("src-lib", libDir); err != nil {
		t.Fatalf("LoadLibrary failed: %v", err)
	}

	lib, _ := ctl.GetLibrary("src-lib")
	path := lib.Functions["raw"]
	expectedPath := filepath.Join(absLibDir, "raw.ts")
	if path != expectedPath {
		t.Fatalf("expected source TS path %q, got %q", expectedPath, path)
	}
}

func TestGetLibraryFound(t *testing.T) {
	projectDir := t.TempDir()
	libDir := "lib"
	if err := os.MkdirAll(filepath.Join(projectDir, libDir), 0755); err != nil {
		t.Fatal(err)
	}

	ctl := NewCodeTemplateLoader(projectDir, slog.Default())
	if err := ctl.LoadLibrary("test-lib", libDir); err != nil {
		t.Fatal(err)
	}

	lib, ok := ctl.GetLibrary("test-lib")
	if !ok {
		t.Fatal("expected to find library")
	}
	if lib.Name != "test-lib" {
		t.Fatalf("expected name 'test-lib', got %q", lib.Name)
	}
}

func TestGetLibraryNotFound(t *testing.T) {
	ctl := NewCodeTemplateLoader(t.TempDir(), slog.Default())
	_, ok := ctl.GetLibrary("nonexistent")
	if ok {
		t.Fatal("expected ok=false for missing library")
	}
}

func TestLibraries(t *testing.T) {
	projectDir := t.TempDir()

	for _, dir := range []string{"lib-a", "lib-b"} {
		if err := os.MkdirAll(filepath.Join(projectDir, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}

	ctl := NewCodeTemplateLoader(projectDir, slog.Default())
	ctl.LoadLibrary("a", "lib-a")
	ctl.LoadLibrary("b", "lib-b")

	libs := ctl.Libraries()
	if len(libs) != 2 {
		t.Fatalf("expected 2 libraries, got %d", len(libs))
	}
	if _, ok := libs["a"]; !ok {
		t.Fatal("expected library 'a'")
	}
	if _, ok := libs["b"]; !ok {
		t.Fatal("expected library 'b'")
	}
}

func TestResolveFunctionFound(t *testing.T) {
	projectDir := t.TempDir()
	libDir := "funcs"
	absLibDir := filepath.Join(projectDir, libDir)
	if err := os.MkdirAll(absLibDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(absLibDir, "myFunc.ts"), []byte("export default function myFunc(){}"), 0644); err != nil {
		t.Fatal(err)
	}

	ctl := NewCodeTemplateLoader(projectDir, slog.Default())
	ctl.LoadLibrary("func-lib", libDir)

	path, ok := ctl.ResolveFunction("myFunc")
	if !ok {
		t.Fatal("expected to resolve function 'myFunc'")
	}
	if path == "" {
		t.Fatal("expected non-empty path")
	}
}

func TestResolveFunctionNotFound(t *testing.T) {
	ctl := NewCodeTemplateLoader(t.TempDir(), slog.Default())
	_, ok := ctl.ResolveFunction("nonexistent")
	if ok {
		t.Fatal("expected ok=false for unresolvable function")
	}
}

func TestResolveFunctionAcrossLibraries(t *testing.T) {
	projectDir := t.TempDir()

	for _, dir := range []string{"lib-x", "lib-y"} {
		absDir := filepath.Join(projectDir, dir)
		if err := os.MkdirAll(absDir, 0755); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(filepath.Join(projectDir, "lib-x", "alpha.js"), []byte("function alpha(){}"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "lib-y", "beta.ts"), []byte("export function beta(){}"), 0644); err != nil {
		t.Fatal(err)
	}

	ctl := NewCodeTemplateLoader(projectDir, slog.Default())
	ctl.LoadLibrary("x", "lib-x")
	ctl.LoadLibrary("y", "lib-y")

	if _, ok := ctl.ResolveFunction("alpha"); !ok {
		t.Fatal("expected to resolve 'alpha' from lib-x")
	}
	if _, ok := ctl.ResolveFunction("beta"); !ok {
		t.Fatal("expected to resolve 'beta' from lib-y")
	}
	if _, ok := ctl.ResolveFunction("gamma"); ok {
		t.Fatal("expected ok=false for 'gamma'")
	}
}
