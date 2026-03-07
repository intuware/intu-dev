package runtime

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

type CodeTemplateLibrary struct {
	Name      string
	Directory string
	Functions map[string]string
}

type CodeTemplateLoader struct {
	projectDir string
	libraries  map[string]*CodeTemplateLibrary
	logger     *slog.Logger
}

func NewCodeTemplateLoader(projectDir string, logger *slog.Logger) *CodeTemplateLoader {
	return &CodeTemplateLoader{
		projectDir: projectDir,
		libraries:  make(map[string]*CodeTemplateLibrary),
		logger:     logger,
	}
}

// LoadLibrary loads all compiled JS files from a code template library directory.
func (ctl *CodeTemplateLoader) LoadLibrary(name, dir string) error {
	absDir := filepath.Join(ctl.projectDir, dir)
	distDir := filepath.Join(ctl.projectDir, "dist", dir)

	if _, err := os.Stat(absDir); os.IsNotExist(err) {
		return fmt.Errorf("code template library directory %q not found", absDir)
	}

	lib := &CodeTemplateLibrary{
		Name:      name,
		Directory: dir,
		Functions: make(map[string]string),
	}

	entries, err := os.ReadDir(absDir)
	if err != nil {
		return fmt.Errorf("read code template library %s: %w", name, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || (!strings.HasSuffix(entry.Name(), ".ts") && !strings.HasSuffix(entry.Name(), ".js")) {
			continue
		}

		baseName := strings.TrimSuffix(strings.TrimSuffix(entry.Name(), ".ts"), ".js")
		jsFile := filepath.Join(distDir, baseName+".js")

		if _, err := os.Stat(jsFile); err == nil {
			lib.Functions[baseName] = jsFile
		} else {
			srcFile := filepath.Join(absDir, entry.Name())
			lib.Functions[baseName] = srcFile
		}
	}

	ctl.libraries[name] = lib
	ctl.logger.Debug("loaded code template library", "name", name, "functions", len(lib.Functions))
	return nil
}

// GetLibrary returns a loaded library by name.
func (ctl *CodeTemplateLoader) GetLibrary(name string) (*CodeTemplateLibrary, bool) {
	lib, ok := ctl.libraries[name]
	return lib, ok
}

// Libraries returns all loaded libraries.
func (ctl *CodeTemplateLoader) Libraries() map[string]*CodeTemplateLibrary {
	return ctl.libraries
}

// ResolveFunction finds a function file across all loaded libraries.
func (ctl *CodeTemplateLoader) ResolveFunction(funcName string) (string, bool) {
	for _, lib := range ctl.libraries {
		if path, ok := lib.Functions[funcName]; ok {
			return path, true
		}
	}
	return "", false
}
