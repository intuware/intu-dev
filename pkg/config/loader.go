package config

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

type Loader struct {
	root string
}

func NewLoader(root string) *Loader {
	return &Loader{root: root}
}

// loadEnvFile reads a .env file and sets env vars that are not already set (OS env wins).
func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		if key == "" {
			continue
		}
		// Remove optional surrounding quotes
		if len(val) >= 2 && (val[0] == '"' && val[len(val)-1] == '"' || val[0] == '\'' && val[len(val)-1] == '\'') {
			val = val[1 : len(val)-1]
		}
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
	return scanner.Err()
}

func (l *Loader) Load(profile string) (*Config, error) {
	// Load .env from project root so ${VAR} in profile and channel YAML resolve (os.ExpandEnv uses process env)
	if err := loadEnvFile(filepath.Join(l.root, ".env")); err != nil {
		return nil, fmt.Errorf("load .env: %w", err)
	}

	v := viper.New()
	v.SetConfigType("yaml")
	v.SetEnvPrefix("INTU")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	basePath := filepath.Join(l.root, "intu.yaml")
	if err := readExpandedYAML(v, basePath, false); err != nil {
		return nil, err
	}

	profilePath := filepath.Join(l.root, fmt.Sprintf("intu.%s.yaml", profile))
	if _, err := os.Stat(profilePath); err == nil {
		if err := readExpandedYAML(v, profilePath, true); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat profile config %s: %w", profilePath, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}

func readExpandedYAML(v *viper.Viper, path string, merge bool) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %s: %w", path, err)
	}

	expanded := os.ExpandEnv(string(raw))
	reader := bytes.NewBufferString(expanded)

	if merge {
		if err := v.MergeConfig(reader); err != nil {
			return fmt.Errorf("merge config file %s: %w", path, err)
		}
		return nil
	}

	if err := v.ReadConfig(reader); err != nil {
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	return nil
}
