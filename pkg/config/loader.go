package config

import (
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

func (l *Loader) Load(profile string) (*Config, error) {
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
