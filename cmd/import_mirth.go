package cmd

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"log/slog"

	"github.com/intuware/intu-dev/pkg/config"
	"github.com/intuware/intu-dev/pkg/logging"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newImportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import channels from external systems",
	}
	cmd.AddCommand(newImportMirthCmd())
	return cmd
}

func newImportMirthCmd() *cobra.Command {
	var dir string
	var overwrite bool

	cmd := &cobra.Command{
		Use:   "mirth <channel.xml>",
		Short: "Import a Mirth Connect channel XML export",
		Long: `Parse a Mirth Connect channel XML export and generate an equivalent
intu channel directory with channel.yaml, transformer.ts, and validator.ts.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.New(rootOpts.logLevel, nil)

			xmlPath := args[0]
			data, err := os.ReadFile(xmlPath)
			if err != nil {
				return fmt.Errorf("read file %s: %w", xmlPath, err)
			}

			ch, warnings, err := parseMirthChannel(data)
			if err != nil {
				return fmt.Errorf("parse Mirth XML: %w", err)
			}

			for _, w := range warnings {
				fmt.Fprintf(cmd.OutOrStdout(), "WARNING: %s\n", w)
			}

			loader := config.NewLoader(dir)
			cfg, err := loader.Load("dev")
			if err != nil {
				cfg = &config.Config{ChannelsDir: "channels"}
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)
			channelDir := filepath.Join(channelsDir, ch.ID)

			if _, err := os.Stat(channelDir); err == nil && !overwrite {
				return fmt.Errorf("channel directory %s already exists (use --overwrite to replace)", channelDir)
			}

			if err := os.MkdirAll(channelDir, 0o755); err != nil {
				return fmt.Errorf("create channel dir: %w", err)
			}

			if err := writeChannelYAML(channelDir, ch); err != nil {
				return fmt.Errorf("write channel.yaml: %w", err)
			}

			if ch.TransformerCode != "" {
				if err := writeTransformerTS(channelDir, ch.TransformerCode); err != nil {
					return fmt.Errorf("write transformer.ts: %w", err)
				}
			}

			if ch.FilterCode != "" {
				if err := writeValidatorTS(channelDir, ch.FilterCode); err != nil {
					return fmt.Errorf("write validator.ts: %w", err)
				}
			}

			logger.Info("Mirth channel imported",
				"source", xmlPath,
				"id", ch.ID,
				"name", ch.Name,
				"outputDir", channelDir,
			)

			fmt.Fprintf(cmd.OutOrStdout(), "Successfully imported Mirth channel '%s' -> %s\n", ch.Name, channelDir)
			fmt.Fprintf(cmd.OutOrStdout(), "  Channel ID:   %s\n", ch.ID)
			fmt.Fprintf(cmd.OutOrStdout(), "  Listener:     %s\n", ch.ListenerType)
			fmt.Fprintf(cmd.OutOrStdout(), "  Destinations: %d\n", len(ch.Destinations))
			if ch.TransformerCode != "" {
				fmt.Fprintln(cmd.OutOrStdout(), "  Transformer:  transformer.ts (generated)")
			}
			if ch.FilterCode != "" {
				fmt.Fprintln(cmd.OutOrStdout(), "  Validator:    validator.ts (generated)")
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite existing channel directory")
	return cmd
}

type mirthImportChannel struct {
	ID              string
	Name            string
	ListenerType    string
	ListenerConfig  map[string]any
	Destinations    []mirthImportDest
	TransformerCode string
	FilterCode      string
}

type mirthImportDest struct {
	Name   string
	Type   string
	Config map[string]any
}

type mirthXMLChannel struct {
	XMLName               xml.Name          `xml:"channel"`
	ID                    string            `xml:"id"`
	Name                  string            `xml:"name"`
	Description           string            `xml:"description"`
	Enabled               string            `xml:"enabled"`
	SourceConnector       mirthXMLConnector `xml:"sourceConnector"`
	DestinationConnectors mirthXMLDestList  `xml:"destinationConnectors"`
}

type mirthXMLDestList struct {
	Connectors []mirthXMLConnector `xml:"connector"`
}

type mirthXMLConnector struct {
	Name          string              `xml:"name"`
	Mode          string              `xml:"mode"`
	Enabled       string              `xml:"enabled"`
	TransportName string              `xml:"transportName"`
	Properties    mirthXMLProperties  `xml:"properties"`
	Transformer   mirthXMLTransformer `xml:"transformer"`
	Filter        mirthXMLFilter      `xml:"filter"`
}

type mirthXMLProperties struct {
	XMLName             xml.Name `xml:"properties"`
	ClassName           string   `xml:"class,attr"`
	ListenerAddress     string   `xml:"listenerAddress"`
	ListenerPort        string   `xml:"listenerPort"`
	Host                string   `xml:"host"`
	Port                string   `xml:"port"`
	RemoteAddress       string   `xml:"remoteAddress"`
	RemotePort          string   `xml:"remotePort"`
	Directory           string   `xml:"directory"`
	FilePattern         string   `xml:"fileFilter"`
	MoveToDir           string   `xml:"moveToDirectory"`
	PollInterval        string   `xml:"pollingFrequency"`
	Method              string   `xml:"method"`
	URL                 string   `xml:"url"`
	CharsetEncoding     string   `xml:"charsetEncoding"`
	ResponseContentType string   `xml:"responseContentType"`
	Mode                string   `xml:"mode"`
	DatabaseDriver      string   `xml:"driver"`
	DatabaseURL         string   `xml:"URL"`
	Query               string   `xml:"query"`
	Topic               string   `xml:"topic"`
	Brokers             string   `xml:"bootstrap.servers"`
	GroupID             string   `xml:"group.id"`
}

type mirthXMLTransformer struct {
	Steps []mirthXMLStep `xml:"steps>step"`
}

type mirthXMLFilter struct {
	Rules []mirthXMLRule `xml:"rules>rule"`
}

type mirthXMLStep struct {
	Name   string `xml:"name"`
	Type   string `xml:"type"`
	Script string `xml:"script"`
}

type mirthXMLRule struct {
	Name     string `xml:"name"`
	Type     string `xml:"type"`
	Script   string `xml:"script"`
	Operator string `xml:"operator"`
}

func parseMirthChannel(data []byte) (*mirthImportChannel, []string, error) {
	var warnings []string

	var mch mirthXMLChannel
	if err := xml.Unmarshal(data, &mch); err != nil {
		return nil, nil, fmt.Errorf("unmarshal XML: %w", err)
	}

	ch := &mirthImportChannel{
		ID:   sanitizeID(mch.Name),
		Name: mch.Name,
	}

	if mch.ID != "" && ch.ID == "" {
		ch.ID = sanitizeID(mch.ID)
	}

	ch.ListenerType, ch.ListenerConfig, warnings = mapMirthSourceConnector(mch.SourceConnector, warnings)

	var transformerParts []string
	for _, step := range mch.SourceConnector.Transformer.Steps {
		if step.Script != "" {
			transformerParts = append(transformerParts, fmt.Sprintf("// Step: %s (type: %s)\n%s", step.Name, step.Type, step.Script))
		}
	}
	if len(transformerParts) > 0 {
		ch.TransformerCode = strings.Join(transformerParts, "\n\n")
	}

	var filterParts []string
	for _, rule := range mch.SourceConnector.Filter.Rules {
		if rule.Script != "" {
			filterParts = append(filterParts, fmt.Sprintf("// Rule: %s (type: %s)\n%s", rule.Name, rule.Type, rule.Script))
		}
	}
	if len(filterParts) > 0 {
		ch.FilterCode = strings.Join(filterParts, "\n\n")
	}

	for _, dc := range mch.DestinationConnectors.Connectors {
		dest, w := mapMirthDestConnector(dc)
		ch.Destinations = append(ch.Destinations, dest)
		warnings = append(warnings, w...)
	}

	return ch, warnings, nil
}

func mapMirthSourceConnector(sc mirthXMLConnector, warnings []string) (string, map[string]any, []string) {
	transport := strings.ToLower(sc.TransportName)
	props := sc.Properties
	cfg := make(map[string]any)

	switch {
	case strings.Contains(transport, "http"):
		listenerType := "http"
		if props.ListenerPort != "" {
			cfg["port"] = props.ListenerPort
		}
		return listenerType, cfg, warnings

	case strings.Contains(transport, "tcp") || strings.Contains(transport, "mllp"):
		listenerType := "tcp"
		if props.ListenerPort != "" {
			cfg["port"] = props.ListenerPort
		}
		if strings.Contains(transport, "mllp") || props.Mode == "MLLP" {
			cfg["mode"] = "mllp"
		}
		return listenerType, cfg, warnings

	case strings.Contains(transport, "file"):
		listenerType := "file"
		if props.Directory != "" {
			cfg["directory"] = props.Directory
		}
		if props.FilePattern != "" {
			cfg["file_pattern"] = props.FilePattern
		}
		if props.PollInterval != "" {
			cfg["poll_interval"] = props.PollInterval + "ms"
		}
		return listenerType, cfg, warnings

	case strings.Contains(transport, "database") || strings.Contains(transport, "jdbc"):
		listenerType := "database"
		if props.DatabaseDriver != "" {
			cfg["driver"] = mapMirthDBDriver(props.DatabaseDriver)
		}
		if props.DatabaseURL != "" {
			cfg["dsn"] = props.DatabaseURL
		}
		if props.Query != "" {
			cfg["query"] = props.Query
		}
		return listenerType, cfg, warnings

	case strings.Contains(transport, "kafka"):
		listenerType := "kafka"
		if props.Topic != "" {
			cfg["topic"] = props.Topic
		}
		if props.Brokers != "" {
			cfg["brokers"] = strings.Split(props.Brokers, ",")
		}
		if props.GroupID != "" {
			cfg["group_id"] = props.GroupID
		}
		return listenerType, cfg, warnings

	case strings.Contains(transport, "dicom"):
		listenerType := "dicom"
		if props.ListenerPort != "" {
			cfg["port"] = props.ListenerPort
		}
		return listenerType, cfg, warnings

	default:
		warnings = append(warnings, fmt.Sprintf("Unsupported source connector type: %s (mapped to http)", sc.TransportName))
		return "http", cfg, warnings
	}
}

func mapMirthDestConnector(dc mirthXMLConnector) (mirthImportDest, []string) {
	var warnings []string
	transport := strings.ToLower(dc.TransportName)
	props := dc.Properties
	cfg := make(map[string]any)

	name := sanitizeID(dc.Name)
	if name == "" {
		name = "destination"
	}

	destType := "http"

	switch {
	case strings.Contains(transport, "http"):
		destType = "http"
		if props.URL != "" {
			cfg["url"] = props.URL
		}
		if props.Method != "" {
			cfg["method"] = strings.ToUpper(props.Method)
		}

	case strings.Contains(transport, "tcp") || strings.Contains(transport, "mllp"):
		destType = "tcp"
		if props.RemoteAddress != "" || props.Host != "" {
			host := props.RemoteAddress
			if host == "" {
				host = props.Host
			}
			cfg["host"] = host
		}
		if props.RemotePort != "" || props.Port != "" {
			port := props.RemotePort
			if port == "" {
				port = props.Port
			}
			cfg["port"] = port
		}
		if strings.Contains(transport, "mllp") || props.Mode == "MLLP" {
			cfg["mode"] = "mllp"
		}

	case strings.Contains(transport, "file"):
		destType = "file"
		if props.Directory != "" {
			cfg["directory"] = props.Directory
		}
		if props.FilePattern != "" {
			cfg["filename_pattern"] = props.FilePattern
		}

	case strings.Contains(transport, "database") || strings.Contains(transport, "jdbc"):
		destType = "database"
		if props.DatabaseDriver != "" {
			cfg["driver"] = mapMirthDBDriver(props.DatabaseDriver)
		}
		if props.DatabaseURL != "" {
			cfg["dsn"] = props.DatabaseURL
		}
		if props.Query != "" {
			cfg["statement"] = props.Query
		}

	case strings.Contains(transport, "kafka"):
		destType = "kafka"
		if props.Topic != "" {
			cfg["topic"] = props.Topic
		}
		if props.Brokers != "" {
			cfg["brokers"] = strings.Split(props.Brokers, ",")
		}

	case strings.Contains(transport, "smtp") || strings.Contains(transport, "email"):
		destType = "smtp"
		if props.Host != "" {
			cfg["host"] = props.Host
		}
		if props.Port != "" {
			cfg["port"] = props.Port
		}

	case strings.Contains(transport, "dicom"):
		destType = "dicom"
		if props.Host != "" {
			cfg["host"] = props.Host
		}
		if props.Port != "" {
			cfg["port"] = props.Port
		}

	case strings.Contains(transport, "jms"):
		destType = "jms"
		if props.URL != "" {
			cfg["url"] = props.URL
		}
		warnings = append(warnings, fmt.Sprintf("JMS destination '%s': manual configuration review required", name))

	default:
		warnings = append(warnings, fmt.Sprintf("Unsupported destination connector type: %s (mapped to http)", dc.TransportName))
	}

	return mirthImportDest{
		Name:   name,
		Type:   destType,
		Config: cfg,
	}, warnings
}

func mapMirthDBDriver(driver string) string {
	d := strings.ToLower(driver)
	switch {
	case strings.Contains(d, "postgres"):
		return "postgres"
	case strings.Contains(d, "mysql"):
		return "mysql"
	case strings.Contains(d, "oracle"):
		return "oracle"
	case strings.Contains(d, "sqlserver") || strings.Contains(d, "jtds"):
		return "sqlserver"
	default:
		return driver
	}
}

func writeChannelYAML(channelDir string, ch *mirthImportChannel) error {
	yamlCfg := map[string]any{
		"id":      ch.ID,
		"enabled": true,
	}

	listener := map[string]any{
		"type": ch.ListenerType,
	}
	if len(ch.ListenerConfig) > 0 {
		listener[ch.ListenerType] = ch.ListenerConfig
	}
	yamlCfg["listener"] = listener

	if ch.TransformerCode != "" || ch.FilterCode != "" {
		pipeline := map[string]any{}
		if ch.TransformerCode != "" {
			pipeline["transformer"] = "transformer.ts"
		}
		if ch.FilterCode != "" {
			pipeline["validator"] = "validator.ts"
		}
		yamlCfg["pipeline"] = pipeline
	}

	if len(ch.Destinations) > 0 {
		var dests []map[string]any
		for _, d := range ch.Destinations {
			dest := map[string]any{
				"name": d.Name,
				"ref":  d.Name,
			}
			dests = append(dests, dest)
		}
		yamlCfg["destinations"] = dests
	}

	data, err := yaml.Marshal(yamlCfg)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(channelDir, "channel.yaml"), data, 0o644)
}

func writeTransformerTS(channelDir string, jsCode string) error {
	ts := fmt.Sprintf(`// Auto-generated from Mirth Connect channel export
// Original JavaScript transformer code wrapped in TypeScript

export default function transform(msg: any, ctx: any): any {
  // --- Original Mirth JavaScript ---
  %s
  // --- End Original Mirth JavaScript ---

  return msg;
}
`, indentCode(jsCode, "  "))

	return os.WriteFile(filepath.Join(channelDir, "transformer.ts"), []byte(ts), 0o644)
}

func writeValidatorTS(channelDir string, jsCode string) error {
	ts := fmt.Sprintf(`// Auto-generated from Mirth Connect channel export
// Original JavaScript filter code wrapped in TypeScript

export default function validate(msg: any, ctx: any): boolean {
  // --- Original Mirth JavaScript ---
  %s
  // --- End Original Mirth JavaScript ---

  return true;
}
`, indentCode(jsCode, "  "))

	return os.WriteFile(filepath.Join(channelDir, "validator.ts"), []byte(ts), 0o644)
}

func indentCode(code, indent string) string {
	lines := strings.Split(code, "\n")
	for i, l := range lines {
		if i > 0 {
			lines[i] = indent + l
		}
	}
	return strings.Join(lines, "\n")
}

func sanitizeID(name string) string {
	result := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		if r == ' ' {
			return '-'
		}
		return -1
	}, name)
	return strings.ToLower(strings.Trim(result, "-_"))
}

func buildAuthMiddleware(cfg *config.Config, logger *slog.Logger) func(http.Handler) http.Handler {
	if cfg.AccessControl == nil || !cfg.AccessControl.Enabled {
		return nil
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				if cfg.AccessControl.Provider == "ldap" {
					user, pass, ok := r.BasicAuth()
					if !ok {
						w.Header().Set("WWW-Authenticate", `Basic realm="intu dashboard"`)
						http.Error(w, "authentication required", http.StatusUnauthorized)
						return
					}
					_ = user
					_ = pass
					logger.Debug("auth check", "user", user, "provider", "ldap")
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
