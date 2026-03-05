package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type ChannelConfig struct {
	ID      string `yaml:"id"`
	Enabled bool   `yaml:"enabled"`

	Tags     []string `yaml:"tags,omitempty"`
	Group    string   `yaml:"group,omitempty"`
	Priority string   `yaml:"priority,omitempty"`

	DataTypes *DataTypesConfig `yaml:"data_types,omitempty"`
	Listener  ListenerConfig   `yaml:"listener"`

	Pipeline    *PipelineConfig    `yaml:"pipeline,omitempty"`
	Validator   *ScriptRef         `yaml:"validator,omitempty"`
	Transformer *ScriptRef         `yaml:"transformer,omitempty"`

	Destinations []ChannelDestination `yaml:"destinations,omitempty"`

	Logging        *ChannelLogging        `yaml:"logging,omitempty"`
	ErrorHandling  *ErrorHandlingConfig   `yaml:"error_handling,omitempty"`
	Lifecycle      *LifecycleConfig       `yaml:"lifecycle,omitempty"`
	MessageStorage *ChannelStorageConfig  `yaml:"message_storage,omitempty"`
	Batch          *BatchConfig           `yaml:"batch,omitempty"`
	Attachments    *AttachmentsConfig     `yaml:"attachments,omitempty"`
	Tracing        *TracingConfig         `yaml:"tracing,omitempty"`
	Performance    *PerformanceConfig     `yaml:"performance,omitempty"`
	Pruning        *ChannelPruningConfig  `yaml:"pruning,omitempty"`
	DependsOn      []string               `yaml:"depends_on,omitempty"`
	StartupOrder   int                    `yaml:"startup_order,omitempty"`
}

type DataTypesConfig struct {
	Inbound            string         `yaml:"inbound,omitempty"`
	Outbound           string         `yaml:"outbound,omitempty"`
	InboundProperties  map[string]any `yaml:"inbound_properties,omitempty"`
	OutboundProperties map[string]any `yaml:"outbound_properties,omitempty"`
}

type ListenerConfig struct {
	Type     string          `yaml:"type"`
	HTTP     *HTTPListener   `yaml:"http,omitempty"`
	TCP      *TCPListener    `yaml:"tcp,omitempty"`
	SFTP     *SFTPListener   `yaml:"sftp,omitempty"`
	File     *FileListener   `yaml:"file,omitempty"`
	Database *DBListener     `yaml:"database,omitempty"`
	Kafka    *KafkaListener  `yaml:"kafka,omitempty"`
	Channel  *ChannelListener `yaml:"channel,omitempty"`
	Email    *EmailListener  `yaml:"email,omitempty"`
	DICOM    *DICOMListener  `yaml:"dicom,omitempty"`
	SOAP     *SOAPListener   `yaml:"soap,omitempty"`
	FHIR     *FHIRListener   `yaml:"fhir,omitempty"`
	IHE      *IHEListener    `yaml:"ihe,omitempty"`
}

type HTTPListener struct {
	Port    int       `yaml:"port"`
	Path    string    `yaml:"path,omitempty"`
	Methods []string  `yaml:"methods,omitempty"`
	TLS     *TLSConfig `yaml:"tls,omitempty"`
	Auth    *AuthConfig `yaml:"auth,omitempty"`
}

type TCPListener struct {
	Port           int        `yaml:"port"`
	Mode           string     `yaml:"mode,omitempty"`
	MaxConnections int        `yaml:"max_connections,omitempty"`
	TimeoutMs      int        `yaml:"timeout_ms,omitempty"`
	TLS            *TLSConfig `yaml:"tls,omitempty"`
	ACK            *ACKConfig `yaml:"ack,omitempty"`
	Response       *ResponseConfig `yaml:"response,omitempty"`
}

type ACKConfig struct {
	Auto        bool   `yaml:"auto,omitempty"`
	SuccessCode string `yaml:"success_code,omitempty"`
	ErrorCode   string `yaml:"error_code,omitempty"`
	RejectCode  string `yaml:"reject_code,omitempty"`
}

type ResponseConfig struct {
	OnSuccess           string `yaml:"on_success,omitempty"`
	OnError             string `yaml:"on_error,omitempty"`
	OnFilterDrop        string `yaml:"on_filter_drop,omitempty"`
	WaitForDestinations bool   `yaml:"wait_for_destinations,omitempty"`
}

type SFTPListener struct {
	Host         string      `yaml:"host"`
	Port         int         `yaml:"port,omitempty"`
	PollInterval string      `yaml:"poll_interval,omitempty"`
	Directory    string      `yaml:"directory,omitempty"`
	FilePattern  string      `yaml:"file_pattern,omitempty"`
	MoveTo       string      `yaml:"move_to,omitempty"`
	ErrorDir     string      `yaml:"error_dir,omitempty"`
	Auth         *AuthConfig `yaml:"auth,omitempty"`
	SortBy       string      `yaml:"sort_by,omitempty"`
}

type FileListener struct {
	Scheme       string      `yaml:"scheme,omitempty"`
	Directory    string      `yaml:"directory,omitempty"`
	FilePattern  string      `yaml:"file_pattern,omitempty"`
	PollInterval string      `yaml:"poll_interval,omitempty"`
	MoveTo       string      `yaml:"move_to,omitempty"`
	ErrorDir     string      `yaml:"error_dir,omitempty"`
	SortBy       string      `yaml:"sort_by,omitempty"`
	FTP          *FTPConfig  `yaml:"ftp,omitempty"`
	S3           *S3Config   `yaml:"s3,omitempty"`
	SMB          *SMBConfig  `yaml:"smb,omitempty"`
}

type FTPConfig struct {
	Host string      `yaml:"host"`
	Port int         `yaml:"port,omitempty"`
	Auth *AuthConfig `yaml:"auth,omitempty"`
}

type S3Config struct {
	Bucket    string      `yaml:"bucket"`
	Region    string      `yaml:"region,omitempty"`
	Prefix    string      `yaml:"prefix,omitempty"`
	Auth      *AuthConfig `yaml:"auth,omitempty"`
}

type SMBConfig struct {
	Host string      `yaml:"host"`
	Auth *AuthConfig `yaml:"auth,omitempty"`
}

type DBListener struct {
	Driver               string     `yaml:"driver"`
	DSN                  string     `yaml:"dsn"`
	PollInterval         string     `yaml:"poll_interval,omitempty"`
	Query                string     `yaml:"query,omitempty"`
	PostProcessStatement string     `yaml:"post_process_statement,omitempty"`
	TLS                  *TLSConfig `yaml:"tls,omitempty"`
}

type KafkaListener struct {
	Brokers  []string    `yaml:"brokers,omitempty"`
	Topic    string      `yaml:"topic"`
	GroupID  string      `yaml:"group_id,omitempty"`
	Offset   string      `yaml:"offset,omitempty"`
	Auth     *AuthConfig `yaml:"auth,omitempty"`
	TLS      *TLSConfig  `yaml:"tls,omitempty"`
}

type ChannelListener struct {
	SourceChannelID string `yaml:"source_channel_id"`
}

type EmailListener struct {
	Protocol         string      `yaml:"protocol,omitempty"`
	Host             string      `yaml:"host"`
	Port             int         `yaml:"port,omitempty"`
	PollInterval     string      `yaml:"poll_interval,omitempty"`
	TLS              *TLSConfig  `yaml:"tls,omitempty"`
	Auth             *AuthConfig `yaml:"auth,omitempty"`
	Folder           string      `yaml:"folder,omitempty"`
	Filter           string      `yaml:"filter,omitempty"`
	ReadAttachments  bool        `yaml:"read_attachments,omitempty"`
	DeleteAfterRead  bool        `yaml:"delete_after_read,omitempty"`
}

type DICOMListener struct {
	Port    int        `yaml:"port"`
	AETitle string     `yaml:"ae_title,omitempty"`
	TLS     *TLSConfig `yaml:"tls,omitempty"`
}

type SOAPListener struct {
	Port        int        `yaml:"port"`
	WSDLPath    string     `yaml:"wsdl_path,omitempty"`
	ServiceName string     `yaml:"service_name,omitempty"`
	TLS         *TLSConfig `yaml:"tls,omitempty"`
}

type FHIRListener struct {
	Port             int        `yaml:"port"`
	BasePath         string     `yaml:"base_path,omitempty"`
	Version          string     `yaml:"version,omitempty"`
	SubscriptionType string     `yaml:"subscription_type,omitempty"`
	TLS              *TLSConfig `yaml:"tls,omitempty"`
}

type IHEListener struct {
	Profile string     `yaml:"profile"`
	Port    int        `yaml:"port"`
	TLS     *TLSConfig `yaml:"tls,omitempty"`
}

type TLSConfig struct {
	Enabled            bool   `yaml:"enabled,omitempty"`
	CertFile           string `yaml:"cert_file,omitempty"`
	KeyFile            string `yaml:"key_file,omitempty"`
	CAFile             string `yaml:"ca_file,omitempty"`
	ClientCertFile     string `yaml:"client_cert_file,omitempty"`
	ClientKeyFile      string `yaml:"client_key_file,omitempty"`
	MinVersion         string `yaml:"min_version,omitempty"`
	ClientAuth         string `yaml:"client_auth,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty"`
}

type AuthConfig struct {
	Type           string `yaml:"type"`
	Username       string `yaml:"username,omitempty"`
	Password       string `yaml:"password,omitempty"`
	Token          string `yaml:"token,omitempty"`
	Key            string `yaml:"key,omitempty"`
	Header         string `yaml:"header,omitempty"`
	QueryParam     string `yaml:"query_param,omitempty"`
	TokenURL       string `yaml:"token_url,omitempty"`
	AuthURL        string `yaml:"auth_url,omitempty"`
	ClientID       string `yaml:"client_id,omitempty"`
	ClientSecret   string `yaml:"client_secret,omitempty"`
	Scopes         []string `yaml:"scopes,omitempty"`
	RedirectURI    string `yaml:"redirect_uri,omitempty"`
	CAFile         string `yaml:"ca_file,omitempty"`
	ClientCertFile string `yaml:"client_cert_file,omitempty"`
	ClientKeyFile  string `yaml:"client_key_file,omitempty"`
	PrivateKeyFile string `yaml:"private_key_file,omitempty"`
	Passphrase     string `yaml:"passphrase,omitempty"`
	Mechanism      string `yaml:"mechanism,omitempty"`
	Handler        string `yaml:"handler,omitempty"`
	AccessKeyID    string `yaml:"access_key_id,omitempty"`
	SecretAccessKey string `yaml:"secret_access_key,omitempty"`
	Domain         string `yaml:"domain,omitempty"`
}

type PipelineConfig struct {
	Preprocessor string `yaml:"preprocessor,omitempty"`
	SourceFilter string `yaml:"source_filter,omitempty"`
	Transformer  string `yaml:"transformer,omitempty"`
	Postprocessor string `yaml:"postprocessor,omitempty"`
}

type ScriptRef struct {
	Runtime    string `yaml:"runtime,omitempty"`
	Entrypoint string `yaml:"entrypoint,omitempty"`
}

type ChannelDestination struct {
	Name                string      `yaml:"name,omitempty"`
	Ref                 string      `yaml:"ref,omitempty"`
	Type                string      `yaml:"type,omitempty"`
	HTTP                *HTTPDestConfig `yaml:"http,omitempty"`
	Kafka               *KafkaDestConfig `yaml:"kafka,omitempty"`
	TCP                 *TCPDestConfig  `yaml:"tcp,omitempty"`
	File                *FileDestConfig `yaml:"file,omitempty"`
	Database            *DBDestConfig   `yaml:"database,omitempty"`
	SMTP                *SMTPDestConfig `yaml:"smtp,omitempty"`
	ChannelDest         *ChannelDestRef `yaml:"channel,omitempty"`
	DICOM               *DICOMDestConfig `yaml:"dicom,omitempty"`
	JMS                 *JMSDestConfig  `yaml:"jms,omitempty"`
	FHIR                *FHIRDestConfig `yaml:"fhir,omitempty"`
	Direct              *DirectDestConfig `yaml:"direct,omitempty"`
	Filter              string `yaml:"filter,omitempty"`
	TransformerFile     string `yaml:"transformer,omitempty"`
	ResponseTransformer string `yaml:"response_transformer,omitempty"`
	Queue               *QueueConfig   `yaml:"queue,omitempty"`
	Retry               *RetryConfig   `yaml:"retry,omitempty"`
}

func (cd *ChannelDestination) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		cd.Name = value.Value
		cd.Ref = value.Value
		return nil
	}
	type plain ChannelDestination
	return value.Decode((*plain)(cd))
}

type TCPDestConfig struct {
	Host      string     `yaml:"host"`
	Port      int        `yaml:"port"`
	Mode      string     `yaml:"mode,omitempty"`
	TimeoutMs int        `yaml:"timeout_ms,omitempty"`
	TLS       *TLSConfig `yaml:"tls,omitempty"`
}

type FileDestConfig struct {
	Scheme          string         `yaml:"scheme,omitempty"`
	Directory       string         `yaml:"directory,omitempty"`
	FilenamePattern string         `yaml:"filename_pattern,omitempty"`
	SFTP            *SFTPListener  `yaml:"sftp,omitempty"`
}

type DBDestConfig struct {
	Driver    string `yaml:"driver,omitempty"`
	DSN       string `yaml:"dsn,omitempty"`
	Statement string `yaml:"statement,omitempty"`
}

type SMTPDestConfig struct {
	Host    string      `yaml:"host"`
	Port    int         `yaml:"port,omitempty"`
	From    string      `yaml:"from,omitempty"`
	To      []string    `yaml:"to,omitempty"`
	Subject string      `yaml:"subject,omitempty"`
	Auth    *AuthConfig `yaml:"auth,omitempty"`
	TLS     *TLSConfig  `yaml:"tls,omitempty"`
}

type ChannelDestRef struct {
	TargetChannelID string `yaml:"target_channel_id"`
}

type DICOMDestConfig struct {
	Host          string `yaml:"host"`
	Port          int    `yaml:"port,omitempty"`
	AETitle       string `yaml:"ae_title,omitempty"`
	CalledAETitle string `yaml:"called_ae_title,omitempty"`
}

type JMSDestConfig struct {
	Provider string      `yaml:"provider,omitempty"`
	URL      string      `yaml:"url,omitempty"`
	Queue    string      `yaml:"queue,omitempty"`
	Auth     *AuthConfig `yaml:"auth,omitempty"`
}

type FHIRDestConfig struct {
	BaseURL    string      `yaml:"base_url,omitempty"`
	Version    string      `yaml:"version,omitempty"`
	Auth       *AuthConfig `yaml:"auth,omitempty"`
	Operations []string    `yaml:"operations,omitempty"`
	Retry      *RetryConfig `yaml:"retry,omitempty"`
}

type DirectDestConfig struct {
	To          string         `yaml:"to,omitempty"`
	From        string         `yaml:"from,omitempty"`
	SMTP        *SMTPDestConfig `yaml:"smtp,omitempty"`
	Certificate string         `yaml:"certificate,omitempty"`
}

type QueueConfig struct {
	Enabled  bool   `yaml:"enabled,omitempty"`
	MaxSize  int    `yaml:"max_size,omitempty"`
	Overflow string `yaml:"overflow,omitempty"`
	Persist  bool   `yaml:"persist,omitempty"`
	Threads  int    `yaml:"threads,omitempty"`
}

type RetryConfig struct {
	MaxAttempts   int      `yaml:"max_attempts,omitempty"`
	Backoff       string   `yaml:"backoff,omitempty"`
	InitialDelayMs int     `yaml:"initial_delay_ms,omitempty"`
	MaxDelayMs    int      `yaml:"max_delay_ms,omitempty"`
	Jitter        bool     `yaml:"jitter,omitempty"`
	RetryOn       []string `yaml:"retry_on,omitempty"`
	NoRetryOn     []string `yaml:"no_retry_on,omitempty"`
}

type ErrorHandlingConfig struct {
	OnError string         `yaml:"on_error,omitempty"`
	DLQ     *DLQRefConfig  `yaml:"dlq,omitempty"`
	Alert   *AlertRefConfig `yaml:"alert,omitempty"`
}

type DLQRefConfig struct {
	Destination string `yaml:"destination,omitempty"`
}

type AlertRefConfig struct {
	Destination string `yaml:"destination,omitempty"`
}

type LifecycleConfig struct {
	OnDeploy   string `yaml:"on_deploy,omitempty"`
	OnUndeploy string `yaml:"on_undeploy,omitempty"`
}

type ChannelLogging struct {
	Level      string          `yaml:"level,omitempty"`
	Payloads   *PayloadLogging `yaml:"payloads,omitempty"`
	TruncateAt int             `yaml:"truncate_at,omitempty"`
}

type PayloadLogging struct {
	Source      bool `yaml:"source,omitempty"`
	Transformed bool `yaml:"transformed,omitempty"`
	Sent        bool `yaml:"sent,omitempty"`
	Response    bool `yaml:"response,omitempty"`
	Filtered    bool `yaml:"filtered,omitempty"`
}

type ChannelStorageConfig struct {
	Enabled       bool     `yaml:"enabled,omitempty"`
	ContentTypes  []string `yaml:"content_types,omitempty"`
	RetentionDays int      `yaml:"retention_days,omitempty"`
}

type BatchConfig struct {
	Enabled        bool   `yaml:"enabled,omitempty"`
	Type           string `yaml:"type,omitempty"`
	SplitOn        string `yaml:"split_on,omitempty"`
	CustomSplitter string `yaml:"custom_splitter,omitempty"`
	MaxBatchSize   int    `yaml:"max_batch_size,omitempty"`
	BatchTimeoutMs int    `yaml:"batch_timeout_ms,omitempty"`
}

type AttachmentsConfig struct {
	Enabled            bool   `yaml:"enabled,omitempty"`
	Store              string `yaml:"store,omitempty"`
	MaxSizeMB          int    `yaml:"max_size_mb,omitempty"`
	InlineThresholdKB  int    `yaml:"inline_threshold_kb,omitempty"`
	Directory          string `yaml:"directory,omitempty"`
	Bucket             string `yaml:"bucket,omitempty"`
	Region             string `yaml:"region,omitempty"`
}

type TracingConfig struct {
	CorrelationIDHeader string `yaml:"correlation_id_header,omitempty"`
	Propagate           bool   `yaml:"propagate,omitempty"`
}

type PerformanceConfig struct {
	ZeroCopy         bool `yaml:"zero_copy,omitempty"`
	SyncDestinations bool `yaml:"sync_destinations,omitempty"`
}

type ChannelPruningConfig struct {
	RetentionDays int  `yaml:"retention_days,omitempty"`
	PruneErrored  bool `yaml:"prune_errored,omitempty"`
}

func LoadChannelConfig(channelDir string) (*ChannelConfig, error) {
	path := filepath.Join(channelDir, "channel.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read channel config %s: %w", path, err)
	}

	expanded := os.ExpandEnv(string(data))

	var cfg ChannelConfig
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse channel config %s: %w", path, err)
	}

	return &cfg, nil
}
