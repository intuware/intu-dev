package config

type Config struct {
	Runtime        RuntimeConfig            `mapstructure:"runtime"`
	ChannelsDir    string                   `mapstructure:"channels_dir"`
	Destinations   map[string]Destination   `mapstructure:"destinations"`
	Kafka          KafkaConfig              `mapstructure:"kafka"`
	Secrets        *SecretsConfig           `mapstructure:"secrets"`
	DeadLetter     *DeadLetterConfig        `mapstructure:"dead_letter"`
	MessageStorage *MessageStorageConfig    `mapstructure:"message_storage"`
	Pruning        *PruningConfig           `mapstructure:"pruning"`
	Observability  *ObservabilityConfig     `mapstructure:"observability"`
	Logging        *LoggingConfig           `mapstructure:"logging"`
	Alerts         []AlertConfig            `mapstructure:"alerts"`
	AccessControl  *AccessControlConfig     `mapstructure:"access_control"`
	Roles          []RoleConfig             `mapstructure:"roles"`
	Audit          *AuditConfig             `mapstructure:"audit"`
	Cluster        *ClusterConfig           `mapstructure:"cluster"`
	Global         *GlobalConfig            `mapstructure:"global"`
	Tenancy        *TenancyConfig           `mapstructure:"tenancy"`
	Dashboard      *DashboardConfig         `mapstructure:"dashboard"`
	CodeTemplates  []CodeTemplateLibraryConfig `mapstructure:"code_templates"`
}

type CodeTemplateLibraryConfig struct {
	Name      string   `mapstructure:"name"`
	Directory string   `mapstructure:"directory"`
	Channels  []string `mapstructure:"channels,omitempty"`
}

type RuntimeConfig struct {
	Name       string            `mapstructure:"name"`
	Profile    string            `mapstructure:"profile"`
	LogLevel   string            `mapstructure:"log_level"`
	Mode       string            `mapstructure:"mode"`
	Storage    StorageConfig     `mapstructure:"storage"`
	Encryption *EncryptionConfig `mapstructure:"encryption"`
	Health     *HealthConfig     `mapstructure:"health"`
	JSRuntime  string            `mapstructure:"js_runtime"`
	WorkerPool int               `mapstructure:"worker_pool"`
}

type StorageConfig struct {
	Driver      string `mapstructure:"driver"`
	PostgresDSN string `mapstructure:"postgres_dsn"`
}

type EncryptionConfig struct {
	KeyFile   string `mapstructure:"key_file"`
	Algorithm string `mapstructure:"algorithm"`
}

type HealthConfig struct {
	Port          int    `mapstructure:"port"`
	Path          string `mapstructure:"path"`
	ReadinessPath string `mapstructure:"readiness_path"`
	LivenessPath  string `mapstructure:"liveness_path"`
}

type KafkaConfig struct {
	Brokers  []string `mapstructure:"brokers"`
	ClientID string   `mapstructure:"client_id"`
}

type Destination struct {
	Type     string                `mapstructure:"type"`
	Kafka    *KafkaDestConfig      `mapstructure:"kafka"`
	HTTP     *HTTPDestConfig       `mapstructure:"http"`
	TCP      *TCPDestMapConfig     `mapstructure:"tcp"`
	File     *FileDestMapConfig    `mapstructure:"file"`
	SFTP     *SFTPDestMapConfig    `mapstructure:"sftp"`
	Database *DBDestMapConfig      `mapstructure:"database"`
	SMTP     *SMTPDestMapConfig    `mapstructure:"smtp"`
	Channel  *ChannelDestMapConfig `mapstructure:"channel"`
	DICOM    *DICOMDestMapConfig   `mapstructure:"dicom"`
	JMS      *JMSDestMapConfig     `mapstructure:"jms"`
	FHIR     *FHIRDestMapConfig    `mapstructure:"fhir"`
	Direct   *DirectDestMapConfig  `mapstructure:"direct"`
	Retry    *RetryMapConfig       `mapstructure:"retry"`
}

type SFTPDestMapConfig struct {
	Host            string          `mapstructure:"host"`
	Port            int             `mapstructure:"port"`
	Directory       string          `mapstructure:"directory"`
	FilenamePattern string          `mapstructure:"filename_pattern"`
	Auth            *HTTPAuthConfig `mapstructure:"auth"`
}

type KafkaDestConfig struct {
	Brokers  []string       `mapstructure:"brokers"`
	Topic    string         `mapstructure:"topic"`
	ClientID string         `mapstructure:"client_id"`
	Auth     *HTTPAuthConfig `mapstructure:"auth"`
	TLS      *TLSMapConfig  `mapstructure:"tls"`
}

type HTTPDestConfig struct {
	URL       string            `mapstructure:"url"`
	Method    string            `mapstructure:"method"`
	Headers   map[string]string `mapstructure:"headers"`
	TimeoutMs int               `mapstructure:"timeout_ms"`
	Auth      *HTTPAuthConfig   `mapstructure:"auth"`
	TLS       *TLSMapConfig     `mapstructure:"tls"`
}

type HTTPAuthConfig struct {
	Type           string   `mapstructure:"type"`
	Token          string   `mapstructure:"token"`
	Username       string   `mapstructure:"username"`
	Password       string   `mapstructure:"password"`
	Key            string   `mapstructure:"key"`
	Header         string   `mapstructure:"header"`
	QueryParam     string   `mapstructure:"query_param"`
	TokenURL       string   `mapstructure:"token_url"`
	ClientID       string   `mapstructure:"client_id"`
	ClientSecret   string   `mapstructure:"client_secret"`
	Scopes         []string `mapstructure:"scopes"`
	PrivateKeyFile string   `mapstructure:"private_key_file"`
	Passphrase     string   `mapstructure:"passphrase"`
}

type TLSMapConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	CAFile             string `mapstructure:"ca_file"`
	ClientCertFile     string `mapstructure:"client_cert_file"`
	ClientKeyFile      string `mapstructure:"client_key_file"`
	MinVersion         string `mapstructure:"min_version"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

type TCPDestMapConfig struct {
	Host      string        `mapstructure:"host"`
	Port      int           `mapstructure:"port"`
	Mode      string        `mapstructure:"mode"`
	TimeoutMs int           `mapstructure:"timeout_ms"`
	TLS       *TLSMapConfig `mapstructure:"tls"`
	KeepAlive bool          `mapstructure:"keep_alive"`
}

type FileDestMapConfig struct {
	Scheme          string `mapstructure:"scheme"`
	Directory       string `mapstructure:"directory"`
	FilenamePattern string `mapstructure:"filename_pattern"`
}

type DBDestMapConfig struct {
	Driver    string `mapstructure:"driver"`
	DSN       string `mapstructure:"dsn"`
	Statement string `mapstructure:"statement"`
	MaxConns  int    `mapstructure:"max_conns"`
}

type SMTPDestMapConfig struct {
	Host    string         `mapstructure:"host"`
	Port    int            `mapstructure:"port"`
	From    string         `mapstructure:"from"`
	To      []string       `mapstructure:"to"`
	Subject string         `mapstructure:"subject"`
	Auth    *HTTPAuthConfig `mapstructure:"auth"`
	TLS     *TLSMapConfig  `mapstructure:"tls"`
}

type ChannelDestMapConfig struct {
	TargetChannelID string `mapstructure:"target_channel_id"`
}

type DICOMDestMapConfig struct {
	Host          string        `mapstructure:"host"`
	Port          int           `mapstructure:"port"`
	AETitle       string        `mapstructure:"ae_title"`
	CalledAETitle string        `mapstructure:"called_ae_title"`
	TimeoutMs     int           `mapstructure:"timeout_ms"`
	TLS           *TLSMapConfig `mapstructure:"tls"`
}

type JMSDestMapConfig struct {
	Provider  string         `mapstructure:"provider"`
	URL       string         `mapstructure:"url"`
	Queue     string         `mapstructure:"queue"`
	Auth      *HTTPAuthConfig `mapstructure:"auth"`
	TimeoutMs int            `mapstructure:"timeout_ms"`
}

type FHIRDestMapConfig struct {
	BaseURL    string         `mapstructure:"base_url"`
	Version    string         `mapstructure:"version"`
	Operations []string       `mapstructure:"operations"`
	Auth       *HTTPAuthConfig `mapstructure:"auth"`
	TLS        *TLSMapConfig  `mapstructure:"tls"`
	TimeoutMs  int            `mapstructure:"timeout_ms"`
}

type DirectDestMapConfig struct {
	To          string        `mapstructure:"to"`
	From        string        `mapstructure:"from"`
	Certificate string        `mapstructure:"certificate"`
	SMTPHost    string        `mapstructure:"smtp_host"`
	SMTPPort    int           `mapstructure:"smtp_port"`
	TLS         *TLSMapConfig `mapstructure:"tls"`
}

type RetryMapConfig struct {
	MaxAttempts    int      `mapstructure:"max_attempts"`
	Backoff        string   `mapstructure:"backoff"`
	InitialDelayMs int      `mapstructure:"initial_delay_ms"`
	MaxDelayMs     int      `mapstructure:"max_delay_ms"`
	Jitter         bool     `mapstructure:"jitter"`
	RetryOn        []string `mapstructure:"retry_on"`
	NoRetryOn      []string `mapstructure:"no_retry_on"`
}

type SecretsConfig struct {
	Provider string                  `mapstructure:"provider"`
	Vault    *VaultConfig            `mapstructure:"vault"`
	AWS      *AWSSecretsManagerConfig `mapstructure:"aws"`
	GCP      *GCPSecretManagerConfig  `mapstructure:"gcp"`
}

type AWSSecretsManagerConfig struct {
	Region    string `mapstructure:"region"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	CacheTTL  string `mapstructure:"cache_ttl"`
}

type GCPSecretManagerConfig struct {
	ProjectID          string `mapstructure:"project_id"`
	CredentialsFile    string `mapstructure:"credentials_file"`
	CacheTTL           string `mapstructure:"cache_ttl"`
}

type VaultConfig struct {
	Address string           `mapstructure:"address"`
	Path    string           `mapstructure:"path"`
	Auth    *VaultAuthConfig `mapstructure:"auth"`
}

type VaultAuthConfig struct {
	Type     string `mapstructure:"type"`
	RoleID   string `mapstructure:"role_id"`
	SecretID string `mapstructure:"secret_id"`
}

type DeadLetterConfig struct {
	Enabled         bool   `mapstructure:"enabled"`
	Destination     string `mapstructure:"destination"`
	IncludeError    bool   `mapstructure:"include_error"`
	IncludeOriginal bool   `mapstructure:"include_original"`
}

type MessageStorageConfig struct {
	Driver    string                   `mapstructure:"driver"`
	Mode      string                   `mapstructure:"mode"`
	Stages    []string                 `mapstructure:"stages"`
	Postgres  *StoragePostgresConfig   `mapstructure:"postgres"`
	S3        *StorageS3Config         `mapstructure:"s3"`
	Retention *StorageRetentionConfig  `mapstructure:"retention"`
}

type StoragePostgresConfig struct {
	DSN          string `mapstructure:"dsn"`
	TablePrefix  string `mapstructure:"table_prefix"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
}

type StorageS3Config struct {
	Bucket   string `mapstructure:"bucket"`
	Region   string `mapstructure:"region"`
	Prefix   string `mapstructure:"prefix"`
	Endpoint string `mapstructure:"endpoint"`
}

type StorageRetentionConfig struct {
	Days                 int    `mapstructure:"days"`
	PruneInterval        string `mapstructure:"prune_interval"`
	PruneErrored         bool   `mapstructure:"prune_errored"`
	ErroredRetentionDays int    `mapstructure:"errored_retention_days"`
}

type PruningConfig struct {
	Enabled              bool   `mapstructure:"enabled"`
	Schedule             string `mapstructure:"schedule"`
	DefaultRetentionDays int    `mapstructure:"default_retention_days"`
	ArchiveBeforePrune   bool   `mapstructure:"archive_before_prune"`
	ArchiveDestination   string `mapstructure:"archive_destination"`
}

type ObservabilityConfig struct {
	OpenTelemetry *OTelConfig       `mapstructure:"opentelemetry"`
	Prometheus    *PrometheusConfig `mapstructure:"prometheus"`
}

type OTelConfig struct {
	Enabled            bool              `mapstructure:"enabled"`
	Endpoint           string            `mapstructure:"endpoint"`
	Protocol           string            `mapstructure:"protocol"`
	Traces             bool              `mapstructure:"traces"`
	Metrics            bool              `mapstructure:"metrics"`
	ServiceName        string            `mapstructure:"service_name"`
	ResourceAttributes map[string]string `mapstructure:"resource_attributes"`
}

type PrometheusConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Port    int    `mapstructure:"port"`
	Path    string `mapstructure:"path"`
}

type AlertConfig struct {
	Name         string          `mapstructure:"name"`
	Trigger      AlertTrigger    `mapstructure:"trigger"`
	Destinations []string        `mapstructure:"destinations"`
}

type AlertTrigger struct {
	Type        string `mapstructure:"type"`
	Channel     string `mapstructure:"channel"`
	Threshold   int    `mapstructure:"threshold"`
	Window      string `mapstructure:"window"`
	ThresholdMs int    `mapstructure:"threshold_ms"`
	Percentile  string `mapstructure:"percentile"`
}

type AccessControlConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Provider string `mapstructure:"provider"`
	LDAP     *LDAPConfig `mapstructure:"ldap"`
	OIDC     *OIDCConfig `mapstructure:"oidc"`
}

type LDAPConfig struct {
	URL          string `mapstructure:"url"`
	BaseDN       string `mapstructure:"base_dn"`
	BindDN       string `mapstructure:"bind_dn"`
	BindPassword string `mapstructure:"bind_password"`
}

type OIDCConfig struct {
	Issuer       string `mapstructure:"issuer"`
	ClientID     string `mapstructure:"client_id"`
	ClientSecret string `mapstructure:"client_secret"`
}

type RoleConfig struct {
	Name        string   `mapstructure:"name"`
	Permissions []string `mapstructure:"permissions"`
}

type AuditConfig struct {
	Enabled     bool     `mapstructure:"enabled"`
	Destination string   `mapstructure:"destination"`
	Events      []string `mapstructure:"events"`
}

type ClusterConfig struct {
	Enabled           bool                  `mapstructure:"enabled"`
	Mode              string                `mapstructure:"mode"`
	Coordination      *CoordinationConfig   `mapstructure:"coordination"`
	InstanceID        string                `mapstructure:"instance_id"`
	HeartbeatInterval string                `mapstructure:"heartbeat_interval"`
	ChannelAssignment *ChannelAssignConfig  `mapstructure:"channel_assignment"`
	Deduplication     *DeduplicationConfig  `mapstructure:"deduplication"`
}

type CoordinationConfig struct {
	Type  string      `mapstructure:"type"`
	Redis *RedisConfig `mapstructure:"redis"`
}

type RedisConfig struct {
	Address      string `mapstructure:"address"`
	Password     string `mapstructure:"password"`
	DB           int    `mapstructure:"db"`
	PoolSize     int    `mapstructure:"pool_size"`
	MinIdleConns int    `mapstructure:"min_idle_conns"`
	TLS          *TLSMapConfig `mapstructure:"tls"`
	KeyPrefix    string `mapstructure:"key_prefix"`
}

type ChannelAssignConfig struct {
	Strategy    string              `mapstructure:"strategy"`
	TagAffinity map[string][]string `mapstructure:"tag_affinity"`
}

type DeduplicationConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	Window       string `mapstructure:"window"`
	Store        string `mapstructure:"store"`
	KeyExtractor string `mapstructure:"key_extractor"`
}

type GlobalConfig struct {
	Hooks *GlobalHooks `mapstructure:"hooks"`
}

type GlobalHooks struct {
	OnStartup   string `mapstructure:"on_startup"`
	OnShutdown  string `mapstructure:"on_shutdown"`
	OnDeployAll string `mapstructure:"on_deploy_all"`
}

type TenancyConfig struct {
	Mode         string `mapstructure:"mode"`
	Isolation    string `mapstructure:"isolation"`
	TenantHeader string `mapstructure:"tenant_header"`
}

type DashboardConfig struct {
	Enabled bool                 `mapstructure:"enabled"`
	Port    int                  `mapstructure:"port"`
	Auth    *DashboardAuthConfig `mapstructure:"auth"`
}

type DashboardAuthConfig struct {
	Provider         string `mapstructure:"provider"`
	Username         string `mapstructure:"username"`
	Password         string `mapstructure:"password"`
	DisableLoginPage bool   `mapstructure:"disable_login_page"`
}

type LoggingConfig struct {
	Transports []LogTransportConfig `mapstructure:"transports"`
}

type LogTransportConfig struct {
	Type          string                  `mapstructure:"type"`
	CloudWatch    *CloudWatchLogConfig    `mapstructure:"cloudwatch"`
	Datadog       *DatadogLogConfig       `mapstructure:"datadog"`
	SumoLogic     *SumoLogicLogConfig     `mapstructure:"sumologic"`
	Elasticsearch *ElasticsearchLogConfig `mapstructure:"elasticsearch"`
	File          *FileLogConfig          `mapstructure:"file"`
}

type CloudWatchLogConfig struct {
	Region    string `mapstructure:"region"`
	LogGroup  string `mapstructure:"log_group"`
	LogStream string `mapstructure:"log_stream"`
}

type DatadogLogConfig struct {
	APIKey  string   `mapstructure:"api_key"`
	Site    string   `mapstructure:"site"`
	Service string   `mapstructure:"service"`
	Source  string   `mapstructure:"source"`
	Tags    []string `mapstructure:"tags"`
}

type SumoLogicLogConfig struct {
	Endpoint       string `mapstructure:"endpoint"`
	SourceCategory string `mapstructure:"source_category"`
	SourceName     string `mapstructure:"source_name"`
}

type ElasticsearchLogConfig struct {
	URLs     []string `mapstructure:"urls"`
	Index    string   `mapstructure:"index"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
	APIKey   string   `mapstructure:"api_key"`
}

type FileLogConfig struct {
	Path      string `mapstructure:"path"`
	MaxSizeMB int    `mapstructure:"max_size_mb"`
	MaxFiles  int    `mapstructure:"max_files"`
	Compress  bool   `mapstructure:"compress"`
}
