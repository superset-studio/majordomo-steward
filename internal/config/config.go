package config

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Storage   StorageConfig   `mapstructure:"storage"`
	Logging   LoggingConfig   `mapstructure:"logging"`
	Pricing   PricingConfig   `mapstructure:"pricing"`
	Providers ProvidersConfig `mapstructure:"providers"`
	Metadata  MetadataConfig  `mapstructure:"metadata"`
	Secrets   SecretsConfig   `mapstructure:"secrets"`
	Upstream  StewardConfig   `mapstructure:"upstream"`
	Managed   ManagedConfig   `mapstructure:"managed"`
}

// StewardConfig holds settings for communicating with the Butler service.
// ButlerBaseURL, StewardToken, and OrgID are set at runtime per registered org —
// they are not loaded from environment variables.
type StewardConfig struct {
	// Runtime-only fields: set by WorkerManager when starting per-org workers.
	ButlerBaseURL string    `mapstructure:"-"`
	StewardToken  string    `mapstructure:"-"`
	OrgID         uuid.UUID `mapstructure:"-"`

	// Global interval settings loaded from environment.
	BatchInterval   time.Duration `mapstructure:"batch_interval"`
	BatchMaxSize    int           `mapstructure:"batch_max_size"`
	KeySyncInterval time.Duration `mapstructure:"key_sync_interval"`
	JobPollInterval time.Duration `mapstructure:"job_poll_interval"`
	JobPollLimit    int           `mapstructure:"job_poll_limit"`
}

// ManagedConfig holds settings for running as a Majordomo-hosted managed steward.
type ManagedConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	MasterToken  string        `mapstructure:"master_token"`
	ButlerURL    string        `mapstructure:"butler_url"`
	PollInterval time.Duration `mapstructure:"poll_interval"`
}

type ServerConfig struct {
	Host                string        `mapstructure:"host"`
	Port                int           `mapstructure:"port"`
	ReadTimeout         time.Duration `mapstructure:"read_timeout"`
	WriteTimeout        time.Duration `mapstructure:"write_timeout"`
	UpstreamTimeout     time.Duration `mapstructure:"upstream_timeout"`
	StreamHeaderTimeout time.Duration `mapstructure:"stream_header_timeout"`
}

type StorageConfig struct {
	Driver   string         `mapstructure:"driver"`
	Postgres PostgresConfig `mapstructure:"postgres"`
}

type PostgresConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	SSLMode  string `mapstructure:"sslmode"`
	MaxConns int    `mapstructure:"max_conns"`
}

func (p *PostgresConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.User, p.Password, p.Host, p.Port, p.Database, p.SSLMode)
}

type LoggingConfig struct {
	Level               string `mapstructure:"level"`
	StoreRequestBody    bool   `mapstructure:"store_request_body"`
	StoreResponseBody   bool   `mapstructure:"store_response_body"`
	MaxBodySize         int    `mapstructure:"max_body_size"`
	MaxRequestBodySize  int    `mapstructure:"max_request_body_size"`
	MaxResponseBodySize int    `mapstructure:"max_response_body_size"`
	BodyStorage         string `mapstructure:"body_storage"` // "none", "s3", "gcs"
}

func (l *LoggingConfig) EffectiveMaxRequestBodySize() int {
	if l.MaxRequestBodySize > 0 {
		return l.MaxRequestBodySize
	}
	if l.MaxBodySize > 0 {
		return l.MaxBodySize
	}
	return 65536
}

func (l *LoggingConfig) EffectiveMaxResponseBodySize() int {
	if l.MaxResponseBodySize > 0 {
		return l.MaxResponseBodySize
	}
	if l.MaxBodySize > 0 {
		return l.MaxBodySize
	}
	return 65536
}

type PricingConfig struct {
	RemoteURL       string        `mapstructure:"remote_url"`
	RefreshInterval time.Duration `mapstructure:"refresh_interval"`
	FallbackFile    string        `mapstructure:"fallback_file"`
	AliasesFile     string        `mapstructure:"aliases_file"`
}

type ProvidersConfig struct {
	OpenAI    ProviderConfig `mapstructure:"openai"`
	Anthropic ProviderConfig `mapstructure:"anthropic"`
	Gemini    ProviderConfig `mapstructure:"gemini"`
}

type ProviderConfig struct {
	BaseURL string `mapstructure:"base_url"`
}

type MetadataConfig struct {
	HLLFlushInterval   time.Duration `mapstructure:"hll_flush_interval"`
	ActiveKeysCacheTTL time.Duration `mapstructure:"active_keys_cache_ttl"`
}

type SecretsConfig struct {
	EncryptionKey string `mapstructure:"encryption_key"`
	AdminToken    string `mapstructure:"admin_token"`
}

func Load() (*Config, error) {
	v := viper.New()
	setDefaults(v)
	bindEnv(v)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func bindEnv(v *viper.Viper) {
	v.BindEnv("server.host", "HOST")
	v.BindEnv("server.port", "PORT")
	v.BindEnv("server.read_timeout", "READ_TIMEOUT")
	v.BindEnv("server.write_timeout", "WRITE_TIMEOUT")
	v.BindEnv("server.upstream_timeout", "UPSTREAM_TIMEOUT")
	v.BindEnv("server.stream_header_timeout", "STREAM_HEADER_TIMEOUT")

	v.BindEnv("storage.postgres.host", "POSTGRES_HOST")
	v.BindEnv("storage.postgres.port", "POSTGRES_PORT")
	v.BindEnv("storage.postgres.user", "POSTGRES_USER")
	v.BindEnv("storage.postgres.password", "POSTGRES_PASSWORD")
	v.BindEnv("storage.postgres.database", "POSTGRES_DB")
	v.BindEnv("storage.postgres.sslmode", "POSTGRES_SSLMODE")
	v.BindEnv("storage.postgres.max_conns", "POSTGRES_MAX_CONNS")

	v.BindEnv("logging.level", "LOG_LEVEL")
	v.BindEnv("logging.store_request_body", "LOG_STORE_REQUEST_BODY")
	v.BindEnv("logging.store_response_body", "LOG_STORE_RESPONSE_BODY")
	v.BindEnv("logging.max_request_body_size", "LOG_MAX_REQUEST_BODY_SIZE")
	v.BindEnv("logging.max_response_body_size", "LOG_MAX_RESPONSE_BODY_SIZE")
	v.BindEnv("logging.body_storage", "LOG_BODY_STORAGE")

	v.BindEnv("pricing.remote_url", "PRICING_REMOTE_URL")
	v.BindEnv("pricing.refresh_interval", "PRICING_REFRESH_INTERVAL")
	v.BindEnv("pricing.fallback_file", "PRICING_FALLBACK_FILE")
	v.BindEnv("pricing.aliases_file", "PRICING_ALIASES_FILE")

	v.BindEnv("providers.openai.base_url", "OPENAI_BASE_URL")
	v.BindEnv("providers.anthropic.base_url", "ANTHROPIC_BASE_URL")
	v.BindEnv("providers.gemini.base_url", "GEMINI_BASE_URL")

	v.BindEnv("metadata.hll_flush_interval", "METADATA_HLL_FLUSH_INTERVAL")
	v.BindEnv("metadata.active_keys_cache_ttl", "METADATA_ACTIVE_KEYS_CACHE_TTL")

	v.BindEnv("secrets.encryption_key", "ENCRYPTION_KEY")
	v.BindEnv("secrets.admin_token", "STEWARD_ADMIN_TOKEN")

	v.BindEnv("upstream.batch_interval", "BATCH_INTERVAL")
	v.BindEnv("upstream.batch_max_size", "BATCH_MAX_SIZE")
	v.BindEnv("upstream.key_sync_interval", "KEY_SYNC_INTERVAL")
	v.BindEnv("upstream.job_poll_interval", "JOB_POLL_INTERVAL")
	v.BindEnv("upstream.job_poll_limit", "JOB_POLL_LIMIT")

	v.BindEnv("managed.enabled", "MANAGED_ENABLED")
	v.BindEnv("managed.master_token", "MANAGED_MASTER_TOKEN")
	v.BindEnv("managed.butler_url", "MANAGED_BUTLER_URL")
	v.BindEnv("managed.poll_interval", "MANAGED_POLL_INTERVAL")
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 7680)
	v.SetDefault("server.read_timeout", 30*time.Second)
	v.SetDefault("server.write_timeout", 600*time.Second)
	v.SetDefault("server.upstream_timeout", 600*time.Second)
	v.SetDefault("server.stream_header_timeout", 0)

	v.SetDefault("storage.driver", "postgres")
	v.SetDefault("storage.postgres.host", "localhost")
	v.SetDefault("storage.postgres.port", 5432)
	v.SetDefault("storage.postgres.user", "")
	v.SetDefault("storage.postgres.password", "")
	v.SetDefault("storage.postgres.database", "majordomo_steward")
	v.SetDefault("storage.postgres.sslmode", "disable")
	v.SetDefault("storage.postgres.max_conns", 20)

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.store_request_body", false)
	v.SetDefault("logging.store_response_body", false)
	v.SetDefault("logging.max_body_size", 65536)
	v.SetDefault("logging.max_request_body_size", 65536)
	v.SetDefault("logging.max_response_body_size", 65536)
	v.SetDefault("logging.body_storage", "none")

	v.SetDefault("pricing.remote_url", "https://www.llm-prices.com/current-v1.json")
	v.SetDefault("pricing.refresh_interval", time.Hour)
	v.SetDefault("pricing.fallback_file", "./pricing.json")
	v.SetDefault("pricing.aliases_file", "./model_aliases.json")

	v.SetDefault("providers.openai.base_url", "https://api.openai.com")
	v.SetDefault("providers.anthropic.base_url", "https://api.anthropic.com")
	v.SetDefault("providers.gemini.base_url", "https://generativelanguage.googleapis.com")

	v.SetDefault("metadata.hll_flush_interval", 60*time.Second)
	v.SetDefault("metadata.active_keys_cache_ttl", 5*time.Minute)

	v.SetDefault("secrets.encryption_key", "")

	v.SetDefault("upstream.batch_interval", 60*time.Second)
	v.SetDefault("upstream.batch_max_size", 500)
	v.SetDefault("upstream.key_sync_interval", 5*time.Minute)
	v.SetDefault("upstream.job_poll_interval", 30*time.Second)
	v.SetDefault("upstream.job_poll_limit", 10)

	v.SetDefault("managed.enabled", false)
	v.SetDefault("managed.master_token", "")
	v.SetDefault("managed.butler_url", "")
	v.SetDefault("managed.poll_interval", 30*time.Second)
}
