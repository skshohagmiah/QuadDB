package config

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config represents the application configuration
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Storage  StorageConfig  `mapstructure:"storage"`
	Cluster  ClusterConfig  `mapstructure:"cluster"`
	Logging  LoggingConfig  `mapstructure:"logging"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
}

// ServerConfig contains server-related configuration
type ServerConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	ReadTimeout  int    `mapstructure:"read_timeout"`
	WriteTimeout int    `mapstructure:"write_timeout"`
	MaxConnections int  `mapstructure:"max_connections"`
}

// StorageConfig contains storage-related configuration
type StorageConfig struct {
	Backend   string `mapstructure:"backend"`
	DataDir   string `mapstructure:"data_dir"`
	BackupDir string `mapstructure:"backup_dir"`
	GCInterval int   `mapstructure:"gc_interval"`
}

// ClusterConfig contains clustering configuration
type ClusterConfig struct {
	Enabled       bool     `mapstructure:"enabled"`
	NodeID        string   `mapstructure:"node_id"`
	BindAddr      string   `mapstructure:"bind_addr"`
	Bootstrap     bool     `mapstructure:"bootstrap"`
	JoinAddresses []string `mapstructure:"join_addresses"`
	DataDir       string   `mapstructure:"data_dir"`
	Replicas      int      `mapstructure:"replicas"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
	File   string `mapstructure:"file"`
}

// MetricsConfig contains metrics configuration
type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Port    int    `mapstructure:"port"`
	Path    string `mapstructure:"path"`
}

// LoadConfig loads configuration from file and environment
func LoadConfig(configPath string) (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	
	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else {
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/gomsg")
	}

	// Set defaults
	setDefaults()

	// Read environment variables
	viper.AutomaticEnv()
	viper.SetEnvPrefix("GOMSG")

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate and set computed values
	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// setDefaults sets default configuration values
func setDefaults() {
	// Server defaults
	viper.SetDefault("server.host", "localhost")
	viper.SetDefault("server.port", 9000)
	viper.SetDefault("server.read_timeout", 30)
	viper.SetDefault("server.write_timeout", 30)
	viper.SetDefault("server.max_connections", 1000)

	// Storage defaults
	viper.SetDefault("storage.backend", "badger")
	viper.SetDefault("storage.data_dir", "./data")
	viper.SetDefault("storage.backup_dir", "./backups")
	viper.SetDefault("storage.gc_interval", 300)

	// Cluster defaults
	viper.SetDefault("cluster.enabled", false)
	viper.SetDefault("cluster.node_id", "")
	viper.SetDefault("cluster.bind_addr", "")
	viper.SetDefault("cluster.bootstrap", false)
	viper.SetDefault("cluster.join_addresses", []string{})
	viper.SetDefault("cluster.data_dir", "./cluster")
	viper.SetDefault("cluster.replicas", 3)

	// Logging defaults
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "text")
	viper.SetDefault("logging.file", "")

	// Metrics defaults
	viper.SetDefault("metrics.enabled", true)
	viper.SetDefault("metrics.port", 8080)
	viper.SetDefault("metrics.path", "/metrics")
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	// Ensure data directories exist
	config.Storage.DataDir = filepath.Clean(config.Storage.DataDir)
	config.Storage.BackupDir = filepath.Clean(config.Storage.BackupDir)
	
	if config.Cluster.Enabled {
		config.Cluster.DataDir = filepath.Clean(config.Cluster.DataDir)
		
		if config.Cluster.NodeID == "" {
			return fmt.Errorf("cluster.node_id is required when clustering is enabled")
		}

		if config.Cluster.BindAddr == "" {
			config.Cluster.BindAddr = fmt.Sprintf("localhost:%d", config.Server.Port+1000)
		}
	}

	// Validate port ranges
	if config.Server.Port < 1 || config.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}

	if config.Metrics.Enabled && (config.Metrics.Port < 1 || config.Metrics.Port > 65535) {
		return fmt.Errorf("metrics.port must be between 1 and 65535")
	}

	return nil
}

// GetDefaultConfig returns a default configuration
func GetDefaultConfig() *Config {
	setDefaults()
	
	var config Config
	viper.Unmarshal(&config)
	validateConfig(&config)
	
	return &config
}