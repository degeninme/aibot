package config

import (
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	"os"
	"strconv"
	"strings"
)

// dryRunOverride: -1 = use env var, 0 = force LIVE, 1 = force DRY
var dryRunOverride int32 = -1

// SetDryRunOverride changes dry-run mode at runtime. mode = "dry", "live", or "default" (env)
func SetDryRunOverride(mode string) {
	switch strings.ToLower(mode) {
	case "dry", "true":
		atomic.StoreInt32(&dryRunOverride, 1)
	case "live", "false":
		atomic.StoreInt32(&dryRunOverride, 0)
	default:
		atomic.StoreInt32(&dryRunOverride, -1)
	}
}

// IsDryRunOverridden reports whether the runtime override is currently set.
func IsDryRunOverridden() bool {
	return atomic.LoadInt32(&dryRunOverride) != -1
}

// Config holds all application configuration
type Config struct {
	// General settings — DryRun is accessed via the IsDryRun() method to support runtime override
	DryRun              bool
	AutoExecute         bool
	
	// Chain settings
	SolanaRPCURL        string
	SolanaWSURL         string
	BaseRPCURL          string
	BaseWSURL           string
	ScanIntervalSolana  time.Duration
	ScanIntervalBase    time.Duration
	
	// Strategy thresholds
	WinProbabilityThreshold float64
	MinVolumeDEX            float64
	MinLiquidity            float64
	MaxHoneypotScore        float64
	MaxSlippage             float64
	
	// Risk management
	SinglePositionPct   float64
	TotalExposurePct    float64
	DailyLossLimit      float64
	AccountBalance      float64
	
	// Execution settings
	MaxSimulateRetries  int
	SimulateTimeout     time.Duration
	ConfirmationsWait   int
	
	// Time windows
	ObservationWindow5m  time.Duration
	ObservationWindow15m time.Duration
	ObservationWindow1h  time.Duration
	DefaultTimeWindow    time.Duration
	
	// API settings
	CoinGeckoAPIKey     string
	OKXAPIKey           string
	OKXAPISecret        string
	OKXAPIPassphrase    string
	TwitterAPIKey       string
	
	// Wallet settings
	UseOKXWallet        bool
	PrivateKey          string // Use with caution - prefer KMS
	
	// Database settings
	DatabaseURL         string
	
	// Telemetry
	PrometheusPort      int
	LogLevel            string
	
	// Blacklist/Whitelist
	BlacklistedTokens   []string
	BlacklistedCreators []string
	WhitelistedTokens   []string
}

// ApplyOverrides syncs runtime overrides (e.g. from dashboard toggle) into the config struct.
// Should be called before reading config flags that support runtime override (currently: DryRun).
func (c *Config) ApplyOverrides() {
	override := atomic.LoadInt32(&dryRunOverride)
	switch override {
	case 1:
		c.DryRun = true
	case 0:
		c.DryRun = false
	}
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	// Load .env file if it exists
	_ = godotenv.Load()
	
	return &Config{
		// General
		DryRun:              getEnvBool("DRY_RUN", true),
		AutoExecute:         getEnvBool("AUTO_EXECUTE", false),
		
		// Chain settings
		SolanaRPCURL:        getEnv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
		SolanaWSURL:         getEnv("SOLANA_WS_URL", "wss://api.mainnet-beta.solana.com"),
		BaseRPCURL:          getEnv("BASE_RPC_URL", "https://mainnet.base.org"),
		BaseWSURL:           getEnv("BASE_WS_URL", "wss://mainnet.base.org"),
		ScanIntervalSolana:  time.Duration(getEnvInt("SCAN_INTERVAL_SOLANA_SEC", 2)) * time.Second,
		ScanIntervalBase:    time.Duration(getEnvInt("SCAN_INTERVAL_BASE_SEC", 2)) * time.Second,
		
		// Strategy thresholds
		WinProbabilityThreshold: getEnvFloat("WIN_PROBABILITY_THRESHOLD", 0.80),
		MinVolumeDEX:            getEnvFloat("MIN_VOLUME_DEX", 10000.0),
		MinLiquidity:            getEnvFloat("MIN_LIQUIDITY", 5000.0),
		MaxHoneypotScore:        getEnvFloat("MAX_HONEYPOT_SCORE", 0.2),
		MaxSlippage:             getEnvFloat("MAX_SLIPPAGE", 0.05),
		
		// Risk management
		SinglePositionPct:   getEnvFloat("SINGLE_POSITION_PCT", 0.01),
		TotalExposurePct:    getEnvFloat("TOTAL_EXPOSURE_PCT", 0.05),
		DailyLossLimit:      getEnvFloat("DAILY_LOSS_LIMIT", 500.0),
		AccountBalance:      getEnvFloat("ACCOUNT_BALANCE", 10000.0),
		
		// Execution
		MaxSimulateRetries:  getEnvInt("MAX_SIMULATE_RETRIES", 3),
		SimulateTimeout:     time.Duration(getEnvInt("SIMULATE_TIMEOUT_SEC", 30)) * time.Second,
		ConfirmationsWait:   getEnvInt("CONFIRMATIONS_WAIT", 2),
		
		// Time windows
		ObservationWindow5m:  5 * time.Minute,
		ObservationWindow15m: 15 * time.Minute,
		ObservationWindow1h:  60 * time.Minute,
		DefaultTimeWindow:    time.Duration(getEnvInt("DEFAULT_TIME_WINDOW_MIN", 15)) * time.Minute,
		
		// API settings
		CoinGeckoAPIKey:     getEnv("COINGECKO_API_KEY", ""),
		OKXAPIKey:           getEnv("OKX_API_KEY", ""),
		OKXAPISecret:        getEnv("OKX_API_SECRET", ""),
		OKXAPIPassphrase:    getEnv("OKX_API_PASSPHRASE", ""),
		TwitterAPIKey:       getEnv("TWITTER_API_KEY", ""),
		
		// Wallet
		UseOKXWallet:        getEnvBool("USE_OKX_WALLET", true),
		PrivateKey:          getEnv("PRIVATE_KEY", ""),
		
		// Database
		DatabaseURL:         getEnv("DATABASE_URL", "sqlite://./meme_bot.db"),
		
		// Telemetry
		PrometheusPort:      getEnvInt("PROMETHEUS_PORT", 9090),
		LogLevel:            getEnv("LOG_LEVEL", "info"),
		
		// Blacklist/Whitelist
		BlacklistedTokens:   getEnvList("BLACKLISTED_TOKENS"),
		BlacklistedCreators: getEnvList("BLACKLISTED_CREATORS"),
		WhitelistedTokens:   getEnvList("WHITELISTED_TOKENS"),
	}
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func getEnvList(key string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return []string{}
}
