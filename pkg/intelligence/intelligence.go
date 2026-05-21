package intelligence

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	BaseURL = "https://data.solanatracker.io"
)

// Client wraps the Solana Tracker Data API
type Client struct {
	apiKey     string
	httpClient *http.Client
	mu         sync.Mutex
	callCount  int
	// Simple in-memory cache to avoid hammering the API
	walletCache map[string]cachedWalletInfo
	tokenCache  map[string]cachedTokenInfo
}

type cachedWalletInfo struct {
	info     *WalletProfile
	cachedAt time.Time
}

type cachedTokenInfo struct {
	info     *TokenInfo
	cachedAt time.Time
}

func NewClient() *Client {
	apiKey := os.Getenv("SOLANA_TRACKER_API_KEY")
	if apiKey == "" {
		log.Println("Intelligence: SOLANA_TRACKER_API_KEY not set — Layer 1/3 disabled")
	}
	return &Client{
		apiKey:      apiKey,
		httpClient:  &http.Client{Timeout: 8 * time.Second},
		walletCache: make(map[string]cachedWalletInfo),
		tokenCache:  make(map[string]cachedTokenInfo),
	}
}

// Enabled returns whether the client has an API key configured
func (c *Client) Enabled() bool {
	return c.apiKey != ""
}

// CallCount returns total API calls made (for cost tracking)
func (c *Client) CallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.callCount
}

func (c *Client) get(ctx context.Context, path string, out interface{}) error {
	if c.apiKey == "" {
		return fmt.Errorf("SOLANA_TRACKER_API_KEY not set")
	}

	url := BaseURL + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.callCount++
	c.mu.Unlock()

	if resp.StatusCode == 429 {
		return fmt.Errorf("rate limited (429)")
	}
	if resp.StatusCode == 404 {
		return fmt.Errorf("not found (404)")
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body[:minInt(200, len(body))]))
	}

	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("parse: %w (body: %s)", err, string(body[:minInt(200, len(body))]))
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ─── LAYER 1: Developer Profiling ─────────────────────────────────────────────

// WalletProfile holds the dev wallet PnL & history
type WalletProfile struct {
	Summary struct {
		TotalPnLUSD    float64 `json:"total"`
		Realized       float64 `json:"realized"`
		Unrealized     float64 `json:"unrealized"`
		WinRate        float64 `json:"winPercentage"`
		TotalTrades    int     `json:"totalTrades"`
		TotalInvested  float64 `json:"totalInvested"`
		LosingTokens   int     `json:"losingTokens"`
		WinningTokens  int     `json:"winningTokens"`
	} `json:"summary"`
	Tokens map[string]interface{} `json:"tokens"`
}

// DevAssessment holds the verdict on a dev wallet
type DevAssessment struct {
	Skip       bool
	Reason     string
	SolBalance float64 // in SOL
	WinRate    float64 // 0.0 - 100.0
	TotalPnL   float64 // USD
	TotalTrades int
}

// AssessDeveloper runs Layer 1 checks on a creator wallet.
// Returns Skip=true if wallet fails any filter.
func (c *Client) AssessDeveloper(ctx context.Context, devAddress string, solBalance float64) *DevAssessment {
	result := &DevAssessment{
		SolBalance: solBalance,
	}

	// Layer 1.1: SOL balance check (configurable threshold)
	minBalance := envFloat("DEV_MIN_SOL_BALANCE", 0.5)
	if solBalance < minBalance {
		result.Skip = true
		result.Reason = fmt.Sprintf("dev_balance_too_low (%.3f < %.3f SOL)", solBalance, minBalance)
		return result
	}

	// Layer 1.2: Wallet PnL check (Solana Tracker)
	if !c.Enabled() {
		// SOL balance passed; can't do PnL check without API key
		return result
	}

	// Check cache (15 min TTL)
	c.mu.Lock()
	cached, ok := c.walletCache[devAddress]
	c.mu.Unlock()
	var profile *WalletProfile
	if ok && time.Since(cached.cachedAt) < 15*time.Minute {
		profile = cached.info
	} else {
		var p WalletProfile
		if err := c.get(ctx, "/pnl/"+devAddress, &p); err != nil {
			log.Printf("Intelligence: PnL fetch failed for %s: %v\n", devAddress[:10], err)
			return result // fail open
		}
		profile = &p
		c.mu.Lock()
		c.walletCache[devAddress] = cachedWalletInfo{info: profile, cachedAt: time.Now()}
		c.mu.Unlock()
	}

	result.WinRate = profile.Summary.WinRate
	result.TotalPnL = profile.Summary.TotalPnLUSD
	result.TotalTrades = profile.Summary.TotalTrades

	// Skip serial losers: only flag wallets with significant history
	maxLossUSD := envFloat("DEV_MAX_LOSS_USD", -500.0) // skip if lost more than $500
	minTradesForJudgment := envInt("DEV_MIN_TRADES_FOR_JUDGMENT", 10)

	if result.TotalTrades >= minTradesForJudgment {
		if result.TotalPnL < maxLossUSD {
			result.Skip = true
			result.Reason = fmt.Sprintf("dev_serial_loser (pnl=%.2f USD over %d trades)",
				result.TotalPnL, result.TotalTrades)
			return result
		}

		// Skip very low win rate devs
		minWinRate := envFloat("DEV_MIN_WIN_RATE", 15.0)
		if result.WinRate < minWinRate {
			result.Skip = true
			result.Reason = fmt.Sprintf("dev_low_winrate (%.1f%% over %d trades)",
				result.WinRate, result.TotalTrades)
			return result
		}
	}

	return result
}

// ─── LAYER 3: Token Metadata & Holders ────────────────────────────────────────

// TokenInfo from Solana Tracker
type TokenInfo struct {
	Token struct {
		Mint        string `json:"mint"`
		Name        string `json:"name"`
		Symbol      string `json:"symbol"`
		Description string `json:"description"`
		Image       string `json:"image"`
		Twitter     string `json:"twitter"`
		Telegram    string `json:"telegram"`
		Website     string `json:"website"`
		Creator     string `json:"creator"`
	} `json:"token"`
	Pools []struct {
		LiquidityUSD flexFloat `json:"liquidityUsd"`
		MarketCap    struct {
			USD flexFloat `json:"usd"`
		} `json:"marketCap"`
		Price struct {
			USD flexFloat `json:"usd"`
		} `json:"price"`
		Curve struct {
			Progress flexFloat `json:"progress"` // 0-100 bonding curve progress (string or number from API)
		} `json:"curve"`
	} `json:"pools"`
	Risk struct {
		Score   flexFloat `json:"score"` // 1-10
		Reasons []struct {
			Name   string `json:"name"`
			Level  string `json:"level"`
			Detail string `json:"description"`
		} `json:"risks"`
	} `json:"risk"`
}

// flexFloat handles fields that Solana Tracker returns as either string or number
type flexFloat float64

func (f *flexFloat) UnmarshalJSON(data []byte) error {
	// Try number first
	var n float64
	if err := json.Unmarshal(data, &n); err == nil {
		*f = flexFloat(n)
		return nil
	}
	// Try string
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		if s == "" {
			*f = 0
			return nil
		}
		var parsed float64
		if _, err := fmt.Sscanf(s, "%f", &parsed); err == nil {
			*f = flexFloat(parsed)
			return nil
		}
	}
	*f = 0 // fail-safe
	return nil
}

// MetadataAssessment is the Layer 3 verdict
type MetadataAssessment struct {
	Skip            bool
	Reason          string
	HasTwitter      bool
	HasTelegram     bool
	HasWebsite      bool
	HasDescription  bool
	RiskScore       float64
	RiskReasons     []string
	BondingProgress float64
	LiquidityUSD    float64
	MarketCapUSD    float64
}

// AssessToken runs Layer 3 checks via Solana Tracker /tokens/{mint}
func (c *Client) AssessToken(ctx context.Context, mint string) *MetadataAssessment {
	result := &MetadataAssessment{}

	if !c.Enabled() {
		// Can't assess without API
		return result
	}

	// Check cache (60s TTL - tokens change fast)
	c.mu.Lock()
	cached, ok := c.tokenCache[mint]
	c.mu.Unlock()

	var info *TokenInfo
	if ok && time.Since(cached.cachedAt) < 60*time.Second {
		info = cached.info
	} else {
		var t TokenInfo
		if err := c.get(ctx, "/tokens/"+mint, &t); err != nil {
			log.Printf("Intelligence: Token fetch failed for %s: %v\n", mint[:10], err)
			return result // fail open
		}
		info = &t
		c.mu.Lock()
		c.tokenCache[mint] = cachedTokenInfo{info: info, cachedAt: time.Now()}
		c.mu.Unlock()
	}

	result.HasTwitter = info.Token.Twitter != ""
	result.HasTelegram = info.Token.Telegram != ""
	result.HasWebsite = info.Token.Website != ""
	result.HasDescription = len(info.Token.Description) > 20
	result.RiskScore = float64(info.Risk.Score)

	for _, r := range info.Risk.Reasons {
		result.RiskReasons = append(result.RiskReasons, r.Name)
	}

	if len(info.Pools) > 0 {
		result.BondingProgress = float64(info.Pools[0].Curve.Progress)
		result.LiquidityUSD = float64(info.Pools[0].LiquidityUSD)
		result.MarketCapUSD = float64(info.Pools[0].MarketCap.USD)
	}

	// Layer 3.1: Require at least one social link
	requireSocial := envBool("REQUIRE_SOCIAL", true)
	if requireSocial && !result.HasTwitter && !result.HasTelegram && !result.HasWebsite {
		result.Skip = true
		result.Reason = "no_social_presence"
		return result
	}

	// Layer 3.2: Reject on high Solana Tracker risk score
	// Score is 1-10 where higher = more risky (snipers, bundlers, insiders detected)
	maxRiskScore := envFloat("MAX_RISK_SCORE", 6.0)
	if result.RiskScore > maxRiskScore {
		result.Skip = true
		result.Reason = fmt.Sprintf("high_risk_score (%.1f > %.1f, flags: %v)",
			result.RiskScore, maxRiskScore, result.RiskReasons)
		return result
	}

	// Layer 4.1: Bonding curve progress floor — wait for proof of momentum
	// Default 15% — entries earlier than this are too risky
	minProgress := envFloat("MIN_BONDING_PROGRESS", 15.0)
	maxProgress := envFloat("MAX_BONDING_PROGRESS", 70.0)
	if result.BondingProgress < minProgress {
		result.Skip = true
		result.Reason = fmt.Sprintf("bonding_progress_too_low (%.1f%% < %.1f%%)",
			result.BondingProgress, minProgress)
		return result
	}
	if result.BondingProgress > maxProgress {
		result.Skip = true
		result.Reason = fmt.Sprintf("bonding_progress_too_high (%.1f%% > %.1f%%, near graduation)",
			result.BondingProgress, maxProgress)
		return result
	}

	return result
}

// ─── Env helpers ──────────────────────────────────────────────────────────────

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var f float64
	if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
		return f
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
		return n
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "true" || v == "1" || v == "yes"
}
