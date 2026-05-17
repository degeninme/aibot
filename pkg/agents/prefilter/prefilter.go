package prefilter

import (
	"log"
	"strings"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

// PreFilterAgent performs basic filtering on discovered tokens
type PreFilterAgent struct {
	config *config.Config
}

// NewPreFilterAgent creates a new pre-filter agent
func NewPreFilterAgent(cfg *config.Config) *PreFilterAgent {
	return &PreFilterAgent{
		config: cfg,
	}
}

// Filter applies pre-filtering rules to a token
func (p *PreFilterAgent) Filter(token models.TokenFound) models.PreFilteredToken {
	result := models.PreFilteredToken{
		Token:    token,
		Priority: "medium",
		Dropped:  false,
		Reasons:  []string{},
	}

	// Check blacklisted tokens
	if p.isBlacklistedToken(token.TokenAddress) {
		result.Dropped = true
		result.Reasons = append(result.Reasons, "token_blacklisted")
		log.Printf("PreFilterAgent: Token %s dropped - blacklisted\n", token.TokenAddress)
		return result
	}

	// Check blacklisted creators
	if p.isBlacklistedCreator(token.CreatorAddress) {
		result.Dropped = true
		result.Reasons = append(result.Reasons, "creator_blacklisted")
		log.Printf("PreFilterAgent: Token %s dropped - creator blacklisted\n", token.TokenAddress)
		return result
	}

	// Check whitelisted tokens (high priority)
	if p.isWhitelistedToken(token.TokenAddress) {
		result.Priority = "high"
		result.Reasons = append(result.Reasons, "token_whitelisted")
		log.Printf("PreFilterAgent: Token %s marked high priority - whitelisted\n", token.TokenAddress)
		return result
	}

	// FIX: ReserveNative is in SOL, MinLiquidity is in USD.
	// Only apply liquidity filter if we actually have a reserve value.
	// Convert SOL to USD at ~$150/SOL for comparison.
	// If ReserveNative == 0 (couldn't parse from logs), allow through.
	if token.InitialLiquidity.ReserveNative > 0 {
		liquidityUSD := token.InitialLiquidity.ReserveNative * 150.0
		if liquidityUSD < p.config.MinLiquidity {
			// Don't drop — just mark low priority so it still passes through
			result.Priority = "low"
			result.Reasons = append(result.Reasons, "low_initial_liquidity")
			log.Printf("PreFilterAgent: Token %s low priority - liquidity $%.2f\n",
				token.TokenAddress, liquidityUSD)
		} else if liquidityUSD > 10000 {
			result.Priority = "high"
			result.Reasons = append(result.Reasons, "high_initial_liquidity")
		}
	}
	// If ReserveNative == 0: keep medium priority, allow through

	// Check for suspicious patterns in metadata (but don't over-filter PumpFun tokens)
	// "pump" in source is expected for PumpFun tokens, not suspicious
	if p.hasSuspiciousMetadata(token) {
		result.Priority = "low"
		result.Reasons = append(result.Reasons, "suspicious_metadata")
		log.Printf("PreFilterAgent: Token %s marked low priority - suspicious metadata\n", token.TokenAddress)
	}

	log.Printf("PreFilterAgent: Token %s passed with priority=%s\n", token.TokenAddress, result.Priority)
	return result
}

func (p *PreFilterAgent) isBlacklistedToken(address string) bool {
	for _, blacklisted := range p.config.BlacklistedTokens {
		if strings.EqualFold(address, blacklisted) {
			return true
		}
	}
	return false
}

func (p *PreFilterAgent) isBlacklistedCreator(address string) bool {
	for _, blacklisted := range p.config.BlacklistedCreators {
		if strings.EqualFold(address, blacklisted) {
			return true
		}
	}
	return false
}

func (p *PreFilterAgent) isWhitelistedToken(address string) bool {
	for _, whitelisted := range p.config.WhitelistedTokens {
		if strings.EqualFold(address, whitelisted) {
			return true
		}
	}
	return false
}

func (p *PreFilterAgent) hasSuspiciousMetadata(token models.TokenFound) bool {
	// Only flag truly suspicious words, not "pump" (expected for PumpFun tokens)
	suspiciousWords := []string{
		"test", "scam", "rug", "fake", "honeypot", "xxx",
	}

	for key, value := range token.Metadata {
		// Skip the "source" key - "pumpfun" is not suspicious
		if key == "source" {
			continue
		}
		lowerValue := strings.ToLower(value)
		for _, word := range suspiciousWords {
			if strings.Contains(lowerValue, word) {
				return true
			}
		}
	}
	return false
}
