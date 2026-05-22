package strategy

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

// BalanceProvider returns the live floating account balance in USD
type BalanceProvider func() float64

type StrategyEvaluatorAgent struct {
	config          *config.Config
	balanceProvider BalanceProvider
	mu              sync.RWMutex
}

func NewStrategyEvaluatorAgent(cfg *config.Config) *StrategyEvaluatorAgent {
	return &StrategyEvaluatorAgent{config: cfg}
}

// SetBalanceProvider wires up the live balance source
func (s *StrategyEvaluatorAgent) SetBalanceProvider(p BalanceProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.balanceProvider = p
	log.Println("StrategyEvaluatorAgent: Live balance provider configured")
}

// liveBalance returns the floating balance (USD), falling back to config.AccountBalance
func (s *StrategyEvaluatorAgent) liveBalance() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.balanceProvider != nil {
		if b := s.balanceProvider(); b > 0 {
			return b
		}
	}
	return s.config.AccountBalance
}

func (s *StrategyEvaluatorAgent) Evaluate(
	safety *models.SafetyReport,
	offchain *models.OffChainMetrics,
	token models.PreFilteredToken,
) (*models.StrategyDecision, error) {
	log.Printf("StrategyEvaluatorAgent: Evaluating token %s\n", token.Token.TokenAddress)

	// AGE CHECK: Skip tokens older than 45 seconds
	tokenAge := time.Since(time.Unix(token.Token.FirstSeenTS, 0))
	if tokenAge > 45*time.Second {
		log.Printf("StrategyEvaluatorAgent: Token %s too old (%.0fs) - skipping\n", 
			token.Token.TokenAddress[:10], tokenAge.Seconds())
		return &models.StrategyDecision{
			TokenAddress:   token.Token.TokenAddress,
			Chain:          token.Token.Chain,
			WinProbability: 0,
			Action:         "skip",
			Rationale:      []string{fmt.Sprintf("token too old: %.0fs", tokenAge.Seconds())},
			EvaluatedAt:    time.Now(),
		}, nil
	}

	decision := &models.StrategyDecision{
		TokenAddress: token.Token.TokenAddress,
		Chain:        token.Token.Chain,
		EvaluatedAt:  time.Now(),
		Rationale:    []string{},
	}

	decision.WinProbability = s.calculateWinProbability(safety, offchain, token)
	decision.ExpectedROI, decision.ExpectedROIStd = s.calculateExpectedROI(offchain, token)
	decision.Confidence = s.determineConfidence(safety, decision.WinProbability)
	decision.Action = s.determineAction(decision)
	decision.SuggestedAmountUSD = s.calculatePositionSize(decision)
	decision.StopLossPct = s.calculateStopLoss(decision)
	decision.TakeProfitPct = s.calculateTakeProfit(decision)
	decision.TimeHorizonMinutes = s.calculateTimeHorizon(decision)

	log.Printf("StrategyEvaluatorAgent: Token %s - WinProb: %.2f, Action: %s, Confidence: %s, Size: $%.2f\n",
		token.Token.TokenAddress[:10], decision.WinProbability, decision.Action, decision.Confidence, decision.SuggestedAmountUSD)

	return decision, nil
}

func (s *StrategyEvaluatorAgent) calculateWinProbability(
	safety *models.SafetyReport,
	offchain *models.OffChainMetrics,
	token models.PreFilteredToken,
) float64 {
	// Cannot trade at all = 0
	if !safety.CanBuy || !safety.CanSell {
		return 0.0
	}

	// Start lower - new tokens are inherently risky
	baseProb := 0.45

	// Priority from scanner (based on dev holdings)
	switch token.Priority {
	case "high":
		baseProb += 0.10 // Dev holds reasonable amount (2-6%)
	case "low":
		baseProb -= 0.15 // Dev sold or holds too much
	}

	// Safety bonuses (these actually matter)
	if safety.HoneypotScore < 0.05 {
		baseProb += 0.15
	} else if safety.HoneypotScore > 0.15 {
		baseProb -= 0.20
	}

	if safety.LiquidityLocked {
		baseProb += 0.10
	}

	if safety.OwnerControls.Renounced {
		baseProb += 0.08
	}

	if !safety.OwnerControls.HasBlacklist && !safety.OwnerControls.HasTransferHook {
		baseProb += 0.05
	}

	// Volume bonus (only if token has traded)
	if offchain.Volume24hDEX > 50000 {
		baseProb += 0.08
	} else if offchain.Volume24hDEX > 10000 {
		baseProb += 0.04
	}

	// Social signals (low threshold for new tokens)
	totalMentions := 0
	for _, count := range offchain.SocialMentions {
		totalMentions += count
	}
	if totalMentions > 10 {
		baseProb += 0.06
	}

	// Velocity
	switch offchain.Velocity {
	case "rising":
		baseProb += 0.05
	case "falling":
		baseProb -= 0.05
	}

	// PumpFun source bonus
	if token.Token.Metadata["source"] == "pumpfun" {
		baseProb += 0.05
	}

	// Cap at realistic levels
	if baseProb > 0.85 {
		baseProb = 0.85
	}
	if baseProb < 0.0 {
		baseProb = 0.0
	}
	return baseProb
}

func (s *StrategyEvaluatorAgent) calculateExpectedROI(
	offchain *models.OffChainMetrics,
	token models.PreFilteredToken,
) (float64, float64) {
	baseROI := 0.25
	if offchain.Volume24hDEX > s.config.MinVolumeDEX*2 {
		baseROI += 0.10
	}
	if token.Token.InitialLiquidity.ReserveNative > 0 {
		baseROI += 0.05
	}
	if offchain.Velocity == "rising" {
		baseROI += 0.10
	}
	
	// Cap ROI
	if baseROI > 0.60 {
		baseROI = 0.60
	}
	return baseROI, 0.30
}

func (s *StrategyEvaluatorAgent) determineConfidence(
	safety *models.SafetyReport,
	winProb float64,
) string {
	if winProb >= 0.75 && safety.HoneypotScore < 0.08 {
		return "high"
	}
	if winProb >= 0.60 && safety.HoneypotScore < 0.15 {
		return "medium"
	}
	return "low"
}

func (s *StrategyEvaluatorAgent) determineAction(decision *models.StrategyDecision) string {
	if decision.WinProbability >= s.config.WinProbabilityThreshold &&
		decision.Confidence != "low" {
		if s.config.AutoExecute {
			return "buy"
		}
		return "list"
	}
	if decision.WinProbability >= 0.50 {
		return "monitor"
	}
	return "skip"
}

func (s *StrategyEvaluatorAgent) calculatePositionSize(decision *models.StrategyDecision) float64 {
	// Apply any runtime config overrides (e.g. dashboard DRY/LIVE toggle)
	s.config.ApplyOverrides()

	// In DRY_RUN mode, use a simulated $500 balance for realistic position sizing
	balance := s.liveBalance()
	if s.config.DryRun {
		balance = 500.0 // simulated capital for paper trading
	}

	// Reserve SOL for transaction fees & rent
	const reserveUSD = 5.0

	available := balance - reserveUSD
	if available <= 1.0 {
		log.Printf("StrategyEvaluatorAgent: Balance too low ($%.2f, reserve=$%.2f) — skipping\n",
			balance, reserveUSD)
		return 0
	}

	// Base position size from config
	maxPosition := available * s.config.SinglePositionPct

	// Scale by confidence (no random jitter)
	confidenceMultiplier := 1.0
	switch decision.Confidence {
	case "high":
		confidenceMultiplier = 1.0
	case "medium":
		confidenceMultiplier = 0.7
	case "low":
		confidenceMultiplier = 0.4
	}

	// Additional scaling by win probability relative to threshold
	probRatio := decision.WinProbability / s.config.WinProbabilityThreshold
	if probRatio > 1.2 {
		probRatio = 1.2
	}
	if probRatio < 0.5 {
		probRatio = 0.5
	}

	suggested := maxPosition * confidenceMultiplier * probRatio

	// Minimum size to cover fees
	if suggested < 1.0 {
		suggested = 1.0
	}

	// Don't suggest more than what's available
	if suggested > available {
		suggested = available
	}

	rounded := math.Round(suggested*100) / 100
	log.Printf("StrategyEvaluatorAgent: Position size — balance=$%.2f available=$%.2f maxPos=$%.2f → $%.2f (conf=%s, prob=%.2f)\n",
		balance, available, maxPosition, rounded, decision.Confidence, decision.WinProbability)
	return rounded
}

func (s *StrategyEvaluatorAgent) calculateStopLoss(decision *models.StrategyDecision) float64 {
	// Higher confidence = tighter stop loss (protect gains)
	switch decision.Confidence {
	case "high":
		return 0.08 // 8% stop loss
	case "medium":
		return 0.12 // 12% stop loss
	}
	return 0.18 // 18% stop loss for low confidence (avoid noise)
}

func (s *StrategyEvaluatorAgent) calculateTakeProfit(decision *models.StrategyDecision) float64 {
	// Scale by win probability
	baseTP := 0.25
	if decision.WinProbability > 0.75 {
		baseTP = 0.40
	} else if decision.WinProbability > 0.65 {
		baseTP = 0.30
	}
	
	// Cap at reasonable levels
	if baseTP > 0.60 {
		baseTP = 0.60
	}
	return math.Round(baseTP*100) / 100
}

func (s *StrategyEvaluatorAgent) calculateTimeHorizon(decision *models.StrategyDecision) int {
	switch decision.Confidence {
	case "high":
		return 60
	case "medium":
		return 45
	}
	return 30
}
