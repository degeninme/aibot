package strategy

import (
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

	log.Printf("StrategyEvaluatorAgent: Token %s - WinProb: %.2f, Action: %s, Confidence: %s\n",
		token.Token.TokenAddress, decision.WinProbability, decision.Action, decision.Confidence)

	return decision, nil
}

func (s *StrategyEvaluatorAgent) calculateWinProbability(
	safety *models.SafetyReport,
	offchain *models.OffChainMetrics,
func (s *StrategyEvaluatorAgent) calculateWinProbability(
    safety *models.SafetyReport,
    offchain *models.OffChainMetrics,
    token models.PreFilteredToken,
) float64 {
    if !safety.CanBuy || !safety.CanSell {
        return 0.0
    }
    
    // Start lower - new tokens are inherently risky
    baseProb := 0.45
    
    // Dev check results (you need to add this to your SafetyReport or pass separately)
    // For now, use token.Priority as proxy
    if token.Priority == "high" {
        baseProb += 0.10  // Dev holds reasonable amount (2-5%)
    } else if token.Priority == "low" {
        baseProb -= 0.15  // Dev sold or holds too much
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
    
    // Volume - only relevant if token has traded
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
    
    // PumpFun source bonus
    if token.Token.Metadata["source"] == "pumpfun" {
        baseProb += 0.05
    }
    
    // Cap at realistic levels
    if baseProb > 0.82 {
        baseProb = 0.82
    }
    if baseProb < 0.0 {
        baseProb = 0.0
    }
    
    return baseProb
}
	// Velocity
	switch offchain.Velocity {
	case "rising":
		baseProb += 0.05
	case "falling":
		baseProb -= 0.05
	}
	// "stable" or "" (unknown for brand-new tokens) = no change

	// Priority
	switch token.Priority {
	case "high":
		baseProb += 0.05
	case "low":
		baseProb -= 0.03
	}

	// PumpFun source bonus
	if token.Token.Metadata["source"] == "pumpfun" {
		baseProb += 0.02
	}

	if baseProb > 1.0 {
		baseProb = 1.0
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
	baseROI := 0.20
	if offchain.Volume24hDEX > s.config.MinVolumeDEX*2 {
		baseROI += 0.10
	}
	if token.Token.InitialLiquidity.ReserveNative > 0 {
		baseROI += 0.05
	}
	if offchain.Velocity == "rising" {
		baseROI += 0.10
	}
	return baseROI, 0.30
}

func (s *StrategyEvaluatorAgent) determineConfidence(
	safety *models.SafetyReport,
	winProb float64,
) string {
	if winProb >= 0.82 && safety.HoneypotScore < 0.1 {
		return "high"
	}
	if winProb >= 0.70 && safety.HoneypotScore < 0.2 {
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
	if decision.WinProbability >= 0.60 {
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

	// Reserve some SOL for transaction fees & rent
	const reserveUSD = 2.0

	available := balance - reserveUSD
	if available <= 0.5 {
		log.Printf("StrategyEvaluatorAgent: Balance too low ($%.2f, reserve=$%.2f) — skipping\n",
			balance, reserveUSD)
		return 0
	}

	// Cap by single position percentage
	maxPosition := available * s.config.SinglePositionPct
	if maxPosition > available {
		maxPosition = available
	}

	// Apply confidence multiplier
	multiplier := 1.0
	switch decision.Confidence {
	case "medium":
		multiplier = 0.7
	case "low":
		multiplier = 0.4
	}

	target := maxPosition * multiplier

	// Randomize 70-100% of target to vary entry sizes (helps with diversification)
	jitter := 0.7 + rand.Float64()*0.3
	suggested := target * jitter

	// Floor at $0.50 to avoid dust trades that don't cover fees
	if suggested < 0.5 {
		suggested = 0.5
	}

	// Don't suggest more than what's available
	if suggested > available {
		suggested = available
	}

	rounded := math.Round(suggested*100) / 100
	log.Printf("StrategyEvaluatorAgent: Position size — balance=$%.2f available=$%.2f maxPos=$%.2f → $%.2f\n",
		balance, available, maxPosition, rounded)
	return rounded
}

func (s *StrategyEvaluatorAgent) calculateStopLoss(decision *models.StrategyDecision) float64 {
	switch decision.Confidence {
	case "high":
		return 0.20
	case "medium":
		return 0.15
	}
	return 0.10
}

func (s *StrategyEvaluatorAgent) calculateTakeProfit(decision *models.StrategyDecision) float64 {
	baseTP := decision.ExpectedROI * 1.5
	if baseTP < 0.20 {
		baseTP = 0.20
	}
	if baseTP > 1.00 {
		baseTP = 1.00
	}
	return math.Round(baseTP*100) / 100
}

func (s *StrategyEvaluatorAgent) calculateTimeHorizon(decision *models.StrategyDecision) int {
	switch decision.Confidence {
	case "high":
		return 60
	case "medium":
		return 30
	}
	return 15
}
