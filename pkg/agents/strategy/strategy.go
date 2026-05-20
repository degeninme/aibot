package strategy

import (
	"log"
	"math"
	"math/rand"
	"strconv"
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

	decision.WinProbability = s.calculateWinProbability(safety, offchain, token, decision)
	decision.ExpectedROI, decision.ExpectedROIStd = s.calculateExpectedROI(offchain, token)
	decision.Confidence = s.determineConfidence(safety, decision.WinProbability, token)
	decision.Action = s.determineAction(decision)
	decision.SuggestedAmountUSD = s.calculatePositionSize(decision)
	decision.StopLossPct = s.calculateStopLoss(decision)
	decision.TakeProfitPct = s.calculateTakeProfit(decision)
	decision.TimeHorizonMinutes = s.calculateTimeHorizon(decision)

	log.Printf("StrategyEvaluatorAgent: Token %s - WinMarketProb: %.2f, Action: %s, Confidence: %s\n",
		token.Token.TokenAddress, decision.WinProbability, decision.Action, decision.Confidence)

	return decision, nil
}

func (s *StrategyEvaluatorAgent) calculateWinProbability(
	safety *models.SafetyReport,
	offchain *models.OffChainMetrics,
	token models.PreFilteredToken,
	decision *models.StrategyDecision,
) float64 {
	// 1. Hard Rules Check
	if !safety.CanBuy || !safety.CanSell {
		decision.Rationale = append(decision.Rationale, "BLOCK: Token transfer flags reporting un-tradeable state")
		return 0.0
	}

	isPumpFun := token.Token.Metadata["source"] == "pumpfun"

	// 2. Base Configuration Adjustments
	var score float64
	if isPumpFun {
		// PumpFun baseline configuration starts lower because noise floor is high
		score = 0.40
	} else {
		score = 0.55
	}

	// 3. Evaluate Metadata Verification Signals
	hasTwitter := token.Token.Metadata["twitter"] != "" && token.Token.Metadata["twitter"] != "null"
	hasTelegram := token.Token.Metadata["telegram"] != "" && token.Token.Metadata["telegram"] != "null"
	hasWebsite := token.Token.Metadata["website"] != "" && token.Token.Metadata["website"] != "null"

	if isPumpFun {
		if hasTwitter && hasTelegram {
			score += 0.25
			decision.Rationale = append(decision.Rationale, "BONUS: Full digital footprint matched (X & TG links verified)")
		} else if hasTwitter || hasTelegram {
			score += 0.10
			decision.Rationale = append(decision.Rationale, "NEUTRAL: Partial digital marketing footprints matched")
		} else {
			// Severe penalty for completely anonymous programmatic bot creation arrays
			score -= 0.35
			decision.Rationale = append(decision.Rationale, "PENALTY: Blind metadata submission detected (No socials provided)")
		}

		// 4. Evaluate Dev Wallet Skin in the Game / Capitalization Constraints
		if devBalStr, ok := token.Token.Metadata["dev_sol_balance"]; ok {
			if devBal, err := strconv.ParseFloat(devBalStr, 64); err == nil {
				if devBal >= 1.5 {
					score += 0.15
					decision.Rationale = append(decision.Rationale, "BONUS: Heavily capitalized creation wallet (>1.5 SOL balance)")
				} else if devBal < 0.2 {
					score -= 0.15
					decision.Rationale = append(decision.Rationale, "PENALTY: Under-capitalized creation burner wallet (<0.2 SOL balance)")
				}
			}
		}

		// 5. Evaluate Structural Insider Distribution Metrics (Bundler Protection)
		if devBuyPctStr, ok := token.Token.Metadata["dev_buy_pct"]; ok {
			if devBuyPct, err := strconv.ParseFloat(devBuyPctStr, 64); err == nil {
				if devBuyPct > 12.0 {
					score -= 0.30
					decision.Rationale = append(decision.Rationale, "PENALTY: Massive developer transaction bundle concentration detected")
				} else if devBuyPct > 0.0 && devBuyPct <= 5.0 {
					score += 0.10
					decision.Rationale = append(decision.Rationale, "BONUS: Organic developer entry position distribution matched")
				}
			}
		}
	} else {
		// Standard Non-Pump DEX Safety Scoring Realignment
		if safety.HoneypotScore < 0.1 {
			score += 0.08
		} else if safety.HoneypotScore > s.config.MaxHoneypotScore {
			score -= 0.15
		}
		if safety.LiquidityLocked {
			score += 0.07
		}
		if safety.OwnerControls.Renounced {
			score += 0.06
		}
	}

	// 6. Velocity and Priority Processing
	switch offchain.Velocity {
	case "rising":
		score += 0.08
		decision.Rationale = append(decision.Rationale, "BONUS: Dynamic buyer transaction signature velocity accelerating")
	case "falling":
		score -= 0.10
		decision.Rationale = append(decision.Rationale, "PENALTY: Momentum loss identified over monitoring interval")
	}

	if token.Priority == "high" {
		score += 0.05
	}

	// Normalize bound limits
	if score > 1.0 {
		score = 1.0
	}
	if score < 0.0 {
		score = 0.0
	}
	return score
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
	token models.PreFilteredToken,
) string {
	isPumpFun := token.Token.Metadata["source"] == "pumpfun"

	if isPumpFun {
		// Strict structural rules for high-confidence allocation targets on PumpFun
		hasTwitter := token.Token.Metadata["twitter"] != "" && token.Token.Metadata["twitter"] != "null"
		hasTelegram := token.Token.Metadata["telegram"] != "" && token.Token.Metadata["telegram"] != "null"
		
		if winProb >= 0.75 && hasTwitter && hasTelegram {
			return "high"
		}
		if winProb >= 0.55 && (hasTwitter || hasTelegram) {
			return "medium"
		}
		return "low"
	}

	// Fallback to legacy safety matrices for standard Raydium DEX pools
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
	if decision.WinProbability >= 0.50 {
		return "monitor"
	}
	return "skip"
}

func (s *StrategyEvaluatorAgent) calculatePositionSize(decision *models.StrategyDecision) float64 {
	s.config.ApplyOverrides()

	balance := s.liveBalance()
	if s.config.DryRun {
		balance = 500.0 
	}

	const reserveUSD = 2.0
	available := balance - reserveUSD
	if available <= 0.5 {
		log.Printf("StrategyEvaluatorAgent: Balance too low ($%.2f, reserve=$%.2f) — skipping\n",
			balance, reserveUSD)
		return 0
	}

	maxPosition := available * s.config.SinglePositionPct
	if maxPosition > available {
		maxPosition = available
	}

	multiplier := 1.0
	switch decision.Confidence {
	case "high":
		multiplier = 1.0
	case "medium":
		multiplier = 0.65
	case "low":
		multiplier = 0.25
	}

	target := maxPosition * multiplier

	// Secure diversification window ranges
	jitter := 0.75 + rand.Float64()*0.25
	suggested := target * jitter

	if suggested < 0.5 {
		suggested = 0.5
	}
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
