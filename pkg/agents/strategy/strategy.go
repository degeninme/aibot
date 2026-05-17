package strategy

import (
	"log"
	"math"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

type StrategyEvaluatorAgent struct {
	config *config.Config
}

func NewStrategyEvaluatorAgent(cfg *config.Config) *StrategyEvaluatorAgent {
	return &StrategyEvaluatorAgent{config: cfg}
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
	token models.PreFilteredToken,
) float64 {
	// Cannot trade at all = 0
	if !safety.CanBuy || !safety.CanSell {
		return 0.0
	}

	// Higher base for new PumpFun tokens — they have 0 volume/social by nature
	baseProb := 0.65

	// Safety bonuses
	if safety.HoneypotScore < 0.1 {
		baseProb += 0.08
	} else if safety.HoneypotScore > s.config.MaxHoneypotScore {
		baseProb -= 0.15
	}

	if safety.LiquidityLocked {
		baseProb += 0.07
	}

	if safety.OwnerControls.Renounced {
		baseProb += 0.06
	}

	if !safety.OwnerControls.HasBlacklist && !safety.OwnerControls.HasTransferHook {
		baseProb += 0.04
	}

	// Volume bonus (optional — new tokens won't have this yet)
	if offchain.Volume24hDEX >= s.config.MinVolumeDEX {
		baseProb += 0.08
	}

	// Social bonus (optional)
	totalMentions := 0
	for _, count := range offchain.SocialMentions {
		totalMentions += count
	}
	if totalMentions > 50 {
		baseProb += 0.06
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
	maxPosition := s.config.AccountBalance * s.config.SinglePositionPct
	multiplier := 1.0
	switch decision.Confidence {
	case "medium":
		multiplier = 0.7
	case "low":
		multiplier = 0.4
	}
	suggested := maxPosition * multiplier
	if suggested < 10 {
		suggested = 10
	}
	return math.Round(suggested*100) / 100
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
