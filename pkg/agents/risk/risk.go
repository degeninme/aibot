package risk

import (
	"log"
	"sync"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

// BalanceProvider is a function that returns the current account balance in USD
// If nil or returns 0, the config.AccountBalance is used as fallback.
type BalanceProvider func() float64

// RiskManagerAgent manages risk controls and circuit breakers
type RiskManagerAgent struct {
	config          *config.Config
	control         *models.RiskControl
	balanceProvider BalanceProvider
	mu              sync.RWMutex
}

// NewRiskManagerAgent creates a new risk manager
func NewRiskManagerAgent(cfg *config.Config) *RiskManagerAgent {
	return &RiskManagerAgent{
		config: cfg,
		control: &models.RiskControl{
			SinglePositionPct: cfg.SinglePositionPct,
			TotalExposurePct:  cfg.TotalExposurePct,
			DailyLossLimit:    cfg.DailyLossLimit,
			CurrentExposure:   0,
			DailyLoss:         0,
			TradingHalted:     false,
			LastResetTime:     time.Now(),
		},
	}
}

// SetBalanceProvider wires up a function that returns live wallet balance in USD.
// Call this once after creating both the risk manager and execution agent.
func (r *RiskManagerAgent) SetBalanceProvider(p BalanceProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.balanceProvider = p
	log.Println("RiskManager: Live balance provider configured")
}

// currentBalance returns live balance if provider exists, else falls back to config
func (r *RiskManagerAgent) currentBalance() float64 {
	if r.balanceProvider != nil {
		if b := r.balanceProvider(); b > 0 {
			return b
		}
	}
	return r.config.AccountBalance
}

// CanExecute checks if a trade can be executed based on risk controls
func (r *RiskManagerAgent) CanExecute(decision *models.StrategyDecision) (bool, string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.control.TradingHalted {
		return false, "trading_halted"
	}

	balance := r.currentBalance()

	maxSinglePosition := balance * r.config.SinglePositionPct
	if decision.SuggestedAmountUSD > maxSinglePosition {
		log.Printf("RiskManager: Trade rejected - exceeds single position limit (%.2f > %.2f, balance=%.2f)\n",
			decision.SuggestedAmountUSD, maxSinglePosition, balance)
		return false, "exceeds_single_position_limit"
	}

	maxTotalExposure := balance * r.config.TotalExposurePct
	if r.control.CurrentExposure+decision.SuggestedAmountUSD > maxTotalExposure {
		log.Printf("RiskManager: Trade rejected - exceeds total exposure limit (%.2f > %.2f, balance=%.2f)\n",
			r.control.CurrentExposure+decision.SuggestedAmountUSD, maxTotalExposure, balance)
		return false, "exceeds_total_exposure_limit"
	}

	if r.control.DailyLoss >= r.config.DailyLossLimit {
		log.Printf("RiskManager: Trade rejected - daily loss limit reached (%.2f)\n",
			r.control.DailyLoss)
		r.haltTrading()
		return false, "daily_loss_limit_reached"
	}

	log.Printf("RiskManager: Trade approved for %s (%.2f USD, live balance=%.2f)\n",
		decision.TokenAddress, decision.SuggestedAmountUSD, balance)

	return true, ""
}

// RecordExecution records a trade execution
func (r *RiskManagerAgent) RecordExecution(result *models.ExecutionResult) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if result.Status == "confirmed" {
		r.control.CurrentExposure += result.AmountUSD
		log.Printf("RiskManager: Recorded execution - Current exposure: %.2f USD\n",
			r.control.CurrentExposure)
	}
}

// RecordProfit records profit/loss from a closed position
func (r *RiskManagerAgent) RecordProfit(tokenAddress string, profitLoss float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if profitLoss < 0 {
		r.control.DailyLoss += -profitLoss
		log.Printf("RiskManager: Recorded loss of %.2f - Daily loss: %.2f\n",
			-profitLoss, r.control.DailyLoss)
		if r.control.DailyLoss >= r.config.DailyLossLimit {
			r.haltTrading()
		}
	} else {
		log.Printf("RiskManager: Recorded profit of %.2f\n", profitLoss)
	}
}

// ReleaseExposure releases exposure when a position is closed
func (r *RiskManagerAgent) ReleaseExposure(amount float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.control.CurrentExposure -= amount
	if r.control.CurrentExposure < 0 {
		r.control.CurrentExposure = 0
	}
	log.Printf("RiskManager: Released exposure - Current exposure: %.2f USD\n",
		r.control.CurrentExposure)
}

// SyncExposure resets the current exposure based on actual open positions.
// Useful after manual sells - call periodically to keep exposure accurate.
func (r *RiskManagerAgent) SyncExposure(actualOpenPositions float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.control.CurrentExposure != actualOpenPositions {
		log.Printf("RiskManager: Syncing exposure: %.2f -> %.2f (likely manual sells detected)\n",
			r.control.CurrentExposure, actualOpenPositions)
		r.control.CurrentExposure = actualOpenPositions
	}
}

// haltTrading triggers the circuit breaker
func (r *RiskManagerAgent) haltTrading() {
	r.control.TradingHalted = true
	log.Println("RiskManager: CIRCUIT BREAKER TRIGGERED - Trading halted!")
}

func (r *RiskManagerAgent) ResumeTrading() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.control.TradingHalted = false
	log.Println("RiskManager: Trading resumed")
}

func (r *RiskManagerAgent) ResetDaily() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.control.DailyLoss = 0
	r.control.LastResetTime = time.Now()
	log.Println("RiskManager: Daily counters reset")
}

func (r *RiskManagerAgent) CheckDailyReset() {
	r.mu.RLock()
	lastReset := r.control.LastResetTime
	r.mu.RUnlock()
	if time.Since(lastReset) > 24*time.Hour {
		r.ResetDaily()
	}
}

func (r *RiskManagerAgent) GetStatus() *models.RiskControl {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return &models.RiskControl{
		SinglePositionPct: r.control.SinglePositionPct,
		TotalExposurePct:  r.control.TotalExposurePct,
		DailyLossLimit:    r.control.DailyLossLimit,
		CurrentExposure:   r.control.CurrentExposure,
		DailyLoss:         r.control.DailyLoss,
		TradingHalted:     r.control.TradingHalted,
		LastResetTime:     r.control.LastResetTime,
	}
}
