package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/agents/execution"
	"github.com/mumugogoing/meme_bot/pkg/agents/listing"
	"github.com/mumugogoing/meme_bot/pkg/agents/offchain"
	"github.com/mumugogoing/meme_bot/pkg/agents/prefilter"
	"github.com/mumugogoing/meme_bot/pkg/agents/risk"
	"github.com/mumugogoing/meme_bot/pkg/agents/safety"
	"github.com/mumugogoing/meme_bot/pkg/agents/scanner"
	"github.com/mumugogoing/meme_bot/pkg/agents/strategy"
	"github.com/mumugogoing/meme_bot/pkg/agents/telemetry"
	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/intelligence"
	"github.com/mumugogoing/meme_bot/pkg/llm"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

// Orchestrator coordinates all agents
type Orchestrator struct {
	config *config.Config
	
	// Agents
	scanner   *scanner.ChainScannerAgent
	prefilter *prefilter.PreFilterAgent
	safety    *safety.OnChainSafetyAgent
	offchain  *offchain.OffChainDataAgent
	strategy  *strategy.StrategyEvaluatorAgent
	listing   *listing.CandidateListingAgent
	execution *execution.ExecutionAgent
	risk      *risk.RiskManagerAgent
	telemetry *telemetry.TelemetryAgent
	llm       *llm.Client
	intel     *intelligence.Client
	
	ctx    context.Context
	cancel context.CancelFunc
}

// NewOrchestrator creates a new orchestrator
func NewOrchestrator(cfg *config.Config) *Orchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	exec := execution.NewExecutionAgent(cfg)
	riskMgr := risk.NewRiskManagerAgent(cfg)
	strategyAgent := strategy.NewStrategyEvaluatorAgent(cfg)

	// Wire up live wallet balance to risk manager AND strategy
	balanceProvider := func() float64 {
		return exec.GetWalletBalanceUSD()
	}
	riskMgr.SetBalanceProvider(balanceProvider)
	strategyAgent.SetBalanceProvider(balanceProvider)

	// Start a goroutine that keeps risk exposure in sync with actual open positions
	// This handles cases where the user manually sold tokens, AND keeps DRY_RUN exposure
	// from accumulating forever as paper positions close
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			actualExposure := exec.GetCurrentExposureUSD()
			riskMgr.SyncExposure(actualExposure)
		}
	}()

	llmClient := llm.NewClient()
	intelClient := intelligence.NewClient()

	return &Orchestrator{
		config:    cfg,
		scanner:   scanner.NewChainScannerAgent(cfg),
		prefilter: prefilter.NewPreFilterAgent(cfg),
		safety:    safety.NewOnChainSafetyAgent(cfg),
		offchain:  offchain.NewOffChainDataAgent(cfg),
		strategy:  strategyAgent,
		listing:   listing.NewCandidateListingAgent(),
		execution: exec,
		risk:      riskMgr,
		telemetry: telemetry.NewTelemetryAgent(),
		llm:       llmClient,
		intel:     intelClient,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start starts the orchestration
func (o *Orchestrator) Start() {
	log.Println("Orchestrator: Starting meme coin trading bot...")
	log.Printf("Orchestrator: DryRun=%v, AutoExecute=%v\n", o.config.DryRun, o.config.AutoExecute)
	
	// Start periodic telemetry logging
	telemetryStop := o.telemetry.StartPeriodicLogging(30 * time.Second)
	defer close(telemetryStop)
	
	// Start chain scanner
	o.scanner.Start()
	defer o.scanner.Stop()
	
	// Start the processing pipeline
	go o.processTokens()
	
	// Start execution processor
	go o.processExecutions()
	
	// Wait for shutdown signal
	<-o.ctx.Done()
	log.Println("Orchestrator: Shutting down...")
}

// Stop stops the orchestration
func (o *Orchestrator) Stop() {
	o.cancel()
}

// processTokens processes discovered tokens through the pipeline
func (o *Orchestrator) processTokens() {
	log.Println("Orchestrator: Token processing pipeline started")
	
	for {
		select {
		case <-o.ctx.Done():
			return
		case token := <-o.scanner.GetTokenChannel():
			go o.processToken(token)
		}
	}
}

// processToken processes a single token through the entire pipeline
func (o *Orchestrator) processToken(token models.TokenFound) {
	startTime := time.Now()
	
	log.Printf("Orchestrator: Processing token %s on %s\n", token.TokenAddress, token.Chain)
	o.telemetry.RecordTokenFound()
	
	// Step 1: Pre-filtering
	prefiltered := o.prefilter.Filter(token)
	o.telemetry.RecordTokenFiltered(prefiltered.Dropped)
	
	if prefiltered.Dropped {
		log.Printf("Orchestrator: Token %s dropped by pre-filter\n", token.TokenAddress)
		return
	}
	
	// Step 2: Safety evaluation
	safetyReport, err := o.safety.Evaluate(o.ctx, prefiltered)
	if err != nil {
		log.Printf("Orchestrator: Safety evaluation failed for %s: %v\n", token.TokenAddress, err)
		return
	}
	
	isHoneypot := safetyReport.HoneypotScore >= o.config.MaxHoneypotScore
	isSafe := o.safety.CanTrade(safetyReport)
	o.telemetry.RecordSafetyCheck(isHoneypot, isSafe)
	
	if !isSafe {
		log.Printf("Orchestrator: Token %s failed safety check (honeypot score: %.2f)\n",
			token.TokenAddress, safetyReport.HoneypotScore)
		return
	}
	
	// Step 3: Off-chain data gathering
	offchainMetrics, err := o.offchain.Gather(o.ctx, prefiltered)
	if err != nil {
		log.Printf("Orchestrator: Off-chain data gathering failed for %s: %v\n", token.TokenAddress, err)
		return
	}
	
	// Step 4: Strategy evaluation
	decision, err := o.strategy.Evaluate(safetyReport, offchainMetrics, prefiltered)
	if err != nil {
		log.Printf("Orchestrator: Strategy evaluation failed for %s: %v\n", token.TokenAddress, err)
		return
	}
	
	o.telemetry.RecordEvaluation()
	o.telemetry.RecordDecisionLatency(time.Since(startTime))
	
	log.Printf("Orchestrator: Token %s - WinProb: %.2f, Action: %s, Confidence: %s\n",
		token.TokenAddress, decision.WinProbability, decision.Action, decision.Confidence)
	
	// Step 5: Check if should list/execute
	if decision.Action == "list" || decision.Action == "buy" {
		candidate := o.listing.AddCandidate(token, *safetyReport, *offchainMetrics, *decision)
		o.telemetry.RecordCandidateListed()
		
		log.Printf("Orchestrator: Token %s added to candidate list\n", token.TokenAddress)
		
		// If auto-execute is enabled and action is buy, it will be processed by execution processor
		if decision.Action == "buy" && o.config.AutoExecute {
			log.Printf("Orchestrator: Token %s queued for execution\n", candidate.Token.TokenAddress)
		}
	} else {
		log.Printf("Orchestrator: Token %s action: %s - not listing\n", token.TokenAddress, decision.Action)
	}
}

// processExecutions processes execution queue
func (o *Orchestrator) processExecutions() {
	log.Println("Orchestrator: Execution processor started")
	
	for {
		select {
		case <-o.ctx.Done():
			return
		case candidate := <-o.listing.GetQueue():
			if o.config.AutoExecute && candidate.StrategyDecision.Action == "buy" {
				go o.executeCandidate(candidate)
			}
		}
	}
}

// executeCandidate executes a trade for a candidate
func (o *Orchestrator) executeCandidate(candidate *models.CandidateToken) {
	startTime := time.Now()
	
	log.Printf("Orchestrator: Executing candidate %s\n", candidate.Token.TokenAddress)
	
	// Check daily reset
	o.risk.CheckDailyReset()

	// ── LAYER 1: Developer Profiling ───────────────────────────────────────
	// Skip if dev wallet is underfunded or a serial rugger.
	if o.intel != nil && envBoolDefault("LAYER1_ENABLED", true) {
		devAddr := candidate.Token.CreatorAddress
		if devAddr == "" {
			log.Printf("Orchestrator: ⚠️ No creator address for %s, skipping Layer 1\n",
				candidate.Token.TokenAddress)
		} else {
			// Fetch dev SOL balance via RPC
			devSol := o.fetchSolBalance(devAddr)
			intelCtx, cancel := context.WithTimeout(o.ctx, 5*time.Second)
			devAssessment := o.intel.AssessDeveloper(intelCtx, devAddr, devSol)
			cancel()

			log.Printf("Orchestrator: Layer 1 dev=%s sol=%.3f pnl=$%.0f winRate=%.0f%% trades=%d skip=%v reason=%s\n",
				devAddr[:10], devAssessment.SolBalance, devAssessment.TotalPnL,
				devAssessment.WinRate, devAssessment.TotalTrades,
				devAssessment.Skip, devAssessment.Reason)

			if devAssessment.Skip {
				log.Printf("Orchestrator: ❌ LAYER 1 REJECTED %s — %s\n",
					candidate.Token.TokenAddress, devAssessment.Reason)
				o.listing.UpdateStatus(candidate.Token.TokenAddress, "layer1_rejected")
				return
			}
		}
	}

	// ── LAYER 3: Metadata & Socials (also includes Layer 4 bonding progress) ──
	// Skip if no social presence, high risk score, or wrong bonding curve stage.
	if o.intel != nil && o.intel.Enabled() && envBoolDefault("LAYER3_ENABLED", true) {
		intelCtx, cancel := context.WithTimeout(o.ctx, 5*time.Second)
		tokenAssessment := o.intel.AssessToken(intelCtx, candidate.Token.TokenAddress)
		cancel()

		log.Printf("Orchestrator: Layer 3 token=%s tw=%v tg=%v web=%v risk=%.1f progress=%.1f%% liq=$%.0f mc=$%.0f skip=%v reason=%s\n",
			candidate.Token.TokenAddress[:10],
			tokenAssessment.HasTwitter, tokenAssessment.HasTelegram, tokenAssessment.HasWebsite,
			tokenAssessment.RiskScore, tokenAssessment.BondingProgress,
			tokenAssessment.LiquidityUSD, tokenAssessment.MarketCapUSD,
			tokenAssessment.Skip, tokenAssessment.Reason)

		if tokenAssessment.Skip {
			log.Printf("Orchestrator: ❌ LAYER 3 REJECTED %s — %s\n",
				candidate.Token.TokenAddress, tokenAssessment.Reason)
			o.listing.UpdateStatus(candidate.Token.TokenAddress, "layer3_rejected")
			return
		}

		log.Printf("Orchestrator: ✓ LAYERS 1+3 PASSED for %s — proceeding to execution\n",
			candidate.Token.TokenAddress)
	}

	// ── TIER 1 LLM FILTER (non-blocking, 2s timeout) ───────────────────────
	// Runs in parallel with risk checks. If LLM says "don't buy", skip.
	// If LLM doesn't respond within 2s OR is disabled, proceed without it.
	if o.llm != nil && o.llm.Enabled() && o.config.LLMFilterEnabled {
		llmCtx, llmCancel := context.WithTimeout(o.ctx, 2*time.Second)
		llmResult := make(chan *llm.TokenFilterResult, 1)

		go func() {
			defer llmCancel()
			input := llm.TokenFilterInput{
				Mint:        candidate.Token.TokenAddress,
				Name:        candidate.Token.Metadata["name"],
				Symbol:      candidate.Token.Metadata["symbol"],
				Description: candidate.Token.Metadata["description"],
				HasTwitter:  candidate.Token.Metadata["twitter"] != "",
				HasTelegram: candidate.Token.Metadata["telegram"] != "",
				HasWebsite:  candidate.Token.Metadata["website"] != "",
			}
			// Parse dev_pct if present
			if v := candidate.Token.Metadata["dev_pct"]; v != "" {
				var f float64
				fmt.Sscanf(v, "%f", &f)
				input.DevPctHolds = f
			}

			result, err := o.llm.FilterToken(llmCtx, input)
			if err != nil {
				log.Printf("Orchestrator: LLM filter error for %s: %v\n", candidate.Token.TokenAddress, err)
				llmResult <- nil
				return
			}
			llmResult <- result
		}()

		select {
		case result := <-llmResult:
			if result != nil {
				log.Printf("Orchestrator: LLM filter for %s — buy=%v score=%d reason=%q\n",
					candidate.Token.TokenAddress, result.Buy, result.Score, result.Reason)
				if !result.Buy {
					log.Printf("Orchestrator: ❌ LLM REJECTED %s (flags: %v)\n",
						candidate.Token.TokenAddress, result.RedFlags)
					o.listing.UpdateStatus(candidate.Token.TokenAddress, "llm_rejected")
					return
				}
			}
		case <-time.After(2 * time.Second):
			log.Printf("Orchestrator: LLM filter timed out for %s — proceeding without LLM verdict\n",
				candidate.Token.TokenAddress)
			// llmCancel is in goroutine; the goroutine will discard its result
		}
	}

	// Check risk management
	canExecute, reason := o.risk.CanExecute(&candidate.StrategyDecision)
	if !canExecute {
		log.Printf("Orchestrator: Execution blocked by risk manager: %s\n", reason)
		o.listing.UpdateStatus(candidate.Token.TokenAddress, "rejected")
		return
	}
	
	// Simulate first if not in dry run
	if !o.config.DryRun {
		success, err := o.execution.Simulate(o.ctx, candidate)
		if err != nil || !success {
			log.Printf("Orchestrator: Simulation failed for %s\n", candidate.Token.TokenAddress)
			o.telemetry.RecordSimulationFailure()
			o.listing.UpdateStatus(candidate.Token.TokenAddress, "rejected")
			return
		}
	}
	
	// Execute trade
	result, err := o.execution.Execute(o.ctx, candidate)
	o.telemetry.RecordExecutionTime(time.Since(startTime))
	
	if err != nil {
		log.Printf("Orchestrator: Execution failed for %s: %v\n", candidate.Token.TokenAddress, err)
		o.telemetry.RecordExecution(false, 0)
		o.listing.UpdateStatus(candidate.Token.TokenAddress, "failed")
		return
	}
	
	if result.Status == "confirmed" {
		log.Printf("Orchestrator: Execution successful for %s - TX: %s\n",
			candidate.Token.TokenAddress, result.TxHash)
		o.telemetry.RecordExecution(true, result.AmountUSD)
		o.risk.RecordExecution(result)
		o.listing.UpdateStatus(candidate.Token.TokenAddress, "executed")
	} else {
		log.Printf("Orchestrator: Execution status %s for %s\n",
			result.Status, candidate.Token.TokenAddress)
		o.telemetry.RecordExecution(false, 0)
		o.listing.UpdateStatus(candidate.Token.TokenAddress, "failed")
	}
}

// GetTelemetry returns the telemetry agent
func (o *Orchestrator) GetTelemetry() *telemetry.TelemetryAgent {
	return o.telemetry
}

// GetListing returns the listing agent
func (o *Orchestrator) GetListing() *listing.CandidateListingAgent {
	return o.listing
}

// GetRisk returns the risk manager
func (o *Orchestrator) GetRisk() *risk.RiskManagerAgent {
	return o.risk
}

// GetExecution returns the execution agent
func (o *Orchestrator) GetExecution() *execution.ExecutionAgent {
	return o.execution
}

// GetConfig returns the active configuration
func (o *Orchestrator) GetConfig() *config.Config {
	return o.config
}

// GetLLM returns the LLM client
func (o *Orchestrator) GetLLM() *llm.Client {
	return o.llm
}

// fetchSolBalance queries the RPC for a wallet's SOL balance
// Returns 0 on error (caller decides whether to fail open or skip)
func (o *Orchestrator) fetchSolBalance(address string) float64 {
	ctx, cancel := context.WithTimeout(o.ctx, 3*time.Second)
	defer cancel()

	bodyStr := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["%s"]}`, address)
	req, err := http.NewRequestWithContext(ctx, "POST", o.config.SolanaRPCURL, strings.NewReader(bodyStr))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 3 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Orchestrator: fetchSolBalance error: %v\n", err)
		return 0
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			Value uint64 `json:"value"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0
	}
	return float64(result.Result.Value) / 1e9 // lamports to SOL
}

// GetIntel returns the intelligence client (for stats endpoint)
func (o *Orchestrator) GetIntel() *intelligence.Client {
	return o.intel
}

// envBoolDefault reads a boolean env var with a default
func envBoolDefault(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "true" || v == "1" || v == "yes"
}
