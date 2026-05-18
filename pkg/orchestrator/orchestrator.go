package orchestrator

import (
	"context"
	"log"
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
	
	ctx    context.Context
	cancel context.CancelFunc
}

// NewOrchestrator creates a new orchestrator
func NewOrchestrator(cfg *config.Config) *Orchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	exec := execution.NewExecutionAgent(cfg)
	riskMgr := risk.NewRiskManagerAgent(cfg)

	// Wire up live wallet balance to risk manager
	riskMgr.SetBalanceProvider(func() float64 {
		return exec.GetWalletBalanceUSD()
	})

	return &Orchestrator{
		config:    cfg,
		scanner:   scanner.NewChainScannerAgent(cfg),
		prefilter: prefilter.NewPreFilterAgent(cfg),
		safety:    safety.NewOnChainSafetyAgent(cfg),
		offchain:  offchain.NewOffChainDataAgent(cfg),
		strategy:  strategy.NewStrategyEvaluatorAgent(cfg),
		listing:   listing.NewCandidateListingAgent(),
		execution: exec,
		risk:      riskMgr,
		telemetry: telemetry.NewTelemetryAgent(),
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
