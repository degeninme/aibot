package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/orchestrator"
	"github.com/rs/cors"
)

var orch *orchestrator.Orchestrator

func main() {
	log.Println("Meme Coin Trading Bot - Starting...")
	
	// Load configuration
	cfg := config.LoadConfig()
	
	// Create orchestrator
	orch = orchestrator.NewOrchestrator(cfg)
	
	// Start orchestrator in background
	go orch.Start()
	
	// Start API server
	startAPIServer(cfg)
}

func startAPIServer(cfg *config.Config) {
	router := mux.NewRouter()
	
	// API routes (must come before static file handler)
	router.HandleFunc("/api/health", healthHandler).Methods("GET")
	router.HandleFunc("/api/status", statusHandler).Methods("GET")
	router.HandleFunc("/api/candidates", candidatesHandler).Methods("GET")
	router.HandleFunc("/api/metrics", metricsHandler).Methods("GET")
	router.HandleFunc("/api/risk", riskHandler).Methods("GET")
	router.HandleFunc("/api/risk/resume", resumeTradingHandler).Methods("POST")
	router.HandleFunc("/api/trades", tradesHandler).Methods("GET")
	router.HandleFunc("/api/wallet", walletHandler).Methods("GET")
	router.HandleFunc("/api/mode", modeHandler).Methods("GET")
	router.HandleFunc("/api/mode", setModeHandler).Methods("POST")
	router.HandleFunc("/api/risk/reset", resetExposureHandler).Methods("POST")
	router.HandleFunc("/api/llm/analyze", llmAnalyzeHandler).Methods("POST")
	router.HandleFunc("/api/llm/chat", llmChatHandler).Methods("POST")
	router.HandleFunc("/api/llm/stats", llmStatsHandler).Methods("GET")
	router.HandleFunc("/api/intel/stats", intelStatsHandler).Methods("GET")
	
	// Serve frontend static files for all other routes
	router.PathPrefix("/").Handler(http.FileServer(http.Dir("./frontend")))
	
	// CORS middleware
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"*"},
	})
	
	handler := c.Handler(router)
	
	// Start server
	port := ":8080"
	server := &http.Server{
		Addr:         port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	
	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		
		log.Println("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		orch.Stop()
		server.Shutdown(ctx)
	}()
	
	log.Printf("API server listening on %s\n", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}
}

// Health check endpoint
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// Status endpoint
func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	telemetry := orch.GetTelemetry()
	metrics := telemetry.GetMetrics()
	
	risk := orch.GetRisk()
	riskStatus := risk.GetStatus()
	
	listing := orch.GetListing()
	candidateCount := listing.GetCandidateCount()
	
	response := map[string]interface{}{
		"status":          "running",
		"candidate_count": candidateCount,
		"trading_halted":  riskStatus.TradingHalted,
		"metrics": map[string]interface{}{
			"tokens_found":    metrics.TokensFound,
			"tokens_filtered": metrics.TokensFiltered,
			"candidates":      metrics.CandidatesListed,
			"executions":      metrics.TradesExecuted,
		},
	}
	
	json.NewEncoder(w).Encode(response)
}

// Candidates endpoint
func candidatesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	listing := orch.GetListing()
	candidates := listing.GetAllCandidates()
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count":      len(candidates),
		"candidates": candidates,
	})
}

// Metrics endpoint
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	telemetry := orch.GetTelemetry()
	metrics := telemetry.GetMetrics()
	
	json.NewEncoder(w).Encode(metrics)
}

// Risk status endpoint
func riskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	risk := orch.GetRisk()
	status := risk.GetStatus()
	
	json.NewEncoder(w).Encode(status)
}

// Resume trading endpoint
func resumeTradingHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	risk := orch.GetRisk()
	risk.ResumeTrading()
	
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Trading resumed",
	})
}

// Trades endpoint - returns all closed trade outcomes
func tradesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	execAgent := orch.GetExecution()
	trades := execAgent.GetClosedTrades()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"count":  len(trades),
		"trades": trades,
	})
}

// Wallet endpoint - returns current SOL & USD balance
func walletHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	execAgent := orch.GetExecution()
	solBal := execAgent.GetWalletBalanceSOL()
	usdBal := execAgent.GetWalletBalanceUSD()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"sol":      solBal,
		"usd":      usdBal,
		"exposure": execAgent.GetCurrentExposureUSD(),
	})
}

// modeHandler returns the current DRY_RUN state
func modeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	cfg := orch.GetConfig()
	cfg.ApplyOverrides()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"dry_run":    cfg.DryRun,
		"overridden": config.IsDryRunOverridden(),
	})
}

// setModeHandler updates DRY_RUN at runtime. Body: {"mode": "dry"|"live"|"default"}
func setModeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var body struct {
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid body"})
		return
	}

	mode := strings.ToLower(body.Mode)
	if mode != "dry" && mode != "live" && mode != "default" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "mode must be dry, live, or default"})
		return
	}

	config.SetDryRunOverride(mode)
	cfg := orch.GetConfig()
	cfg.ApplyOverrides()

	log.Printf("MainAPI: Mode toggled to %s (DryRun=%v)\n", mode, cfg.DryRun)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ok",
		"mode":       mode,
		"dry_run":    cfg.DryRun,
		"overridden": config.IsDryRunOverridden(),
	})
}

// resetExposureHandler manually clears stuck risk-manager exposure.
// Useful when paper-trading or after manual sells when exposure gets stuck.
func resetExposureHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	orch.GetRisk().ResetExposure()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"message": "exposure cleared",
	})
}

// llmAnalyzeHandler (Tier 2) — runs daily reflection on closed trades
func llmAnalyzeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	llmClient := orch.GetLLM()
	if llmClient == nil || !llmClient.Enabled() {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "LLM not configured. Set ANTHROPIC_API_KEY in env.",
		})
		return
	}

	execAgent := orch.GetExecution()
	trades := execAgent.GetClosedTrades()

	if len(trades) < 5 {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":         "not enough closed trades yet",
			"trades_needed": 5,
			"trades_have":   len(trades),
		})
		return
	}

	// Cap to last 100 trades to keep prompt size reasonable
	if len(trades) > 100 {
		trades = trades[len(trades)-100:]
	}

	tradesJSON, _ := json.MarshalIndent(trades, "", "  ")

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	analysis, err := llmClient.AnalyzeTrades(ctx, string(tradesJSON))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"analysis":    analysis,
		"trade_count": len(trades),
		"stats":       llmClient.Stats(),
	})
}

// llmChatHandler (Tier 3) — dashboard chat with the bot state
func llmChatHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	llmClient := orch.GetLLM()
	if llmClient == nil || !llmClient.Enabled() {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "LLM not configured",
		})
		return
	}

	var req struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Message == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "message required"})
		return
	}

	execAgent := orch.GetExecution()
	riskMgr := orch.GetRisk()

	// Build current state snapshot
	state := map[string]interface{}{
		"wallet_sol":      execAgent.GetWalletBalanceSOL(),
		"wallet_usd":      execAgent.GetWalletBalanceUSD(),
		"exposure_usd":    execAgent.GetCurrentExposureUSD(),
		"risk":            riskMgr.GetStatus(),
		"recent_trades":   tail(execAgent.GetClosedTrades(), 20),
		"dry_run":         orch.GetConfig().DryRun,
		"llm_filter_on":   orch.GetConfig().LLMFilterEnabled,
	}
	stateJSON, _ := json.MarshalIndent(state, "", "  ")

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	answer, err := llmClient.Chat(ctx, req.Message, string(stateJSON))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"answer": answer,
		"stats":  llmClient.Stats(),
	})
}

// llmStatsHandler returns LLM usage stats
func llmStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	llmClient := orch.GetLLM()
	if llmClient == nil {
		json.NewEncoder(w).Encode(map[string]bool{"enabled": false})
		return
	}
	stats := llmClient.Stats()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled":             llmClient.Enabled(),
		"filter_active":       orch.GetConfig().LLMFilterEnabled,
		"call_count":          stats.CallCount,
		"total_input_tokens":  stats.TotalInputTokens,
		"total_output_tokens": stats.TotalOutputTokens,
		"estimated_cost_usd":  stats.EstimatedCostUSD,
	})
}

// tail returns the last n elements of a slice, or all if shorter
func tail[T any](s []T, n int) []T {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// intelStatsHandler returns Solana Tracker API usage stats
func intelStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	intel := orch.GetIntel()
	if intel == nil {
		json.NewEncoder(w).Encode(map[string]bool{"enabled": false})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled":    intel.Enabled(),
		"call_count": intel.CallCount(),
	})
}
