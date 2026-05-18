package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
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
