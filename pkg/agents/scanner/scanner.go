package scanner

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

const (
	PumpFunProgram  = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
	PumpSwapProgram = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
)

// ─── RPC types ────────────────────────────────────────────────────────────────

type rpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type signatureInfo struct {
	Signature string      `json:"signature"`
	Slot      uint64      `json:"slot"`
	Err       interface{} `json:"err"`
	BlockTime *int64      `json:"blockTime"`
}

type txResult struct {
	Transaction struct {
		Message struct {
			AccountKeys []string `json:"accountKeys"`
		} `json:"message"`
	} `json:"transaction"`
	Meta struct {
		PostTokenBalances []tokenBalance `json:"postTokenBalances"`
		PreTokenBalances  []tokenBalance `json:"preTokenBalances"`
		LogMessages       []string       `json:"logMessages"`
		Err               interface{}    `json:"err"`
	} `json:"meta"`
	BlockTime *int64 `json:"blockTime"`
}

type tokenBalance struct {
	AccountIndex  int    `json:"accountIndex"`
	Mint          string `json:"mint"`
	Owner         string `json:"owner"`
	UITokenAmount struct {
		UIAmount float64 `json:"uiAmount"`
	} `json:"uiTokenAmount"`
}

// heliusTx is used for Helius enhanced transaction parsing
type heliusTx struct {
	Signature   string `json:"signature"`
	Type        string `json:"type"`
	Description string `json:"description"`
	TokenMint   string `json:"tokenMint"`
	FeePayer    string `json:"feePayer"`
	Timestamp   int64  `json:"timestamp"`
	Events      struct {
		Token *struct {
			Mint string `json:"mint"`
		} `json:"token"`
	} `json:"events"`
}

// ─── ChainScannerAgent ────────────────────────────────────────────────────────

type ChainScannerAgent struct {
	config       *config.Config
	tokenChannel chan models.TokenFound
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	httpClient   *http.Client
	seenSigs     map[string]bool
	seenSigsMu   sync.Mutex
	seenMints    map[string]bool
	seenMintsMu  sync.Mutex
	heliusAPIKey string
	pauseCheck   func() bool // returns true → skip current scan
}

func NewChainScannerAgent(cfg *config.Config) *ChainScannerAgent {
	ctx, cancel := context.WithCancel(context.Background())

	// Extract Helius API key from RPC URL if present
	apiKey := ""
	rpcURL := cfg.SolanaRPCURL
	if idx := strings.Index(rpcURL, "api-key="); idx != -1 {
		apiKey = rpcURL[idx+8:]
		if end := strings.Index(apiKey, "&"); end != -1 {
			apiKey = apiKey[:end]
		}
	}

	return &ChainScannerAgent{
		config:       cfg,
		tokenChannel: make(chan models.TokenFound, 100),
		ctx:          ctx,
		cancel:       cancel,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
		seenSigs:     make(map[string]bool),
		seenMints:    make(map[string]bool),
		heliusAPIKey: apiKey,
	}
}

func (s *ChainScannerAgent) Start() {
	log.Println("ChainScannerAgent: Starting chain monitoring...")
	s.wg.Add(1)
	go s.scanSolana()
	if os.Getenv("BASE_RPC_URL") != "" {
		s.wg.Add(1)
		go s.scanBase()
	} else {
		log.Println("ChainScannerAgent: BASE_RPC_URL not set, skipping Base chain scanning")
	}
	if s.heliusAPIKey != "" {
		log.Println("ChainScannerAgent: Helius API key detected - using enhanced transaction API")
	}
	log.Println("ChainScannerAgent: Chain monitoring started")
}

func (s *ChainScannerAgent) Stop() {
	log.Println("ChainScannerAgent: Stopping...")
	s.cancel()
	s.wg.Wait()
	close(s.tokenChannel)
	log.Println("ChainScannerAgent: Stopped")
}

func (s *ChainScannerAgent) GetTokenChannel() <-chan models.TokenFound {
	return s.tokenChannel
}

func (s *ChainScannerAgent) scanSolana() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.ScanIntervalSolana)
	defer ticker.Stop()
	log.Printf("ChainScannerAgent: Solana scanner started (interval: %v)\n", s.config.ScanIntervalSolana)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Skip if paused (saves RPC calls)
			if s.pauseCheck != nil && s.pauseCheck() {
				continue
			}
			s.scanSolanaNewTokens()
		}
	}
}

// SetPauseCheck wires up a function the scanner calls before each scan.
// If the function returns true, the scan is skipped.
func (s *ChainScannerAgent) SetPauseCheck(fn func() bool) {
	s.pauseCheck = fn
}

func (s *ChainScannerAgent) scanBase() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.ScanIntervalBase)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.scanBaseNewTokens()
		}
	}
}

func (s *ChainScannerAgent) rpcCall(method string, params []interface{}) (*rpcResponse, error) {
	req := rpcRequest{Jsonrpc: "2.0", ID: 1, Method: method, Params: params}
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(s.ctx, "POST", s.config.SolanaRPCURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var rpcResp rpcResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return &rpcResp, nil
}

func (s *ChainScannerAgent) getRecentSignatures(programID string, limit int) ([]signatureInfo, error) {
	resp, err := s.rpcCall("getSignaturesForAddress", []interface{}{
		programID,
		map[string]interface{}{"limit": limit, "commitment": "confirmed"},
	})
	if err != nil {
		return nil, err
	}
	var sigs []signatureInfo
	if err := json.Unmarshal(resp.Result, &sigs); err != nil {
		return nil, err
	}
	return sigs, nil
}

func (s *ChainScannerAgent) getTransaction(sig string) (*txResult, error) {
	resp, err := s.rpcCall("getTransaction", []interface{}{
		sig,
		map[string]interface{}{
			"encoding":                       "json",
			"maxSupportedTransactionVersion": 0,
		},
	})
	if err != nil {
		return nil, err
	}
	if string(resp.Result) == "null" {
		return nil, nil
	}
	var tx txResult
	if err := json.Unmarshal(resp.Result, &tx); err != nil {
		return nil, err
	}
	return &tx, nil
}

// getHeliusEnhancedTxs uses Helius enhanced transactions API
func (s *ChainScannerAgent) getHeliusEnhancedTxs(limit int) ([]heliusTx, error) {
	url := fmt.Sprintf("https://api.helius.xyz/v0/addresses/%s/transactions?api-key=%s&limit=%d",
		PumpFunProgram, s.heliusAPIKey, limit)
	httpReq, err := http.NewRequestWithContext(s.ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var txs []heliusTx
	if err := json.Unmarshal(body, &txs); err != nil {
		return nil, fmt.Errorf("helius parse error: %w (body: %s)", err, string(body[:minInt(200, len(body))]))
	}
	return txs, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *ChainScannerAgent) scanSolanaNewTokens() {
	log.Println("ChainScannerAgent: Scanning Solana for new tokens...")

	// Use Helius enhanced API if we have a key
	if s.heliusAPIKey != "" {
		s.scanWithHelius()
	} else {
		// Fallback: raw RPC with large limit
		s.scanProgram(PumpFunProgram, "pumpfun", 50)
	}
}

// scanWithHelius uses Helius enhanced API to get token creation events
func (s *ChainScannerAgent) scanWithHelius() {
	txs, err := s.getHeliusEnhancedTxs(50)
	if err != nil {
		log.Printf("ChainScannerAgent: Helius API error: %v, falling back to raw RPC\n", err)
		s.scanProgram(PumpFunProgram, "pumpfun", 50)
		return
	}

	found := 0
	for _, tx := range txs {
		// Only process create/mint type
		isCreate := strings.Contains(strings.ToLower(tx.Type), "create") ||
			strings.Contains(strings.ToLower(tx.Type), "mint")
		if !isCreate {
			continue
		}

		// Skip seen signatures
		s.seenSigsMu.Lock()
		seen := s.seenSigs[tx.Signature]
		if !seen {
			s.seenSigs[tx.Signature] = true
		}
		s.seenSigsMu.Unlock()
		if seen {
			continue
		}

		// Extract mint
		mint := tx.TokenMint
		if mint == "" && tx.Events.Token != nil {
			mint = tx.Events.Token.Mint
		}
		
		// Fall back to full tx fetch if mint not directly available
		if mint == "" || !strings.HasSuffix(mint, "pump") {
			fullTx, err := s.getTransaction(tx.Signature)
			if err != nil || fullTx == nil || fullTx.Meta.Err != nil {
				continue
			}
			token := s.extractTokenFromTx(fullTx, tx.Signature, "pumpfun")
			if token != nil {
				// Dev check before emitting
				devResult := s.checkDevWallet(token.TokenAddress, token.CreatorAddress)
				if devResult.skip {
					continue
				}
				// Set priority based on dev holdings
				priority := s.determinePriority(devResult.devPct)
				
				s.seenMintsMu.Lock()
				mintSeen := s.seenMints[token.TokenAddress]
				if !mintSeen {
					s.seenMints[token.TokenAddress] = true
				}
				s.seenMintsMu.Unlock()
				if !mintSeen {
					found++
					log.Printf("ChainScannerAgent: 🚀 New token via Helius+RPC! Mint: %s (dev %.2f%%, priority=%s)\n",
						token.TokenAddress[:10], devResult.devPct, priority)
					s.emitTokenFoundWithPriority(*token, priority)
				}
			}
			continue
		}

		// Deduplicate by mint
		s.seenMintsMu.Lock()
		mintSeen := s.seenMints[mint]
		if !mintSeen {
			s.seenMints[mint] = true
		}
		s.seenMintsMu.Unlock()
		if mintSeen {
			continue
		}

		// Mayhem mode check - need full tx for log messages
		fullTx, err := s.getTransaction(tx.Signature)
		if err == nil && fullTx != nil && fullTx.Meta.Err == nil {
			if isMayhemFromLogs(fullTx.Meta.LogMessages) {
				log.Printf("ChainScannerAgent: Skipping mayhem mode token: %s\n", mint[:10])
				continue
			}
		}

		// Dev wallet check — reject if dev sold or holds too much
		var devResult devCheckResult
		if os.Getenv("DEV_CHECK") != "false" {
			devResult = s.checkDevWallet(mint, tx.FeePayer)
			if devResult.skip {
				continue
			}
		}

		priority := s.determinePriority(devResult.devPct)

		token := &models.TokenFound{
			Chain:          models.ChainSolana,
			TokenAddress:   mint,
			FirstSeenTS:    tx.Timestamp,
			CreatorAddress: tx.FeePayer,
			TxHash:         tx.Signature,
			InitialLiquidity: models.InitialLiquidity{
				Pair: "pumpfun",
			},
			Metadata: map[string]string{
				"source": "pumpfun",
			},
		}

		found++
		log.Printf("ChainScannerAgent: 🚀 New token via Helius! Mint: %s creator: %s (dev %.2f%%, priority=%s)\n", 
			mint[:10], tx.FeePayer[:10], devResult.devPct, priority)
		s.emitTokenFoundWithPriority(*token, priority)
	}
	log.Printf("ChainScannerAgent: [helius/pumpfun] %d new tokens found\n", found)
}

// determinePriority sets priority based on dev hold percentage
func (s *ChainScannerAgent) determinePriority(devPct float64) string {
	if devPct >= 2.0 && devPct <= 6.0 {
		return "high" // Ideal dev hold range
	}
	if devPct > 0 && devPct < 2.0 {
		return "medium" // Low but not zero
	}
	if devPct == 0 {
		return "low" // Dev sold immediately
	}
	if devPct > 8.0 {
		return "low" // Dev holds too much
	}
	return "medium"
}

// isMayhemFromLogs returns true if any log message indicates mayhem mode
func isMayhemFromLogs(logs []string) bool {
	for _, msg := range logs {
		lower := strings.ToLower(msg)
		if strings.Contains(lower, "mayhem") ||
			strings.Contains(lower, "is_mayhem_mode: true") ||
			strings.Contains(lower, "ismayhem") {
			return true
		}
	}
	return false
}

// scanProgram is the raw RPC fallback
func (s *ChainScannerAgent) scanProgram(programID, source string, limit int) {
	sigs, err := s.getRecentSignatures(programID, limit)
	if err != nil {
		log.Printf("ChainScannerAgent: Error fetching %s signatures: %v\n", source, err)
		return
	}

	newCount, tokenCount := 0, 0
	for _, sig := range sigs {
		if sig.Err != nil {
			continue
		}
		s.seenSigsMu.Lock()
		seen := s.seenSigs[sig.Signature]
		if !seen {
			s.seenSigs[sig.Signature] = true
			if len(s.seenSigs) > 10000 {
				s.pruneSeen()
			}
		}
		s.seenSigsMu.Unlock()
		if seen {
			continue
		}
		newCount++

		tx, err := s.getTransaction(sig.Signature)
		if err != nil {
			log.Printf("ChainScannerAgent: Error fetching tx: %v\n", err)
			continue
		}
		if tx == nil || tx.Meta.Err != nil {
			continue
		}

		token := s.extractTokenFromTx(tx, sig.Signature, source)
		if token != nil {
			// Dev check
			devResult := s.checkDevWallet(token.TokenAddress, token.CreatorAddress)
			if devResult.skip {
				continue
			}
			priority := s.determinePriority(devResult.devPct)
			
			s.seenMintsMu.Lock()
			mintSeen := s.seenMints[token.TokenAddress]
			if !mintSeen {
				s.seenMints[token.TokenAddress] = true
			}
			s.seenMintsMu.Unlock()
			if !mintSeen {
				tokenCount++
				log.Printf("ChainScannerAgent: 🚀 New token via %s! Mint: %s (dev %.2f%%, priority=%s)\n", 
					source, token.TokenAddress[:10], devResult.devPct, priority)
				s.emitTokenFoundWithPriority(*token, priority)
			}
		}
	}
	log.Printf("ChainScannerAgent: [%s] %d new txs, %d tokens found\n", source, newCount, tokenCount)
}

func (s *ChainScannerAgent) extractTokenFromTx(tx *txResult, txHash, source string) *models.TokenFound {
	accounts := tx.Transaction.Message.AccountKeys
	if len(accounts) == 0 {
		return nil
	}
	logs := tx.Meta.LogMessages

	if source == "pumpfun" {
		// Check for create_v2 instruction and mayhem
		isCreate := false
		isMayhem := false
		for _, msg := range logs {
			if strings.Contains(msg, "Instruction: Create") ||
				strings.Contains(msg, "create_v2") ||
				strings.Contains(msg, "CreateV2") {
				isCreate = true
			}
			lowerMsg := strings.ToLower(msg)
			if strings.Contains(lowerMsg, "mayhem") ||
				strings.Contains(lowerMsg, "is_mayhem_mode: true") {
				isMayhem = true
			}
		}
		if !isCreate {
			return nil
		}
		if isMayhem {
			log.Printf("ChainScannerAgent: Skipping mayhem token from logs: %s\n", txHash[:20])
			return nil
		}

		// Find mint ending in "pump"
		mint := ""
		for _, bal := range tx.Meta.PostTokenBalances {
			if strings.HasSuffix(bal.Mint, "pump") {
				mint = bal.Mint
				break
			}
		}
		if mint == "" {
			for _, acc := range accounts {
				if strings.HasSuffix(acc, "pump") {
					mint = acc
					break
				}
			}
		}
		if mint == "" {
			return nil
		}

		var reserveToken float64
		for _, bal := range tx.Meta.PostTokenBalances {
			if bal.Mint == mint {
				reserveToken = bal.UITokenAmount.UIAmount
				break
			}
		}
		reserveNative := s.estimateSOLReserve(logs)
		return s.buildToken(mint, accounts[0], reserveToken, reserveNative, txHash, source, tx.BlockTime)
	}

	return nil
}

func (s *ChainScannerAgent) buildToken(mint, creator string, reserveToken, reserveNative float64, txHash, source string, blockTime *int64) *models.TokenFound {
	if reserveNative > 0 {
		liquidityUSD := reserveNative * 86.0
		if liquidityUSD < s.config.MinLiquidity {
			log.Printf("ChainScannerAgent: Skipping %s - liquidity $%.2f < min $%.2f\n", mint[:10], liquidityUSD, s.config.MinLiquidity)
			return nil
		}
	}
	ts := time.Now().Unix()
	if blockTime != nil {
		ts = *blockTime
	}
	return &models.TokenFound{
		Chain:          models.ChainSolana,
		TokenAddress:   mint,
		FirstSeenTS:    ts,
		CreatorAddress: creator,
		TxHash:         txHash,
		InitialLiquidity: models.InitialLiquidity{
			Pair:          source,
			ReserveToken:  reserveToken,
			ReserveNative: reserveNative,
		},
		Metadata: map[string]string{"source": source},
	}
}

func (s *ChainScannerAgent) estimateSOLReserve(logs []string) float64 {
	for _, msg := range logs {
		var amount float64
		if n, _ := fmt.Sscanf(msg, "Program log: sol_amount: %f", &amount); n == 1 {
			return amount / 1e9
		}
		if n, _ := fmt.Sscanf(msg, "Program log: virtual_sol_reserves: %f", &amount); n == 1 {
			return amount / 1e9
		}
		if n, _ := fmt.Sscanf(msg, "Program log: real_sol_reserves: %f", &amount); n == 1 {
			return amount / 1e9
		}
	}
	return 0
}

func (s *ChainScannerAgent) pruneSeen() {
	count := 0
	for k := range s.seenSigs {
		delete(s.seenSigs, k)
		count++
		if count >= 5000 {
			break
		}
	}
}

func (s *ChainScannerAgent) scanBaseNewTokens() {
	log.Println("ChainScannerAgent: Scanning Base for new tokens...")
}

// devCheckResult holds results of the dev wallet inspection
type devCheckResult struct {
	skip       bool
	reason     string
	devBalance uint64
	supply     uint64
	devPct     float64
}

// checkDevWallet examines creator/dev holdings & detects dev sells
// Returns devCheckResult.skip=true if we should NOT buy this token
func (s *ChainScannerAgent) checkDevWallet(mint, creator string) devCheckResult {
	if creator == "" {
		return devCheckResult{}
	}

	// Get dev's token account for this mint
	devBalance, decimals, err := s.fetchTokenBalance(creator, mint)
	if err != nil {
		log.Printf("ChainScannerAgent: dev check - couldn't fetch %s balance for %s: %v\n",
			creator[:10], mint[:10], err)
		return devCheckResult{} // fail open on RPC errors
	}

	// Get total supply from mint account
	supply, err := s.fetchMintSupply(mint)
	if err != nil || supply == 0 {
		// Default to PumpFun's standard 1B supply with 6 decimals
		supply = 1_000_000_000 * 1_000_000
	}

	pct := 0.0
	if supply > 0 {
		pct = float64(devBalance) / float64(supply) * 100.0
	}

	// Detect dev sell: bonding curve has activity (mint has trades) but dev holds 0
	if devBalance == 0 {
		log.Printf("ChainScannerAgent: 🚫 Dev SOLD %
