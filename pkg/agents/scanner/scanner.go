package scanner

import (
	"bytes"
	"context"
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
	PumpFunProgram = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
)

// ─── KOL wallets to track ────────────────────────────────────────────────────
// Override via KOL_WALLETS env var (comma-separated "addr:name" pairs).

var defaultKOLs = map[string]string{
	"525LueqAyZJueCoiisfWy6nyh4MTvmF4X9jSqi6efXJT": "JOJI",
	"CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o": "CENTED",
	"Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt": "THEO",
	"AuPp4YTMTyqxYXQnHc5KUc6pUuCSsHQpBJhgnD45yqrf": "DANI",
	"8rvAsDKeAcEjEkiZMug9k8v1y8mW6gQQiMobd89Uy7qR": "CASINO",
	"4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9": "DECU",
	"7bsTkeWcSPG6nzsbXucxV89YUULoSExNJdX2WqfLHwZ4": "BIGWARZ",
	"5B79fMkcFeRTiwm7ehsZsFiKsC7m7n1Bgv9yLxPp9q2X": "BANDIT",
	"FAicXNV5FVqtfbpn4Zccs71XcfGeyxBSGbqLDyDJZjke": "RADIANCE",
	"4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk": "JIJO",
	"5ZuV8eqkvzYFVEKbLvGBdexL2tFv7E5BCd2HZpjqbdg":  "DOKI",
	"B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC": "KADINOX",
	"8MaVa9kdt3NW4Q5HyNAm1X5LbR8PQRVDc1W8NMVK88D5": "DAUMEN",
	"4nwfXw7n98jEQn93VWY7Cuf1jnn1scHXuXCPGVYS9k6T": "FROST",
	"3BLjRcxWGtR7WRshJ3hL25U3RjWr5Ud98wMcczQqk4Ei": "SEBIASTEIN",
}

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

// ─── ChainScannerAgent ────────────────────────────────────────────────────────

type ChainScannerAgent struct {
	config       *config.Config
	tokenChannel chan models.TokenFound
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	httpClient   *http.Client

	kolWallets  map[string]string // address → name
	rpcURL      string
	seenSigs    map[string]bool
	seenSigsMu  sync.Mutex
	seenMints   map[string]bool
	seenMintsMu sync.Mutex
}

func NewChainScannerAgent(cfg *config.Config) *ChainScannerAgent {
	ctx, cancel := context.WithCancel(context.Background())

	kols := loadKOLsFromEnv()
	if len(kols) == 0 {
		kols = defaultKOLs
	}

	return &ChainScannerAgent{
		config:       cfg,
		tokenChannel: make(chan models.TokenFound, 100),
		ctx:          ctx,
		cancel:       cancel,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
		kolWallets:   kols,
		rpcURL:       cfg.SolanaRPCURL,
		seenSigs:     make(map[string]bool),
		seenMints:    make(map[string]bool),
	}
}

func loadKOLsFromEnv() map[string]string {
	envStr := os.Getenv("KOL_WALLETS")
	if envStr == "" {
		return nil
	}
	result := make(map[string]string)
	parts := strings.Split(envStr, ",")
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if idx := strings.Index(p, ":"); idx > 0 {
			result[p[:idx]] = strings.TrimSpace(p[idx+1:])
		} else {
			result[p] = fmt.Sprintf("KOL%d", i+1)
		}
	}
	return result
}

func (s *ChainScannerAgent) Start() {
	log.Printf("ChainScannerAgent: Starting KOL tracker for %d wallets:\n", len(s.kolWallets))
	for addr, name := range s.kolWallets {
		log.Printf("  • %s (%s...)\n", name, addr[:8])
	}

	s.wg.Add(1)
	go s.pollAllKOLsLoop()

	log.Println("ChainScannerAgent: KOL tracker started (polling every 3s)")
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

// ─── Polling loop ─────────────────────────────────────────────────────────────

func (s *ChainScannerAgent) pollAllKOLsLoop() {
	defer s.wg.Done()

	// Stagger startup: launch one goroutine per wallet but space them 300ms apart
	// to avoid hitting RPC rate limits during the initial signature priming
	i := 0
	for addr, name := range s.kolWallets {
		s.wg.Add(1)
		go s.pollKOLWallet(addr, name, time.Duration(i*300)*time.Millisecond)
		i++
	}

	<-s.ctx.Done()
}

func (s *ChainScannerAgent) pollKOLWallet(addr, name string, startDelay time.Duration) {
	defer s.wg.Done()

	// Initial delay to spread out priming across wallets
	if startDelay > 0 {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(startDelay):
		}
	}

	// Poll every 5 seconds — gives us 15 wallets × ~1 req each = ~3 req/sec
	// well within Helius free tier limits
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Initial backfill: mark recent sigs as seen so we don't act on old txs
	if sigs, err := s.getRecentSignatures(addr, 10); err == nil {
		s.seenSigsMu.Lock()
		for _, sig := range sigs {
			s.seenSigs[sig.Signature] = true
		}
		s.seenSigsMu.Unlock()
		log.Printf("ChainScannerAgent: %s — primed with %d recent sigs\n", name, len(sigs))
	} else {
		log.Printf("ChainScannerAgent: %s — prime failed: %v (will retry on first tick)\n", name, err)
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkKOLForNewBuys(addr, name)
		}
	}
}

func (s *ChainScannerAgent) checkKOLForNewBuys(addr, name string) {
	sigs, err := s.getRecentSignatures(addr, 5)
	if err != nil {
		// Don't spam logs — RPC errors are usually transient
		return
	}

	for _, sig := range sigs {
		if sig.Err != nil {
			continue
		}

		s.seenSigsMu.Lock()
		if s.seenSigs[sig.Signature] {
			s.seenSigsMu.Unlock()
			continue
		}
		s.seenSigs[sig.Signature] = true
		if len(s.seenSigs) > 5000 {
			s.pruneSeen()
		}
		s.seenSigsMu.Unlock()

		// Process in background so one slow tx doesn't block others
		go s.processKOLTransaction(sig.Signature, addr, name)
	}
}

func (s *ChainScannerAgent) processKOLTransaction(sig, kolAddr, kolName string) {
	tx, err := s.getTransaction(sig)
	if err != nil {
		log.Printf("ChainScannerAgent: Could not fetch %s tx %s: %v\n", kolName, sig[:16], err)
		return
	}
	if tx == nil || tx.Meta.Err != nil {
		return
	}

	// Check transaction logs for PumpFun + Buy + not Mayhem
	hasPumpFun := false
	isBuy := false
	isSell := false
	isMayhem := false

	for _, msg := range tx.Meta.LogMessages {
		if strings.Contains(msg, PumpFunProgram) {
			hasPumpFun = true
		}
		lower := strings.ToLower(msg)
		if strings.Contains(lower, "instruction: buy") {
			isBuy = true
		}
		if strings.Contains(lower, "instruction: sell") {
			isSell = true
		}
		if strings.Contains(lower, "mayhem") ||
			strings.Contains(lower, "is_mayhem_mode: true") {
			isMayhem = true
		}
	}

	if !hasPumpFun || isSell || !isBuy {
		return
	}

	if isMayhem {
		log.Printf("ChainScannerAgent: %s bought a MAYHEM token — skipping\n", kolName)
		return
	}

	// Find the PumpFun mint
	mint := ""
	for _, bal := range tx.Meta.PostTokenBalances {
		if bal.Owner == kolAddr && strings.HasSuffix(bal.Mint, "pump") {
			mint = bal.Mint
			break
		}
	}
	if mint == "" {
		// Fallback — any mint ending in "pump"
		for _, bal := range tx.Meta.PostTokenBalances {
			if strings.HasSuffix(bal.Mint, "pump") {
				mint = bal.Mint
				break
			}
		}
	}
	if mint == "" {
		// Last resort — scan account keys
		for _, acc := range tx.Transaction.Message.AccountKeys {
			if strings.HasSuffix(acc, "pump") {
				mint = acc
				break
			}
		}
	}
	if mint == "" {
		return
	}

	// Deduplicate by mint — multiple KOLs buying same token = first one wins
	s.seenMintsMu.Lock()
	if s.seenMints[mint] {
		s.seenMintsMu.Unlock()
		log.Printf("ChainScannerAgent: %s bought %s — already bought from another KOL\n", kolName, mint[:10])
		return
	}
	s.seenMints[mint] = true
	s.seenMintsMu.Unlock()

	log.Printf("ChainScannerAgent: 🎯 KOL BUY — %s bought %s | sig=%s\n",
		kolName, mint, sig[:16])

	ts := time.Now().Unix()
	if tx.BlockTime != nil {
		ts = *tx.BlockTime
	}

	token := models.TokenFound{
		Chain:          models.ChainSolana,
		TokenAddress:   mint,
		FirstSeenTS:    ts,
		CreatorAddress: kolAddr,
		TxHash:         sig,
		InitialLiquidity: models.InitialLiquidity{
			Pair: "pumpfun",
		},
		Metadata: map[string]string{
			"source":   "kol_tracker",
			"kol_name": kolName,
			"kol_addr": kolAddr,
		},
	}

	s.emitTokenFound(token)
}

// ─── Shared RPC helpers ───────────────────────────────────────────────────────

func (s *ChainScannerAgent) rpcCall(method string, params []interface{}) (*rpcResponse, error) {
	req := rpcRequest{Jsonrpc: "2.0", ID: 1, Method: method, Params: params}
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(s.ctx, "POST", s.rpcURL, bytes.NewReader(body))
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
	var rr rpcResponse
	if err := json.Unmarshal(respBody, &rr); err != nil {
		return nil, err
	}
	if rr.Error != nil {
		return nil, fmt.Errorf("RPC %s error %d: %s", method, rr.Error.Code, rr.Error.Message)
	}
	return &rr, nil
}

func (s *ChainScannerAgent) getRecentSignatures(address string, limit int) ([]signatureInfo, error) {
	resp, err := s.rpcCall("getSignaturesForAddress", []interface{}{
		address,
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
	// Retry up to 3 times on rate limit (with backoff)
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			// Wait 500ms × attempt before retrying
			select {
			case <-s.ctx.Done():
				return nil, s.ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}

		resp, err := s.rpcCall("getTransaction", []interface{}{
			sig,
			map[string]interface{}{
				"encoding":                       "json",
				"maxSupportedTransactionVersion": 0,
			},
		})
		if err != nil {
			lastErr = err
			// Only retry on rate limit, fail fast on other errors
			if strings.Contains(err.Error(), "-32429") || strings.Contains(err.Error(), "rate limited") {
				continue
			}
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
	return nil, lastErr
}

func (s *ChainScannerAgent) emitTokenFound(token models.TokenFound) {
	select {
	case s.tokenChannel <- token:
		log.Printf("ChainScannerAgent: Token emitted to pipeline — %s\n", token.TokenAddress)
	case <-s.ctx.Done():
		return
	default:
		log.Println("ChainScannerAgent: Warning — token channel full, dropping event")
	}
}

func (s *ChainScannerAgent) pruneSeen() {
	count := 0
	for k := range s.seenSigs {
		delete(s.seenSigs, k)
		count++
		if count >= 2500 {
			break
		}
	}
}
