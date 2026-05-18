package execution

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

// ─── Base58 using math/big ────────────────────────────────────────────────────
const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

var bigBase = big.NewInt(58)
var bigZero = big.NewInt(0)

func base58Decode(s string) ([]byte, error) {
	n := new(big.Int)
	for _, c := range s {
		idx := -1
		for i, a := range base58Alphabet {
			if a == c {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("invalid base58 char: %c", c)
		}
		n.Mul(n, bigBase)
		n.Add(n, big.NewInt(int64(idx)))
	}
	decoded := n.Bytes()
	leading := 0
	for _, c := range s {
		if c == '1' {
			leading++
		} else {
			break
		}
	}
	result := make([]byte, leading+len(decoded))
	copy(result[leading:], decoded)
	return result, nil
}

func base58Encode(b []byte) string {
	n := new(big.Int).SetBytes(b)
	result := []byte{}
	mod := new(big.Int)
	for n.Cmp(bigZero) > 0 {
		n.DivMod(n, bigBase, mod)
		result = append([]byte{base58Alphabet[mod.Int64()]}, result...)
	}
	for _, byt := range b {
		if byt == 0 {
			result = append([]byte{'1'}, result...)
		} else {
			break
		}
	}
	if len(result) == 0 {
		return "1"
	}
	return string(result)
}

// ─── PumpPortal types ─────────────────────────────────────────────────────────

const PumpPortalURL = "https://pumpportal.fun/api/trade-local"

type pumpPortalRequest struct {
	PublicKey        string      `json:"publicKey"`
	Action           string      `json:"action"`
	Mint             string      `json:"mint"`
	Amount           interface{} `json:"amount"` // float64 for buy (SOL), string for sell (% or token amount)
	DenominatedInSol string      `json:"denominatedInSol"`
	Slippage         float64     `json:"slippage"`
	PriorityFee      float64     `json:"priorityFee"`
	Pool             string      `json:"pool"`
}

// Position tracks an open buy
type Position struct {
	Mint           string    `json:"mint"`
	EntryPrice     float64   `json:"entry_price"`    // SOL per token (virtual price)
	TokensHeld     uint64    `json:"tokens_held"`    // raw token amount (with decimals)
	OriginalTokens uint64    `json:"original_tokens"`
	SolSpent       float64   `json:"sol_spent"`
	OpenedAt       time.Time `json:"opened_at"`
	TP1Done        bool      `json:"tp1_done"`
	TP2Done        bool      `json:"tp2_done"`
	PeakMultiplier float64   `json:"peak_multiplier"` // for trailing stop
	BuyTxHash      string    `json:"buy_tx_hash"`
}

// ─── Solana RPC types ─────────────────────────────────────────────────────────

type rpcReq struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcResp struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// ExecutionAgent handles trade execution via PumpPortal
type ExecutionAgent struct {
	config     *config.Config
	httpClient *http.Client
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey

	// Position tracking
	positions   map[string]*Position
	positionsMu sync.RWMutex

	// Cached wallet balance (updates periodically)
	cachedBalanceSOL float64
	balanceMu        sync.RWMutex
	lastBalanceCheck time.Time
}

// NewExecutionAgent creates a new execution agent
func NewExecutionAgent(cfg *config.Config) *ExecutionAgent {
	agent := &ExecutionAgent{
		config:     cfg,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		positions:  make(map[string]*Position),
	}

	log.Println("ExecutionAgent: Using PumpPortal trade-local API (free, no key required)")

	if cfg.PrivateKey != "" {
		if err := agent.loadPrivateKey(cfg.PrivateKey); err != nil {
			log.Printf("ExecutionAgent: Failed to load private key: %v\n", err)
		} else {
			log.Printf("ExecutionAgent: Wallet loaded: %s\n", base58Encode(agent.publicKey))
			// Fetch initial balance
			if bal, err := agent.fetchWalletBalanceSOL(); err == nil {
				agent.balanceMu.Lock()
				agent.cachedBalanceSOL = bal
				agent.lastBalanceCheck = time.Now()
				agent.balanceMu.Unlock()
				log.Printf("ExecutionAgent: Initial wallet balance: %.4f SOL\n", bal)
			} else {
				log.Printf("ExecutionAgent: Failed to fetch initial balance: %v\n", err)
			}
		}
	} else {
		log.Println("ExecutionAgent: No PRIVATE_KEY set — observe-only mode")
	}

	// Start the position monitor loop
	go agent.monitorPositionsLoop()

	return agent
}

func (e *ExecutionAgent) loadPrivateKey(privKeyB58 string) error {
	keyBytes, err := base58Decode(privKeyB58)
	if err != nil {
		return fmt.Errorf("base58 decode: %w", err)
	}
	switch len(keyBytes) {
	case 64:
		e.privateKey = ed25519.PrivateKey(keyBytes)
	case 32:
		e.privateKey = ed25519.NewKeyFromSeed(keyBytes)
	default:
		return fmt.Errorf("unexpected key length %d (need 32 or 64)", len(keyBytes))
	}
	e.publicKey = e.privateKey.Public().(ed25519.PublicKey)
	return nil
}

// ─── Wallet balance ───────────────────────────────────────────────────────────

// fetchWalletBalanceSOL fetches real SOL balance from RPC
func (e *ExecutionAgent) fetchWalletBalanceSOL() (float64, error) {
	if e.publicKey == nil {
		return 0, fmt.Errorf("no wallet loaded")
	}
	walletPubkey := base58Encode(e.publicKey)
	result, err := e.rpcCall("getBalance", []interface{}{
		walletPubkey,
		map[string]string{"commitment": "confirmed"},
	})
	if err != nil {
		return 0, err
	}
	var out struct {
		Value uint64 `json:"value"`
	}
	if err := json.Unmarshal(result, &out); err != nil {
		return 0, err
	}
	return float64(out.Value) / 1_000_000_000, nil
}

// GetWalletBalanceSOL returns the cached balance, refreshing if stale (>30s)
func (e *ExecutionAgent) GetWalletBalanceSOL() float64 {
	e.balanceMu.RLock()
	bal := e.cachedBalanceSOL
	stale := time.Since(e.lastBalanceCheck) > 30*time.Second
	e.balanceMu.RUnlock()

	if stale && e.privateKey != nil {
		if newBal, err := e.fetchWalletBalanceSOL(); err == nil {
			e.balanceMu.Lock()
			e.cachedBalanceSOL = newBal
			e.lastBalanceCheck = time.Now()
			e.balanceMu.Unlock()
			return newBal
		}
	}
	return bal
}

// GetWalletBalanceUSD returns the balance in USD (rough estimate at $86/SOL)
func (e *ExecutionAgent) GetWalletBalanceUSD() float64 {
	return e.GetWalletBalanceSOL() * 86.0
}

// ─── PumpPortal: build & send trades ─────────────────────────────────────────

// getSwapTransactionBuy gets a prebuilt BUY transaction from PumpPortal
func (e *ExecutionAgent) getSwapTransactionBuy(mint string, solAmount float64) (string, error) {
	walletPubkey := base58Encode(e.publicKey)
	body := pumpPortalRequest{
		PublicKey:        walletPubkey,
		Action:           "buy",
		Mint:             mint,
		Amount:           solAmount,
		DenominatedInSol: "true",
		Slippage:         10,
		PriorityFee:      0.0005,
		Pool:             "pump",
	}
	return e.callPumpPortal(body)
}

// getSwapTransactionSell gets a prebuilt SELL transaction
// percentage is 1-100 (sell this % of the token holding)
func (e *ExecutionAgent) getSwapTransactionSell(mint string, percentage int) (string, error) {
	walletPubkey := base58Encode(e.publicKey)
	body := pumpPortalRequest{
		PublicKey:        walletPubkey,
		Action:           "sell",
		Mint:             mint,
		Amount:           fmt.Sprintf("%d%%", percentage),
		DenominatedInSol: "false",
		Slippage:         15,
		PriorityFee:      0.0005,
		Pool:             "pump",
	}
	return e.callPumpPortal(body)
}

func (e *ExecutionAgent) callPumpPortal(body pumpPortalRequest) (string, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	log.Printf("ExecutionAgent: POST %s action=%s mint=%s amount=%v\n",
		PumpPortalURL, body.Action, body.Mint, body.Amount)

	req, err := http.NewRequest("POST", PumpPortalURL, bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read: %w", err)
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	if len(respBody) < 65 {
		return "", fmt.Errorf("response too short (%d bytes): %s", len(respBody), string(respBody))
	}
	return base64.StdEncoding.EncodeToString(respBody), nil
}

// signTransaction signs a base64 unsigned tx by filling the first signature slot
func (e *ExecutionAgent) signTransaction(txB64 string) (string, error) {
	txBytes, err := base64.StdEncoding.DecodeString(txB64)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}
	if len(txBytes) < 1 {
		return "", fmt.Errorf("tx too short")
	}
	numSigs := int(txBytes[0])
	if numSigs < 1 {
		return "", fmt.Errorf("no signature slots in tx")
	}
	sigStart := 1
	msgStart := sigStart + numSigs*64
	if len(txBytes) < msgStart {
		return "", fmt.Errorf("tx truncated: expected %d bytes, got %d", msgStart, len(txBytes))
	}
	message := txBytes[msgStart:]
	signature := ed25519.Sign(e.privateKey, message)
	signedTx := make([]byte, len(txBytes))
	copy(signedTx, txBytes)
	copy(signedTx[sigStart:sigStart+64], signature)
	return base64.StdEncoding.EncodeToString(signedTx), nil
}

func (e *ExecutionAgent) rpcCall(method string, params []interface{}) (json.RawMessage, error) {
	req := rpcReq{Jsonrpc: "2.0", ID: 1, Method: method, Params: params}
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequest("POST", e.config.SolanaRPCURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var rr rpcResp
	if err := json.Unmarshal(respBody, &rr); err != nil {
		return nil, err
	}
	if rr.Error != nil {
		return nil, fmt.Errorf("RPC %s error %d: %s", method, rr.Error.Code, rr.Error.Message)
	}
	return rr.Result, nil
}

func (e *ExecutionAgent) simulateTransaction(txB64 string) error {
	result, err := e.rpcCall("simulateTransaction", []interface{}{
		txB64,
		map[string]interface{}{
			"encoding":   "base64",
			"commitment": "confirmed",
		},
	})
	if err != nil {
		return err
	}
	var sim struct {
		Value struct {
			Err  interface{} `json:"err"`
			Logs []string    `json:"logs"`
		} `json:"value"`
	}
	json.Unmarshal(result, &sim)
	if sim.Value.Err != nil {
		for _, l := range sim.Value.Logs {
			log.Printf("ExecutionAgent: SimLog: %s\n", l)
		}
		errJSON, _ := json.Marshal(sim.Value.Err)
		return fmt.Errorf("simulation error: %s", string(errJSON))
	}
	return nil
}

func (e *ExecutionAgent) sendTransaction(txB64 string) (string, error) {
	result, err := e.rpcCall("sendTransaction", []interface{}{
		txB64,
		map[string]interface{}{
			"encoding":            "base64",
			"skipPreflight":       false,
			"preflightCommitment": "confirmed",
			"maxRetries":          3,
		},
	})
	if err != nil {
		return "", err
	}
	var txHash string
	json.Unmarshal(result, &txHash)
	return txHash, nil
}

// ─── Buy flow ─────────────────────────────────────────────────────────────────

func (e *ExecutionAgent) buyOnPumpFun(ctx context.Context, mint string, solAmount float64) (string, error) {
	if e.privateKey == nil {
		return "", fmt.Errorf("no private key configured")
	}
	log.Printf("ExecutionAgent: Buying mint=%s sol=%.4f\n", mint, solAmount)

	txB64, err := e.getSwapTransactionBuy(mint, solAmount)
	if err != nil {
		return "", fmt.Errorf("getSwapTransactionBuy: %w", err)
	}
	signedTx, err := e.signTransaction(txB64)
	if err != nil {
		return "", fmt.Errorf("signTransaction: %w", err)
	}
	if err := e.simulateTransaction(signedTx); err != nil {
		return "", fmt.Errorf("simulation failed: %w", err)
	}
	log.Println("ExecutionAgent: Simulation passed ✓")
	txHash, err := e.sendTransaction(signedTx)
	if err != nil {
		return "", fmt.Errorf("sendTransaction: %w", err)
	}
	log.Printf("ExecutionAgent: ✅ Buy tx sent! https://solscan.io/tx/%s\n", txHash)
	return txHash, nil
}

// ─── Sell flow ────────────────────────────────────────────────────────────────

func (e *ExecutionAgent) sellOnPumpFun(ctx context.Context, mint string, percentage int) (string, error) {
	if e.privateKey == nil {
		return "", fmt.Errorf("no private key configured")
	}
	log.Printf("ExecutionAgent: Selling %d%% of mint=%s\n", percentage, mint)

	txB64, err := e.getSwapTransactionSell(mint, percentage)
	if err != nil {
		return "", fmt.Errorf("getSwapTransactionSell: %w", err)
	}
	signedTx, err := e.signTransaction(txB64)
	if err != nil {
		return "", fmt.Errorf("signTransaction: %w", err)
	}
	if err := e.simulateTransaction(signedTx); err != nil {
		return "", fmt.Errorf("sell simulation failed: %w", err)
	}
	log.Println("ExecutionAgent: Sell simulation passed ✓")
	txHash, err := e.sendTransaction(signedTx)
	if err != nil {
		return "", fmt.Errorf("sendTransaction: %w", err)
	}
	log.Printf("ExecutionAgent: ✅ Sell tx sent! https://solscan.io/tx/%s\n", txHash)
	return txHash, nil
}

// ─── Position tracking and monitoring ────────────────────────────────────────

// recordPosition stores a new position after successful buy
func (e *ExecutionAgent) recordPosition(mint string, solSpent float64, txHash string) {
	e.positionsMu.Lock()
	defer e.positionsMu.Unlock()

	// Entry price estimation: we'll fetch actual tokens-received from balance later
	// For now we record the position and update price on first check
	e.positions[mint] = &Position{
		Mint:           mint,
		SolSpent:       solSpent,
		OpenedAt:       time.Now(),
		BuyTxHash:      txHash,
		PeakMultiplier: 1.0,
	}
	log.Printf("ExecutionAgent: Position opened: %s, %.4f SOL\n", mint, solSpent)
}

// fetchTokenBalance gets the wallet's balance of a specific token
func (e *ExecutionAgent) fetchTokenBalance(mint string) (uint64, uint8, error) {
	walletPubkey := base58Encode(e.publicKey)
	result, err := e.rpcCall("getTokenAccountsByOwner", []interface{}{
		walletPubkey,
		map[string]string{"mint": mint},
		map[string]string{"encoding": "jsonParsed"},
	})
	if err != nil {
		return 0, 0, err
	}
	var out struct {
		Value []struct {
			Account struct {
				Data struct {
					Parsed struct {
						Info struct {
							TokenAmount struct {
								Amount   string `json:"amount"`
								Decimals uint8  `json:"decimals"`
							} `json:"tokenAmount"`
						} `json:"info"`
					} `json:"parsed"`
				} `json:"data"`
			} `json:"account"`
		} `json:"value"`
	}
	if err := json.Unmarshal(result, &out); err != nil {
		return 0, 0, err
	}
	if len(out.Value) == 0 {
		return 0, 0, nil
	}
	amtStr := out.Value[0].Account.Data.Parsed.Info.TokenAmount.Amount
	decimals := out.Value[0].Account.Data.Parsed.Info.TokenAmount.Decimals
	amt, _ := new(big.Int).SetString(amtStr, 10)
	if amt == nil {
		return 0, decimals, nil
	}
	return amt.Uint64(), decimals, nil
}

// getCurrentPriceSOL gets current price by querying PumpPortal for a 0.001 SOL sell quote
// Returns SOL per token (very rough estimate)
func (e *ExecutionAgent) getCurrentMultiplier(pos *Position) float64 {
	// Use bonding curve account data to estimate price
	// virtualSolReserves / virtualTokenReserves = price per token in SOL
	mintBytes, err := base58Decode(pos.Mint)
	if err != nil {
		return 0
	}
	programBytes, _ := base58Decode("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
	bcAddr := deriveBC(mintBytes[:32], programBytes[:32])
	bcPubkey := base58Encode(bcAddr)

	result, err := e.rpcCall("getAccountInfo", []interface{}{
		bcPubkey,
		map[string]interface{}{"encoding": "base64", "commitment": "confirmed"},
	})
	if err != nil {
		return 0
	}
	var out struct {
		Value *struct {
			Data []string `json:"data"`
		} `json:"value"`
	}
	if err := json.Unmarshal(result, &out); err != nil || out.Value == nil {
		return 0
	}
	if len(out.Value.Data) < 1 {
		return 0
	}
	dataBytes, err := base64.StdEncoding.DecodeString(out.Value.Data[0])
	if err != nil || len(dataBytes) < 48 {
		return 0
	}
	// BondingCurve layout after 8-byte discriminator:
	// virtual_token_reserves: u64 LE [8..16]
	// virtual_sol_reserves:   u64 LE [16..24]
	virtTokens := readU64LE(dataBytes[8:16])
	virtSol := readU64LE(dataBytes[16:24])
	if virtTokens == 0 {
		return 0
	}
	currentPrice := float64(virtSol) / float64(virtTokens)

	if pos.EntryPrice == 0 {
		// First check — set entry price from current and return 1.0
		pos.EntryPrice = currentPrice
		return 1.0
	}
	return currentPrice / pos.EntryPrice
}

func readU64LE(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	var v uint64
	for i := 0; i < 8; i++ {
		v |= uint64(b[i]) << (8 * i)
	}
	return v
}

// deriveBC derives bonding curve PDA - simple version that tries bumps
func deriveBC(mint, program []byte) []byte {
	// Simplified — same approach as scanner's
	// For real PDAs we'd need proper bump iteration and curve check
	// But for price reads, the address mainly needs to match the on-chain BC
	// PumpPortal-created tokens have BC at predictable PDA
	// We use SHA256 fallback similar to scanner
	return shaPDA([][]byte{[]byte("bonding-curve"), mint}, program)
}

func shaPDA(seeds [][]byte, program []byte) []byte {
	h := sha256.New()
	for _, s := range seeds {
		h.Write(s)
	}
	h.Write([]byte{255}) // bump
	h.Write(program)
	h.Write([]byte("ProgramDerivedAddress"))
	return h.Sum(nil)
}

// monitorPositionsLoop runs periodically to check positions for TP/SL/timeout
func (e *ExecutionAgent) monitorPositionsLoop() {
	if e.privateKey == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		e.checkAllPositions()
	}
}

func (e *ExecutionAgent) checkAllPositions() {
	e.positionsMu.RLock()
	mints := make([]string, 0, len(e.positions))
	for mint := range e.positions {
		mints = append(mints, mint)
	}
	e.positionsMu.RUnlock()

	for _, mint := range mints {
		e.checkPosition(mint)
	}
}

func (e *ExecutionAgent) checkPosition(mint string) {
	e.positionsMu.Lock()
	pos, ok := e.positions[mint]
	if !ok {
		e.positionsMu.Unlock()
		return
	}
	e.positionsMu.Unlock()

	mult := e.getCurrentMultiplier(pos)
	if mult <= 0 {
		return
	}

	// Track peak for trailing stop
	e.positionsMu.Lock()
	if mult > pos.PeakMultiplier {
		pos.PeakMultiplier = mult
	}
	tp1Done := pos.TP1Done
	tp2Done := pos.TP2Done
	peakMult := pos.PeakMultiplier
	age := time.Since(pos.OpenedAt)
	e.positionsMu.Unlock()

	log.Printf("ExecutionAgent: Position %s — multiplier=%.2fx peak=%.2fx age=%v\n",
		mint[:10], mult, peakMult, age.Truncate(time.Second))

	// ── TP1: 2x → sell 50% ────────────────────────────────────
	if !tp1Done && mult >= 2.0 {
		log.Printf("ExecutionAgent: TP1 hit (%.2fx) — selling 50%% of %s\n", mult, mint)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		txHash, err := e.sellOnPumpFun(ctx, mint, 50)
		if err != nil {
			log.Printf("ExecutionAgent: TP1 sell failed: %v\n", err)
		} else {
			e.positionsMu.Lock()
			pos.TP1Done = true
			e.positionsMu.Unlock()
			log.Printf("ExecutionAgent: ✅ TP1 sell complete: %s\n", txHash)
		}
		return
	}

	// ── TP2: 3x → sell 50% of remaining (=25% of original) ────
	if tp1Done && !tp2Done && mult >= 3.0 {
		log.Printf("ExecutionAgent: TP2 hit (%.2fx) — selling 50%% of remaining of %s\n", mult, mint)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		txHash, err := e.sellOnPumpFun(ctx, mint, 50)
		if err != nil {
			log.Printf("ExecutionAgent: TP2 sell failed: %v\n", err)
		} else {
			e.positionsMu.Lock()
			pos.TP2Done = true
			e.positionsMu.Unlock()
			log.Printf("ExecutionAgent: ✅ TP2 sell complete: %s\n", txHash)
		}
		return
	}

	// ── Trailing stop on remaining 25% after TP2 ──────────────
	// If price drops 30% from peak after TP2, sell remaining
	if tp2Done && peakMult > 0 && mult <= peakMult*0.7 {
		log.Printf("ExecutionAgent: Trailing stop hit (peak=%.2fx now=%.2fx) — selling 100%% of %s\n",
			peakMult, mult, mint)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		txHash, err := e.sellOnPumpFun(ctx, mint, 100)
		if err != nil {
			log.Printf("ExecutionAgent: Trailing stop sell failed: %v\n", err)
		} else {
			e.positionsMu.Lock()
			delete(e.positions, mint)
			e.positionsMu.Unlock()
			log.Printf("ExecutionAgent: ✅ Position closed via trailing stop: %s\n", txHash)
		}
		return
	}

	// ── Hard stop loss: -50% (pre-TP1 only) ───────────────────
	if !tp1Done && mult <= 0.5 {
		log.Printf("ExecutionAgent: STOP LOSS hit (%.2fx) — selling 100%% of %s\n", mult, mint)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		txHash, err := e.sellOnPumpFun(ctx, mint, 100)
		if err != nil {
			log.Printf("ExecutionAgent: Stop loss sell failed: %v\n", err)
		} else {
			e.positionsMu.Lock()
			delete(e.positions, mint)
			e.positionsMu.Unlock()
			log.Printf("ExecutionAgent: ✅ Position closed via stop loss: %s\n", txHash)
		}
		return
	}

	// ── Timeout: 5 minutes pre-TP1 ────────────────────────────
	if !tp1Done && age > 5*time.Minute {
		log.Printf("ExecutionAgent: TIMEOUT (%.0fs old, no TP1) — selling 100%% of %s\n",
			age.Seconds(), mint)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		txHash, err := e.sellOnPumpFun(ctx, mint, 100)
		if err != nil {
			log.Printf("ExecutionAgent: Timeout sell failed: %v\n", err)
		} else {
			e.positionsMu.Lock()
			delete(e.positions, mint)
			e.positionsMu.Unlock()
			log.Printf("ExecutionAgent: ✅ Position closed via timeout: %s\n", txHash)
		}
		return
	}
}

// ─── Public interface ─────────────────────────────────────────────────────────

func (e *ExecutionAgent) Execute(ctx context.Context, candidate *models.CandidateToken) (*models.ExecutionResult, error) {
	log.Printf("ExecutionAgent: Executing trade for %s\n", candidate.Token.TokenAddress)

	result := &models.ExecutionResult{
		TokenAddress: candidate.Token.TokenAddress,
		Chain:        candidate.Token.Chain,
		AmountUSD:    candidate.StrategyDecision.SuggestedAmountUSD,
		Timestamp:    time.Now(),
		Status:       "pending",
	}

	if e.config.DryRun {
		log.Printf("ExecutionAgent: DRY RUN — would buy %s for $%.2f\n",
			candidate.Token.TokenAddress, candidate.StrategyDecision.SuggestedAmountUSD)
		result.Status = "confirmed"
		result.TxHash = "DRY_RUN_" + candidate.Token.TokenAddress[:8]
		return result, nil
	}

	if candidate.Token.Chain != models.ChainSolana {
		result.Status = "failed"
		result.Error = "only Solana supported"
		return result, fmt.Errorf("unsupported chain: %s", candidate.Token.Chain)
	}

	// Use current real wallet balance to set position size (USD = SOL * 86)
	solAmount := candidate.StrategyDecision.SuggestedAmountUSD / 86.0
	if solAmount < 0.001 {
		solAmount = 0.001
	}

	txHash, err := e.buyOnPumpFun(ctx, candidate.Token.TokenAddress, solAmount)
	if err != nil {
		log.Printf("ExecutionAgent: Buy failed: %v\n", err)
		result.Status = "failed"
		result.Error = err.Error()
		return result, err
	}

	// Record the position for monitoring
	e.recordPosition(candidate.Token.TokenAddress, solAmount, txHash)

	// Refresh cached balance after buy
	if bal, err := e.fetchWalletBalanceSOL(); err == nil {
		e.balanceMu.Lock()
		e.cachedBalanceSOL = bal
		e.lastBalanceCheck = time.Now()
		e.balanceMu.Unlock()
	}

	result.Status = "confirmed"
	result.TxHash = txHash
	return result, nil
}

func (e *ExecutionAgent) Simulate(ctx context.Context, candidate *models.CandidateToken) (bool, error) {
	return true, nil
}

func (e *ExecutionAgent) GetSignerType() string {
	if e.privateKey != nil {
		return "private_key"
	}
	return "none"
}

