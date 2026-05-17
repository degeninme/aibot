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
	PumpFunProgram  = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
	PumpSwapProgram = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
)

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
	// Debug: count how many txs we've fully inspected
	debugCount int
}

func NewChainScannerAgent(cfg *config.Config) *ChainScannerAgent {
	ctx, cancel := context.WithCancel(context.Background())
	return &ChainScannerAgent{
		config:       cfg,
		tokenChannel: make(chan models.TokenFound, 100),
		ctx:          ctx,
		cancel:       cancel,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
		seenSigs:     make(map[string]bool),
		seenMints:    make(map[string]bool),
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
			s.scanSolanaNewTokens()
		}
	}
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
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
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

func (s *ChainScannerAgent) scanSolanaNewTokens() {
	log.Println("ChainScannerAgent: Scanning Solana for new tokens...")
	s.scanProgram(PumpFunProgram, "pumpfun", 15)
	s.scanProgram(PumpSwapProgram, "pumpswap", 5)
}

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
			if len(s.seenSigs) > 5000 {
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

		// ── DEBUG: Print first 3 unseen transactions in full detail ──────────
		s.seenSigsMu.Lock()
		debugIdx := s.debugCount
		if s.debugCount < 3 {
			s.debugCount++
		}
		s.seenSigsMu.Unlock()

		if debugIdx < 3 {
			log.Printf("=== DEBUG TX [%s] sig=%s ===", source, sig.Signature[:20])
			log.Printf("  Accounts (%d): %v", len(tx.Transaction.Message.AccountKeys), tx.Transaction.Message.AccountKeys)
			log.Printf("  PreTokenBalances (%d):", len(tx.Meta.PreTokenBalances))
			for i, b := range tx.Meta.PreTokenBalances {
				log.Printf("    [%d] mint=%s owner=%s amount=%v", i, b.Mint, b.Owner, b.UITokenAmount.UIAmount)
			}
			log.Printf("  PostTokenBalances (%d):", len(tx.Meta.PostTokenBalances))
			for i, b := range tx.Meta.PostTokenBalances {
				log.Printf("    [%d] mint=%s owner=%s amount=%v", i, b.Mint, b.Owner, b.UITokenAmount.UIAmount)
			}
			log.Printf("  LogMessages (%d):", len(tx.Meta.LogMessages))
			for i, m := range tx.Meta.LogMessages {
				log.Printf("    [%d] %s", i, m)
			}
			log.Printf("=== END DEBUG TX ===")
		}
		// ── END DEBUG ──────────────────────────────────────────────────────────

		token := s.extractTokenFromTx(tx, sig.Signature, source)
		if token != nil {
			s.seenMintsMu.Lock()
			mintSeen := s.seenMints[token.TokenAddress]
			if !mintSeen {
				s.seenMints[token.TokenAddress] = true
			}
			s.seenMintsMu.Unlock()
			if !mintSeen {
				tokenCount++
				log.Printf("ChainScannerAgent: 🚀 New token via %s! Mint: %s\n", source, token.TokenAddress)
				s.emitTokenFound(*token)
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
		isCreate := false
		for _, msg := range logs {
			if strings.Contains(msg, "Create") {
				isCreate = true
				break
			}
		}
		if !isCreate {
			return nil
		}

		if len(tx.Meta.PostTokenBalances) == 0 {
			for _, acc := range accounts {
				if strings.HasSuffix(acc, "pump") {
					return s.buildToken(acc, accounts[0], 0, 0, txHash, source, tx.BlockTime)
				}
			}
			return nil
		}

		mint := tx.Meta.PostTokenBalances[0].Mint
		if !strings.HasSuffix(mint, "pump") {
			for _, bal := range tx.Meta.PostTokenBalances {
				if strings.HasSuffix(bal.Mint, "pump") {
					mint = bal.Mint
					break
				}
			}
		}
		if mint == "" || !strings.HasSuffix(mint, "pump") {
			return nil
		}

		reserveToken := tx.Meta.PostTokenBalances[0].UITokenAmount.UIAmount
		reserveNative := s.estimateSOLReserve(logs)
		return s.buildToken(mint, accounts[0], reserveToken, reserveNative, txHash, source, tx.BlockTime)
	}

	if source == "pumpswap" {
		isPoolCreate := false
		for _, msg := range logs {
			if strings.Contains(msg, "create_pool") ||
				strings.Contains(msg, "CreatePool") ||
				strings.Contains(msg, "Initialize") {
				isPoolCreate = true
				break
			}
		}
		if !isPoolCreate {
			return nil
		}
		const WSOL = "So11111111111111111111111111111111111111112"
		for _, bal := range tx.Meta.PostTokenBalances {
			if bal.Mint != "" && bal.Mint != WSOL {
				reserveNative := s.estimateSOLReserve(logs)
				creator := ""
				if len(accounts) > 0 {
					creator = accounts[0]
				}
				return s.buildToken(bal.Mint, creator, bal.UITokenAmount.UIAmount, reserveNative, txHash, source, tx.BlockTime)
			}
		}
	}
	return nil
}

func (s *ChainScannerAgent) buildToken(mint, creator string, reserveToken, reserveNative float64, txHash, source string, blockTime *int64) *models.TokenFound {
	if reserveNative > 0 {
		liquidityUSD := reserveNative * 150.0
		if liquidityUSD < s.config.MinLiquidity {
			log.Printf("ChainScannerAgent: Skipping %s - liquidity $%.2f < min $%.2f\n", mint, liquidityUSD, s.config.MinLiquidity)
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
		if count >= 2500 {
			break
		}
	}
}

func (s *ChainScannerAgent) scanBaseNewTokens() {
	log.Println("ChainScannerAgent: Scanning Base for new tokens...")
}

func (s *ChainScannerAgent) emitTokenFound(token models.TokenFound) {
	select {
	case s.tokenChannel <- token:
		log.Printf("ChainScannerAgent: Token emitted - %s\n", token.TokenAddress)
	case <-s.ctx.Done():
		return
	default:
		log.Println("ChainScannerAgent: Warning - token channel full, dropping event")
	}
}
