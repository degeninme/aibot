package execution

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
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

// ─── PumpPortal swap request ──────────────────────────────────────────────────

type pumpPortalRequest struct {
	PublicKey         string  `json:"publicKey"`
	Action            string  `json:"action"`
	Mint              string  `json:"mint"`
	Amount            float64 `json:"amount"`
	DenominatedInSol  string  `json:"denominatedInSol"`
	Slippage          float64 `json:"slippage"`
	PriorityFee       float64 `json:"priorityFee"`
	Pool              string  `json:"pool"`
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

// ExecutionAgent handles trade execution via QuickNode's PumpFun API
type ExecutionAgent struct {
	config       *config.Config
	httpClient   *http.Client
	privateKey   ed25519.PrivateKey
	publicKey    ed25519.PublicKey
	quicknodeURL string
}

// NewExecutionAgent creates a new execution agent
func NewExecutionAgent(cfg *config.Config) *ExecutionAgent {
	// Read Metis endpoint — accepts either METIS_URL (QuickNode standard) or QUICKNODE_URL
	// Optional — PumpPortal is used by default since it's free and stays current
	metisURL := os.Getenv("METIS_URL")
	if metisURL == "" {
		metisURL = os.Getenv("QUICKNODE_URL")
	}

	agent := &ExecutionAgent{
		config:       cfg,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		quicknodeURL: strings.TrimRight(metisURL, "/"),
	}

	log.Println("ExecutionAgent: Using PumpPortal trade-local API (free, no key required)")

	if cfg.PrivateKey != "" {
		if err := agent.loadPrivateKey(cfg.PrivateKey); err != nil {
			log.Printf("ExecutionAgent: Failed to load private key: %v\n", err)
		} else {
			log.Printf("ExecutionAgent: Wallet loaded: %s\n", base58Encode(agent.publicKey))
		}
	} else {
		log.Println("ExecutionAgent: No PRIVATE_KEY set — observe-only mode")
	}
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

// ─── PumpPortal API: get prebuilt unsigned transaction ───────────────────────

const PumpPortalURL = "https://pumpportal.fun/api/trade-local"

func (e *ExecutionAgent) getSwapTransaction(mint string, lamports uint64) (string, error) {
	walletPubkey := base58Encode(e.publicKey)
	solAmount := float64(lamports) / 1_000_000_000

	body := pumpPortalRequest{
		PublicKey:        walletPubkey,
		Action:           "buy",
		Mint:             mint,
		Amount:           solAmount,
		DenominatedInSol: "true",
		Slippage:         10,      // 10% slippage tolerance
		PriorityFee:      0.0005,  // 0.0005 SOL priority fee
		Pool:             "pump",
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	log.Printf("ExecutionAgent: POST %s wallet=%s mint=%s sol=%.4f\n",
		PumpPortalURL, walletPubkey, mint, solAmount)

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

	// PumpPortal returns RAW transaction bytes (not JSON), encode as base64
	if len(respBody) < 65 {
		return "", fmt.Errorf("response too short (%d bytes): %s", len(respBody), string(respBody))
	}

	return base64.StdEncoding.EncodeToString(respBody), nil
}

// ─── Sign and send the prebuilt transaction ──────────────────────────────────

// signTransaction signs a base64-encoded unsigned (or partially signed) transaction
// QuickNode returns a transaction with an empty signature slot — we fill it in
func (e *ExecutionAgent) signTransaction(txB64 string) (string, error) {
	txBytes, err := base64.StdEncoding.DecodeString(txB64)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}

	if len(txBytes) < 1 {
		return "", fmt.Errorf("tx too short")
	}

	// Solana wire format: [numSignatures (1 byte)][signatures (64 bytes each)][message]
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

	// Place signature in the first slot (fee payer is always first signer)
	signedTx := make([]byte, len(txBytes))
	copy(signedTx, txBytes)
	copy(signedTx[sigStart:sigStart+64], signature)

	return base64.StdEncoding.EncodeToString(signedTx), nil
}

// rpcCall makes a Solana JSON-RPC call (for sending the signed tx)
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

// ─── The actual buy flow ──────────────────────────────────────────────────────

func (e *ExecutionAgent) buyOnPumpFun(ctx context.Context, mint string, solAmount float64) (string, error) {
	if e.privateKey == nil {
		return "", fmt.Errorf("no private key configured")
	}

	lamports := uint64(solAmount * 1_000_000_000)
	log.Printf("ExecutionAgent: Buying mint=%s sol=%.4f lamports=%d\n", mint, solAmount, lamports)

	// 1. Get prebuilt transaction from QuickNode
	txB64, err := e.getSwapTransaction(mint, lamports)
	if err != nil {
		return "", fmt.Errorf("getSwapTransaction: %w", err)
	}
	log.Println("ExecutionAgent: Got prebuilt tx from QuickNode")

	// 2. Sign it with our private key
	signedTx, err := e.signTransaction(txB64)
	if err != nil {
		return "", fmt.Errorf("signTransaction: %w", err)
	}
	log.Println("ExecutionAgent: Transaction signed")

	// 3. Simulate first
	if err := e.simulateTransaction(signedTx); err != nil {
		return "", fmt.Errorf("simulation failed: %w", err)
	}
	log.Println("ExecutionAgent: Simulation passed ✓")

	// 4. Send
	txHash, err := e.sendTransaction(signedTx)
	if err != nil {
		return "", fmt.Errorf("sendTransaction: %w", err)
	}

	log.Printf("ExecutionAgent: ✅ Buy tx sent! https://solscan.io/tx/%s\n", txHash)
	return txHash, nil
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

	// Convert USD → SOL (~$86/SOL estimate)
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

	result.Status = "confirmed"
	result.TxHash = txHash
	return result, nil
}

func (e *ExecutionAgent) Simulate(ctx context.Context, candidate *models.CandidateToken) (bool, error) {
	// Simulation now happens inside buyOnPumpFun — skip duplicate
	return true, nil
}

func (e *ExecutionAgent) GetSignerType() string {
	if e.privateKey != nil {
		return "private_key"
	}
	return "none"
}
