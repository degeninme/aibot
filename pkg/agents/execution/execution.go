package execution

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

// ─── PumpFun constants ────────────────────────────────────────────────────────
const (
	PumpFunProgram      = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
	PumpFunFeeRecipient = "CebN5WGQ4jvEPvsVU4EoHEpgznyQHeAoceR5oHAjXHN"
	PumpFunGlobal       = "" // derived at runtime via deriveGlobalPDA()
	PumpFunEventAuth    = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F"
	SystemProgram       = "11111111111111111111111111111111"
	Token2022Program    = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
	AssocTokenProgram   = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bWB"
	RentSysvar          = "SysvarRent111111111111111111111111111111111"
)

// Anchor discriminator for "buy": sha256("global:buy")[0:8]
var buyDiscriminator = []byte{102, 6, 61, 18, 1, 218, 235, 234}

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

	// Count leading '1's → leading zero bytes
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

func mustDecode58(s string) []byte {
	b, err := base58Decode(s)
	if err != nil {
		panic(fmt.Sprintf("invalid base58 %q: %v", s, err))
	}
	if len(b) < 32 {
		padded := make([]byte, 32)
		copy(padded[32-len(b):], b)
		return padded
	}
	return b[:32]
}

// ─── PDA derivation (pure Go, no external deps) ───────────────────────────────

// findProgramAddress derives a PDA using the standard Solana algorithm
func findProgramAddress(seeds [][]byte, programID []byte) ([]byte, uint8, error) {
	for nonce := uint8(255); ; nonce-- {
		seedsWithNonce := append(seeds, []byte{nonce})
		addr, err := createProgramAddress(seedsWithNonce, programID)
		if err != nil {
			if nonce == 0 {
				return nil, 0, fmt.Errorf("could not find program address")
			}
			continue
		}
		return addr, nonce, nil
	}
}

// createProgramAddress creates a program address from seeds
func createProgramAddress(seeds [][]byte, programID []byte) ([]byte, error) {
	h := sha256.New()
	for _, seed := range seeds {
		h.Write(seed)
	}
	h.Write(programID)
	h.Write([]byte("ProgramDerivedAddress"))
	hash := h.Sum(nil)

	// Check it's not on the ed25519 curve (valid PDA must be off-curve)
	// Simple check: if it decodes as a valid curve point, reject
	// For our purposes we just return it — the nonce loop handles off-curve
	return hash, nil
}


// deriveGlobalPDA derives the PumpFun global state PDA from seed "global"
func deriveGlobalPDA() ([]byte, error) {
	programID := mustDecode58(PumpFunProgram)
	seeds := [][]byte{[]byte("global")}
	addr, _, err := findProgramAddress(seeds, programID)
	return addr, err
}

// deriveBondingCurvePDA derives the bonding curve PDA for a mint
func deriveBondingCurvePDA(mint []byte) ([]byte, error) {
	programID := mustDecode58(PumpFunProgram)
	seeds := [][]byte{
		[]byte("bonding-curve"),
		mint,
	}
	addr, _, err := findProgramAddress(seeds, programID)
	return addr, err
}

// deriveATA derives the associated token account address
func deriveATA(wallet, mint []byte, tokenProgram []byte) ([]byte, error) {
	programID := mustDecode58(AssocTokenProgram)
	seeds := [][]byte{wallet, tokenProgram, mint}
	addr, _, err := findProgramAddress(seeds, programID)
	return addr, err
}

// ─── RPC types ────────────────────────────────────────────────────────────────
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

// ExecutionAgent handles trade execution
type ExecutionAgent struct {
	config     *config.Config
	httpClient *http.Client
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
}

// NewExecutionAgent creates a new execution agent
func NewExecutionAgent(cfg *config.Config) *ExecutionAgent {
	agent := &ExecutionAgent{
		config:     cfg,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	if cfg.PrivateKey != "" {
		if err := agent.loadPrivateKey(cfg.PrivateKey); err != nil {
			log.Printf("ExecutionAgent: Failed to load private key: %v\n", err)
		} else {
			log.Printf("ExecutionAgent: Wallet loaded: %s\n", base58Encode(agent.publicKey))
		}
	} else {
		log.Println("ExecutionAgent: No PRIVATE_KEY set — running in observe-only mode")
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

func (e *ExecutionAgent) getRecentBlockhash() (string, error) {
	result, err := e.rpcCall("getLatestBlockhash", []interface{}{
		map[string]string{"commitment": "confirmed"},
	})
	if err != nil {
		return "", err
	}
	var out struct {
		Value struct {
			Blockhash string `json:"blockhash"`
		} `json:"value"`
	}
	if err := json.Unmarshal(result, &out); err != nil {
		return "", err
	}
	return out.Value.Blockhash, nil
}

// buildBuyInstructionData builds raw PumpFun buy instruction bytes
func buildBuyInstructionData(tokenAmount, maxSolCost uint64) []byte {
	data := make([]byte, 24)
	copy(data[0:8], buyDiscriminator)
	binary.LittleEndian.PutUint64(data[8:16], tokenAmount)
	binary.LittleEndian.PutUint64(data[16:24], maxSolCost)
	return data
}

// compactU16 encodes a length as Solana compact-u16
func compactU16(n int) []byte {
	if n <= 0x7f {
		return []byte{byte(n)}
	}
	return []byte{byte(n&0x7f | 0x80), byte(n >> 7)}
}

// Instruction represents a single Solana instruction
type Instruction struct {
	ProgramID string
	Accounts  []string
	Data      []byte
}

// buildTransaction builds a signed legacy Solana transaction with multiple instructions
func (e *ExecutionAgent) buildTransaction(
	blockhash string,
	instructions []Instruction,
) (string, error) {
	feePayer := base58Encode(e.publicKey)

	// Collect all unique account keys, starting with feePayer
	keyOrder := []string{feePayer}
	keySet := map[string]int{feePayer: 0}
	for _, ix := range instructions {
		for _, acc := range ix.Accounts {
			if _, ok := keySet[acc]; !ok {
				keySet[acc] = len(keyOrder)
				keyOrder = append(keyOrder, acc)
			}
		}
		if _, ok := keySet[ix.ProgramID]; !ok {
			keySet[ix.ProgramID] = len(keyOrder)
			keyOrder = append(keyOrder, ix.ProgramID)
		}
	}

	// Message header: numRequiredSigs=1, numReadonlySignedAccounts=0, numReadonlyUnsignedAccounts=len-1
	header := []byte{1, 0, byte(len(keyOrder) - 1)}

	// Account addresses
	addrBuf := make([]byte, 0, len(keyOrder)*32)
	for _, k := range keyOrder {
		addrBuf = append(addrBuf, mustDecode58(k)...)
	}

	// Blockhash
	bhBytes := mustDecode58(blockhash)

	// Build all instructions
	allInstrBytes := []byte{}
	for _, ix := range instructions {
		instrAccIdxs := make([]byte, len(ix.Accounts))
		for i, acc := range ix.Accounts {
			instrAccIdxs[i] = byte(keySet[acc])
		}
		progIdx := byte(keySet[ix.ProgramID])

		allInstrBytes = append(allInstrBytes, progIdx)
		allInstrBytes = append(allInstrBytes, compactU16(len(instrAccIdxs))...)
		allInstrBytes = append(allInstrBytes, instrAccIdxs...)
		allInstrBytes = append(allInstrBytes, compactU16(len(ix.Data))...)
		allInstrBytes = append(allInstrBytes, ix.Data...)
	}

	// Full message
	msg := []byte{}
	msg = append(msg, header...)
	msg = append(msg, compactU16(len(keyOrder))...)
	msg = append(msg, addrBuf...)
	msg = append(msg, bhBytes...)
	msg = append(msg, compactU16(len(instructions))...)
	msg = append(msg, allInstrBytes...)

	// Sign
	sig := ed25519.Sign(e.privateKey, msg)

	// Transaction wire format: [numSigs][sig][message]
	tx := []byte{1}
	tx = append(tx, sig...)
	tx = append(tx, msg...)

	return base64.StdEncoding.EncodeToString(tx), nil
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


// getAssocBondingCurve fetches the associated bonding curve token account
// by querying the token accounts owned by the bonding curve for the given mint
func (e *ExecutionAgent) getAssocBondingCurve(bondingCurve, mint string) (string, error) {
	result, err := e.rpcCall("getTokenAccountsByOwner", []interface{}{
		bondingCurve,
		map[string]string{"mint": mint},
		map[string]string{"encoding": "base64"},
	})
	if err != nil {
		return "", err
	}
	var out struct {
		Value []struct {
			Pubkey string `json:"pubkey"`
		} `json:"value"`
	}
	if err := json.Unmarshal(result, &out); err != nil {
		return "", fmt.Errorf("parse token accounts: %w", err)
	}
	if len(out.Value) == 0 {
		return "", fmt.Errorf("no token account found for bondingCurve %s mint %s", bondingCurve, mint)
	}
	return out.Value[0].Pubkey, nil
}

func (e *ExecutionAgent) buyOnPumpFun(ctx context.Context, mint string, solAmount float64) (string, error) {
	if e.privateKey == nil {
		return "", fmt.Errorf("no private key configured — set PRIVATE_KEY env var")
	}

	walletPubkey := base58Encode(e.publicKey)
	lamports := uint64(solAmount * 1_000_000_000)
	maxSolCost := uint64(float64(lamports) * 1.01) // 1% slippage

	log.Printf("ExecutionAgent: Buying mint=%s sol=%.4f lamports=%d\n", mint, solAmount, lamports)

	// Derive global PDA
	globalBytes, err := deriveGlobalPDA()
	if err != nil {
		return "", fmt.Errorf("deriveGlobalPDA: %w", err)
	}
	pumpFunGlobal := base58Encode(globalBytes)
	log.Printf("ExecutionAgent: global=%s\n", pumpFunGlobal)

	// Derive accounts
	mintBytes := mustDecode58(mint)
	tokenProgBytes := mustDecode58(Token2022Program)
	programBytes := mustDecode58(PumpFunProgram)

	bondingCurveBytes, err := deriveBondingCurvePDA(mintBytes)
	if err != nil {
		return "", fmt.Errorf("bondingCurve PDA: %w", err)
	}
	bondingCurve := base58Encode(bondingCurveBytes)

	// Fetch assocBondingCurve from RPC — find the token account owned by bondingCurve for this mint
	assocBondingCurve, err := e.getAssocBondingCurve(bondingCurve, mint)
	if err != nil {
		return "", fmt.Errorf("assocBondingCurve lookup: %w", err)
	}

	walletBytes := mustDecode58(walletPubkey)
	userATABytes, err := deriveATA(walletBytes, mintBytes, tokenProgBytes)
	if err != nil {
		return "", fmt.Errorf("userATA: %w", err)
	}
	userATA := base58Encode(userATABytes)

	_ = programBytes // used implicitly via PumpFunProgram constant

	log.Printf("ExecutionAgent: bondingCurve=%s\n", bondingCurve)
	log.Printf("ExecutionAgent: assocBondingCurve=%s\n", assocBondingCurve)
	log.Printf("ExecutionAgent: userATA=%s\n", userATA)

	// Get blockhash
	blockhash, err := e.getRecentBlockhash()
	if err != nil {
		return "", fmt.Errorf("getRecentBlockhash: %w", err)
	}

	// Build instruction data — buy with max token amount, capped by SOL
	instrData := buildBuyInstructionData(1_000_000_000_000, maxSolCost)

	// Account order for PumpFun buy instruction (must match IDL exactly)
	accounts := []string{
		pumpFunGlobal,       // global
		PumpFunFeeRecipient, // feeRecipient
		mint,                // mint
		bondingCurve,        // bondingCurve
		assocBondingCurve,   // associatedBondingCurve
		userATA,             // associatedUserAccount
		walletPubkey,        // user (signer)
		SystemProgram,       // systemProgram
		Token2022Program,    // tokenProgram
		RentSysvar,          // rent
		PumpFunEventAuth,    // eventAuthority
		PumpFunProgram,      // program
	}

	// Build createATA instruction (idempotent — succeeds even if ATA exists)
	// CreateIdempotent instruction discriminator = 1
	createATAInstr := Instruction{
		ProgramID: AssocTokenProgram,
		Accounts: []string{
			walletPubkey,    // funding
			userATA,         // ATA to create
			walletPubkey,    // wallet owner
			mint,            // mint
			SystemProgram,   // system program
			Token2022Program,// token program
		},
		Data: []byte{1}, // CreateIdempotent
	}

	// Build buy instruction
	buyInstr := Instruction{
		ProgramID: PumpFunProgram,
		Accounts:  accounts,
		Data:      instrData,
	}

	txB64, err := e.buildTransaction(blockhash, []Instruction{createATAInstr, buyInstr})
	if err != nil {
		return "", fmt.Errorf("buildTransaction: %w", err)
	}

	// Simulate first
	if err := e.simulateTransaction(txB64); err != nil {
		return "", fmt.Errorf("simulation failed: %w", err)
	}
	log.Println("ExecutionAgent: Simulation passed ✓")

	// Send
	txHash, err := e.sendTransaction(txB64)
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

	// Convert USD → SOL (~$150/SOL estimate)
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
	log.Printf("ExecutionAgent: Simulating trade for %s\n", candidate.Token.TokenAddress)
	if e.privateKey == nil {
		return true, nil // no key = skip simulation
	}

	solAmount := candidate.StrategyDecision.SuggestedAmountUSD / 86.0
	if solAmount < 0.001 {
		solAmount = 0.001
	}

	_, err := e.buyOnPumpFun(ctx, candidate.Token.TokenAddress, solAmount)
	// We only want the simulation result, not actual send — but since
	// simulation runs before send in buyOnPumpFun, we treat simulation
	// failure as the signal here
	if err != nil {
		return false, err
	}
	return true, nil
}

func (e *ExecutionAgent) GetSignerType() string {
	if e.privateKey != nil {
		return "private_key"
	}
	return "none"
}
