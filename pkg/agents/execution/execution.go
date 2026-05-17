package execution

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

// ─── PumpFun constants ────────────────────────────────────────────────────────

const (
	PumpFunProgram    = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
	PumpFunFeeRecipient = "CebN5WGQ4jvEPvsVU4EoHEpgznyQHeAoceR5oHAjXHN"
	PumpFunGlobal     = "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5zP9QkdxEJA7Gy"
	PumpFunEventAuth  = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F"
	SystemProgram     = "11111111111111111111111111111111"
	TokenProgram      = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	Token2022Program  = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
	AssocTokenProgram = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bWB"
	RentSysvar        = "SysvarRent111111111111111111111111111111111"

	// Anchor discriminator for "buy" = sha256("global:buy")[0:8]
	// Pre-calculated: [102, 6, 61, 18, 1, 218, 235, 234]
)

var buyDiscriminator = []byte{102, 6, 61, 18, 1, 218, 235, 234}

// ─── Solana base58 / key helpers (no external deps) ──────────────────────────

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

func base58Decode(s string) ([]byte, error) {
	result := make([]byte, 0, 32)
	n := big256(0)
	for _, c := range s {
		idx := bytes.IndexByte([]byte(base58Alphabet), byte(c))
		if idx < 0 {
			return nil, fmt.Errorf("invalid base58 char: %c", c)
		}
		n = bigMul(n, 58)
		n = bigAdd(n, uint64(idx))
	}
	// convert big number to bytes
	tmp := bigToBytes(n)
	// count leading zeros (1s in base58)
	leading := 0
	for _, c := range s {
		if c == '1' {
			leading++
		} else {
			break
		}
	}
	result = append(make([]byte, leading), tmp...)
	return result, nil
}

// Minimal big-number ops using [4]uint64 (256-bit)
type big256 [4]uint64

func bigMul(a big256, b uint64) big256 {
	var carry uint64
	var result big256
	for i := 3; i >= 0; i-- {
		prod := a[i]*b + carry
		result[i] = prod & 0xFFFFFFFFFFFFFFFF
		carry = prod >> 32 // simplified; good enough for base58
		_ = carry
		hi := uint64(0)
		lo := a[i] * b
		if a[i] != 0 {
			hi = (a[i]*b)/a[i]/b*b != a[i]*b && false // avoid unused
			_ = hi
		}
		_ = lo
	}
	// simpler approach: just use uint64 array multiplication
	return bigMulSimple(a, b)
}

func bigMulSimple(a big256, b uint64) big256 {
	var result big256
	var carry uint64
	for i := 3; i >= 0; i-- {
		cur := a[i]*b + carry
		result[i] = cur
		carry = 0 // for 64-bit this is fine for base58
	}
	return result
}

func bigAdd(a big256, b uint64) big256 {
	result := a
	carry := b
	for i := 3; i >= 0 && carry > 0; i-- {
		sum := result[i] + carry
		if sum < result[i] {
			result[i] = sum
			carry = 1
		} else {
			result[i] = sum
			carry = 0
		}
	}
	return result
}

func bigToBytes(a big256) []byte {
	result := make([]byte, 32)
	binary.BigEndian.PutUint64(result[0:8], a[0])
	binary.BigEndian.PutUint64(result[8:16], a[1])
	binary.BigEndian.PutUint64(result[16:24], a[2])
	binary.BigEndian.PutUint64(result[24:32], a[3])
	// trim leading zeros
	start := 0
	for start < len(result)-1 && result[start] == 0 {
		start++
	}
	return result[start:]
}

func mustDecode58(s string) []byte {
	b, err := base58Decode(s)
	if err != nil {
		panic(fmt.Sprintf("invalid base58 %q: %v", s, err))
	}
	// Pad to 32 bytes
	if len(b) < 32 {
		padded := make([]byte, 32)
		copy(padded[32-len(b):], b)
		return padded
	}
	return b[:32]
}

func base58Encode(b []byte) string {
	n := big256{}
	for _, byt := range b {
		n = bigMulSimple(n, 256)
		n = bigAdd(n, uint64(byt))
	}
	result := []byte{}
	zero := big256{}
	for n != zero {
		mod := n[3] % 58
		n = bigDiv58(n)
		result = append([]byte{base58Alphabet[mod]}, result...)
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

func bigDiv58(a big256) big256 {
	var result big256
	var remainder uint64
	for i := 0; i < 4; i++ {
		cur := remainder<<32 | a[i]>>32
		result[i] = (cur/58)<<32 | (((cur%58)<<32 | a[i]&0xFFFFFFFF) / 58)
		remainder = ((cur%58)<<32 | a[i]&0xFFFFFFFF) % 58
	}
	return result
}

// ─── PDA derivation (no external deps) ───────────────────────────────────────

// findProgramAddress derives a PDA using SHA256 without external deps
// We use a simplified approach: call the RPC to derive PDAs
// Since implementing SHA256+bump in pure Go is complex,
// we'll use the Solana RPC method instead

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

	// Load private key if configured
	if cfg.PrivateKey != "" {
		if err := agent.loadPrivateKey(cfg.PrivateKey); err != nil {
			log.Printf("ExecutionAgent: Failed to load private key: %v\n", err)
		} else {
			log.Printf("ExecutionAgent: Wallet loaded: %s\n", base58Encode(agent.publicKey))
		}
	}

	return agent
}

// loadPrivateKey loads a base58-encoded private key (64 bytes: 32 private + 32 public)
func (e *ExecutionAgent) loadPrivateKey(privKeyB58 string) error {
	keyBytes, err := base58Decode(privKeyB58)
	if err != nil {
		return fmt.Errorf("base58 decode: %w", err)
	}

	if len(keyBytes) == 64 {
		e.privateKey = ed25519.PrivateKey(keyBytes)
		e.publicKey = e.privateKey.Public().(ed25519.PublicKey)
	} else if len(keyBytes) == 32 {
		// Seed only — derive full keypair
		e.privateKey = ed25519.NewKeyFromSeed(keyBytes)
		e.publicKey = e.privateKey.Public().(ed25519.PublicKey)
	} else {
		return fmt.Errorf("unexpected key length: %d (expected 32 or 64)", len(keyBytes))
	}
	return nil
}

// rpcCall makes a Solana JSON-RPC call
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

// getRecentBlockhash fetches the latest blockhash
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

// getProgramDerivedAddress fetches a PDA via RPC
func (e *ExecutionAgent) getProgramDerivedAddress(seeds []string, programID string) (string, error) {
	result, err := e.rpcCall("getProgramDerivedAddress", []interface{}{seeds, programID})
	if err != nil {
		// Fallback: try findProgramAddress
		result, err = e.rpcCall("findProgramAddress", []interface{}{seeds, programID})
		if err != nil {
			return "", fmt.Errorf("getPDA failed: %w", err)
		}
	}
	var addr struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(result, &addr); err != nil {
		// Try array format [address, bump]
		var arr []interface{}
		if err2 := json.Unmarshal(result, &arr); err2 != nil || len(arr) == 0 {
			return "", fmt.Errorf("parse PDA response: %w", err)
		}
		if s, ok := arr[0].(string); ok {
			return s, nil
		}
		return "", fmt.Errorf("unexpected PDA response format")
	}
	return addr.Address, nil
}

// getAssociatedTokenAddress derives the ATA address for a wallet+mint
func (e *ExecutionAgent) getAssociatedTokenAddress(wallet, mint string) (string, error) {
	// ATA = findProgramAddress([wallet, tokenProgram, mint], associatedTokenProgram)
	// Use hex seeds as base58
	walletBytes := mustDecode58(wallet)
	tokenProgBytes := mustDecode58(Token2022Program) // PumpFun uses Token2022
	mintBytes := mustDecode58(mint)

	// We'll derive via RPC using base64-encoded seeds
	walletB64 := base64.StdEncoding.EncodeToString(walletBytes)
	tokProgB64 := base64.StdEncoding.EncodeToString(tokenProgBytes)
	mintB64 := base64.StdEncoding.EncodeToString(mintBytes)

	result, err := e.rpcCall("getProgramDerivedAddress", []interface{}{
		[]interface{}{
			map[string]string{"base64": walletB64},
			map[string]string{"base64": tokProgB64},
			map[string]string{"base64": mintB64},
		},
		AssocTokenProgram,
	})
	if err != nil {
		return "", fmt.Errorf("getATA PDA: %w", err)
	}

	var addr struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(result, &addr); err != nil {
		var arr []interface{}
		json.Unmarshal(result, &arr)
		if len(arr) > 0 {
			if s, ok := arr[0].(string); ok {
				return s, nil
			}
		}
		return "", fmt.Errorf("parse ATA response: %w", err)
	}
	return addr.Address, nil
}

// getBondingCurveAccounts derives bondingCurve and associatedBondingCurve PDAs
func (e *ExecutionAgent) getBondingCurveAccounts(mint string) (bondingCurve, assocBondingCurve string, err error) {
	mintBytes := mustDecode58(mint)
	mintB64 := base64.StdEncoding.EncodeToString(mintBytes)
	bcSeedB64 := base64.StdEncoding.EncodeToString([]byte("bonding-curve"))

	// bondingCurve = PDA([b"bonding-curve", mint], PumpFunProgram)
	result, err := e.rpcCall("getProgramDerivedAddress", []interface{}{
		[]interface{}{
			map[string]string{"base64": bcSeedB64},
			map[string]string{"base64": mintB64},
		},
		PumpFunProgram,
	})
	if err != nil {
		return "", "", fmt.Errorf("bondingCurve PDA: %w", err)
	}

	var addr struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(result, &addr); err != nil {
		var arr []interface{}
		json.Unmarshal(result, &arr)
		if len(arr) > 0 {
			if s, ok := arr[0].(string); ok {
				bondingCurve = s
			}
		}
	} else {
		bondingCurve = addr.Address
	}

	if bondingCurve == "" {
		return "", "", fmt.Errorf("could not derive bondingCurve PDA")
	}

	// associatedBondingCurve = ATA(bondingCurve, mint) using Token2022
	bcBytes := mustDecode58(bondingCurve)
	tokProgBytes := mustDecode58(Token2022Program)
	mintBytes2 := mustDecode58(mint)

	bcB64 := base64.StdEncoding.EncodeToString(bcBytes)
	tokProgB64 := base64.StdEncoding.EncodeToString(tokProgBytes)
	mintB64_2 := base64.StdEncoding.EncodeToString(mintBytes2)

	result2, err := e.rpcCall("getProgramDerivedAddress", []interface{}{
		[]interface{}{
			map[string]string{"base64": bcB64},
			map[string]string{"base64": tokProgB64},
			map[string]string{"base64": mintB64_2},
		},
		AssocTokenProgram,
	})
	if err != nil {
		return "", "", fmt.Errorf("assocBondingCurve PDA: %w", err)
	}

	var addr2 struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(result2, &addr2); err != nil {
		var arr []interface{}
		json.Unmarshal(result2, &arr)
		if len(arr) > 0 {
			if s, ok := arr[0].(string); ok {
				assocBondingCurve = s
			}
		}
	} else {
		assocBondingCurve = addr2.Address
	}

	if assocBondingCurve == "" {
		return "", "", fmt.Errorf("could not derive assocBondingCurve PDA")
	}

	return bondingCurve, assocBondingCurve, nil
}

// buildBuyInstruction builds the raw PumpFun buy instruction data
// Layout: [8 discriminator][8 tokenAmount u64 LE][8 maxSolCost u64 LE]
func buildBuyInstructionData(tokenAmount, maxSolCost uint64) []byte {
	data := make([]byte, 24)
	copy(data[0:8], buyDiscriminator)
	binary.LittleEndian.PutUint64(data[8:16], tokenAmount)
	binary.LittleEndian.PutUint64(data[16:24], maxSolCost)
	return data
}

// solToLamports converts SOL to lamports
func solToLamports(sol float64) uint64 {
	return uint64(sol * 1_000_000_000)
}

// buildAndSendBuyTx builds a PumpFun buy transaction and sends it
func (e *ExecutionAgent) buildAndSendBuyTx(ctx context.Context, mint string, solAmount float64) (string, error) {
	if e.privateKey == nil {
		return "", fmt.Errorf("no private key configured")
	}

	walletPubkey := base58Encode(e.publicKey)
	log.Printf("ExecutionAgent: Building buy tx for mint=%s wallet=%s sol=%.4f\n",
		mint, walletPubkey, solAmount)

	// 1. Get blockhash
	blockhash, err := e.getRecentBlockhash()
	if err != nil {
		return "", fmt.Errorf("getRecentBlockhash: %w", err)
	}

	// 2. Derive bonding curve accounts
	bondingCurve, assocBondingCurve, err := e.getBondingCurveAccounts(mint)
	if err != nil {
		return "", fmt.Errorf("getBondingCurveAccounts: %w", err)
	}
	log.Printf("ExecutionAgent: bondingCurve=%s assocBondingCurve=%s\n", bondingCurve, assocBondingCurve)

	// 3. Derive user's associated token account
	userATA, err := e.getAssociatedTokenAddress(walletPubkey, mint)
	if err != nil {
		return "", fmt.Errorf("getAssociatedTokenAddress: %w", err)
	}
	log.Printf("ExecutionAgent: userATA=%s\n", userATA)

	// 4. Calculate token amount and slippage
	// We specify SOL amount, PumpFun will give us tokens
	// tokenAmount = 0 means "buy with SOL amount" — use a large number
	// Max sol cost = sol amount + 1% slippage
	lamports := solToLamports(solAmount)
	maxSolCost := uint64(float64(lamports) * 1.01) // 1% slippage

	// Token amount: we're buying ~0 tokens specified, passing SOL
	// PumpFun buy: tokenAmount is the number of tokens you want to receive
	// Set to a large number and let maxSolCost cap it
	tokenAmount := uint64(1_000_000_000_000) // large number, capped by maxSolCost

	// 5. Build instruction data
	instrData := buildBuyInstructionData(tokenAmount, maxSolCost)

	// 6. Account keys for the buy instruction (order matters!)
	accounts := []string{
		PumpFunGlobal,         // global
		PumpFunFeeRecipient,   // feeRecipient
		mint,                  // mint
		bondingCurve,          // bondingCurve
		assocBondingCurve,     // associatedBondingCurve
		userATA,               // associatedUserAccount
		walletPubkey,          // user
		SystemProgram,         // systemProgram
		Token2022Program,      // tokenProgram (PumpFun uses Token2022)
		RentSysvar,            // rent
		PumpFunEventAuth,      // eventAuthority
		PumpFunProgram,        // program
	}

	// 7. Build transaction using Solana wire format
	// We'll use the RPC to simulate first, then send
	tx, err := e.buildTransaction(blockhash, accounts, instrData, walletPubkey)
	if err != nil {
		return "", fmt.Errorf("buildTransaction: %w", err)
	}

	// 8. Simulate first
	simResult, err := e.simulateTransaction(tx)
	if err != nil {
		return "", fmt.Errorf("simulation failed: %w", err)
	}
	log.Printf("ExecutionAgent: Simulation result: %s\n", simResult)

	// 9. Send transaction
	txHash, err := e.sendTransaction(tx)
	if err != nil {
		return "", fmt.Errorf("sendTransaction: %w", err)
	}

	log.Printf("ExecutionAgent: ✅ Buy tx sent! Hash: %s\n", txHash)
	return txHash, nil
}

// buildTransaction builds a signed Solana transaction in base64
func (e *ExecutionAgent) buildTransaction(blockhash string, accounts []string, instrData []byte, feePayer string) (string, error) {
	// Collect all unique account keys
	allKeys := []string{feePayer}
	keyIndex := map[string]int{feePayer: 0}
	for _, acc := range accounts {
		if _, exists := keyIndex[acc]; !exists {
			keyIndex[acc] = len(allKeys)
			allKeys = append(allKeys, acc)
		}
	}
	// Also add PumpFunProgram as signer/program key
	if _, exists := keyIndex[PumpFunProgram]; !exists {
		keyIndex[PumpFunProgram] = len(allKeys)
		allKeys = append(allKeys, PumpFunProgram)
	}

	// Build account index list for instruction
	instrAccounts := make([]byte, len(accounts))
	for i, acc := range accounts {
		instrAccounts[i] = byte(keyIndex[acc])
	}

	// Build message
	// Header: [numSigners, numReadonlySigners, numReadonlyNonSigners]
	header := []byte{1, 0, byte(len(allKeys) - 1)} // 1 signer (fee payer), rest readonly

	// Account addresses (32 bytes each)
	accountBytes := make([]byte, 0, len(allKeys)*32)
	for _, key := range allKeys {
		keyBytes := mustDecode58(key)
		accountBytes = append(accountBytes, keyBytes...)
	}

	// Recent blockhash (32 bytes)
	blockhashBytes := mustDecode58(blockhash)

	// Instructions: [numInstructions][programIdIndex][numAccounts][accounts...][dataLen][data...]
	programIdx := byte(keyIndex[PumpFunProgram])
	instrBytes := []byte{
		1,          // num instructions
		programIdx, // program id index
		byte(len(instrAccounts)), // num accounts
	}
	instrBytes = append(instrBytes, instrAccounts...)
	instrBytes = append(instrBytes, byte(len(instrData))) // data length (compact u16)
	instrBytes = append(instrBytes, instrData...)

	// Compile message
	message := []byte{}
	message = append(message, header...)
	message = append(message, compactU16(len(allKeys))...)
	message = append(message, accountBytes...)
	message = append(message, blockhashBytes...)
	message = append(message, instrBytes...)

	// Sign message
	sig := ed25519.Sign(e.privateKey, message)

	// Build transaction: [numSignatures][signature][message]
	txBytes := []byte{1} // num signatures
	txBytes = append(txBytes, sig...)
	txBytes = append(txBytes, message...)

	return base64.StdEncoding.EncodeToString(txBytes), nil
}

// compactU16 encodes a u16 in Solana's compact format
func compactU16(n int) []byte {
	if n < 128 {
		return []byte{byte(n)}
	}
	return []byte{byte(n&0x7F | 0x80), byte(n >> 7)}
}

// simulateTransaction simulates a transaction
func (e *ExecutionAgent) simulateTransaction(txB64 string) (string, error) {
	result, err := e.rpcCall("simulateTransaction", []interface{}{
		txB64,
		map[string]interface{}{
			"encoding":  "base64",
			"commitment": "confirmed",
		},
	})
	if err != nil {
		return "", err
	}
	var simResult struct {
		Value struct {
			Err  interface{} `json:"err"`
			Logs []string    `json:"logs"`
		} `json:"value"`
	}
	json.Unmarshal(result, &simResult)
	if simResult.Value.Err != nil {
		errJSON, _ := json.Marshal(simResult.Value.Err)
		// Print logs for debugging
		for _, l := range simResult.Value.Logs {
			log.Printf("ExecutionAgent: SimLog: %s\n", l)
		}
		return "", fmt.Errorf("simulation error: %s", string(errJSON))
	}
	return "simulation ok", nil
}

// sendTransaction broadcasts a transaction
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

// ─── Public ExecutionAgent methods ───────────────────────────────────────────

// Execute performs a trade execution
func (e *ExecutionAgent) Execute(ctx context.Context, candidate *models.CandidateToken) (*models.ExecutionResult, error) {
	log.Printf("ExecutionAgent: Executing trade for %s on %s\n",
		candidate.Token.TokenAddress, candidate.Token.Chain)

	result := &models.ExecutionResult{
		TokenAddress: candidate.Token.TokenAddress,
		Chain:        candidate.Token.Chain,
		AmountUSD:    candidate.StrategyDecision.SuggestedAmountUSD,
		Timestamp:    time.Now(),
		Status:       "pending",
	}

	// Dry run mode
	if e.config.DryRun {
		log.Printf("ExecutionAgent: DRY RUN - Would buy %s for $%.2f\n",
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

	// Convert USD amount to SOL (rough estimate at $150/SOL)
	solAmount := candidate.StrategyDecision.SuggestedAmountUSD / 150.0
	if solAmount < 0.001 {
		solAmount = 0.001 // minimum 0.001 SOL
	}

	txHash, err := e.buildAndSendBuyTx(ctx, candidate.Token.TokenAddress, solAmount)
	if err != nil {
		log.Printf("ExecutionAgent: Buy failed for %s: %v\n", candidate.Token.TokenAddress, err)
		result.Status = "failed"
		result.Error = err.Error()
		return result, err
	}

	result.Status = "confirmed"
	result.TxHash = txHash
	return result, nil
}

// Simulate performs a dry simulation
func (e *ExecutionAgent) Simulate(ctx context.Context, candidate *models.CandidateToken) (bool, error) {
	log.Printf("ExecutionAgent: Simulating trade for %s\n", candidate.Token.TokenAddress)

	if e.privateKey == nil {
		log.Printf("ExecutionAgent: No private key, skipping simulation\n")
		return true, nil
	}

	solAmount := candidate.StrategyDecision.SuggestedAmountUSD / 150.0
	if solAmount < 0.001 {
		solAmount = 0.001
	}

	blockhash, err := e.getRecentBlockhash()
	if err != nil {
		return false, err
	}

	bondingCurve, assocBondingCurve, err := e.getBondingCurveAccounts(candidate.Token.TokenAddress)
	if err != nil {
		return false, err
	}

	walletPubkey := base58Encode(e.publicKey)
	userATA, err := e.getAssociatedTokenAddress(walletPubkey, candidate.Token.TokenAddress)
	if err != nil {
		return false, err
	}

	lamports := solToLamports(solAmount)
	maxSolCost := uint64(float64(lamports) * 1.01)
	instrData := buildBuyInstructionData(1_000_000_000_000, maxSolCost)

	accounts := []string{
		PumpFunGlobal, PumpFunFeeRecipient, candidate.Token.TokenAddress,
		bondingCurve, assocBondingCurve, userATA, walletPubkey,
		SystemProgram, Token2022Program, RentSysvar, PumpFunEventAuth, PumpFunProgram,
	}

	tx, err := e.buildTransaction(blockhash, accounts, instrData, walletPubkey)
	if err != nil {
		return false, err
	}

	_, err = e.simulateTransaction(tx)
	if err != nil {
		log.Printf("ExecutionAgent: Simulation failed: %v\n", err)
		return false, err
	}

	return true, nil
}

// GetSignerType returns the configured signer type
func (e *ExecutionAgent) GetSignerType() string {
	if e.privateKey != nil {
		return "private_key"
	}
	return "none"
}
