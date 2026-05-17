package scanner

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mumugogoing/meme_bot/pkg/config"
	"github.com/mumugogoing/meme_bot/pkg/models"
)

// ChainScannerAgent monitors on-chain events for new tokens
type ChainScannerAgent struct {
	config       *config.Config
	tokenChannel chan models.TokenFound
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// NewChainScannerAgent creates a new chain scanner agent
func NewChainScannerAgent(cfg *config.Config) *ChainScannerAgent {
	ctx, cancel := context.WithCancel(context.Background())
	return &ChainScannerAgent{
		config:       cfg,
		tokenChannel: make(chan models.TokenFound, 100),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start begins scanning both chains
func (s *ChainScannerAgent) Start() {
	log.Println("ChainScannerAgent: Starting chain monitoring...")
	
	// Start Solana scanner
	s.wg.Add(1)
	go s.scanSolana()
	
	// Start Base scanner only if BASE_RPC_URL is configured
	if os.Getenv("BASE_RPC_URL") != "" {
		s.wg.Add(1)
		go s.scanBase()
	} else {
		log.Println("ChainScannerAgent: BASE_RPC_URL not set, skipping Base chain scanning")
	}
	
	log.Println("ChainScannerAgent: Chain monitoring started")
}

// Stop stops all scanning operations
func (s *ChainScannerAgent) Stop() {
	log.Println("ChainScannerAgent: Stopping...")
	s.cancel()
	s.wg.Wait()
	close(s.tokenChannel)
	log.Println("ChainScannerAgent: Stopped")
}

// GetTokenChannel returns the channel for discovered tokens
func (s *ChainScannerAgent) GetTokenChannel() <-chan models.TokenFound {
	return s.tokenChannel
}

// scanSolana monitors Solana chain for new tokens
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

// scanBase monitors Base chain for new tokens
func (s *ChainScannerAgent) scanBase() {
	defer s.wg.Done()
	
	ticker := time.NewTicker(s.config.ScanIntervalBase)
	defer ticker.Stop()
	
	log.Printf("ChainScannerAgent: Base scanner started (interval: %v)\n", s.config.ScanIntervalBase)
	
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.scanBaseNewTokens()
		}
	}
}

// scanSolanaNewTokens scans for new tokens on Solana
func (s *ChainScannerAgent) scanSolanaNewTokens() {
	// TODO: Implement actual Solana RPC scanning
	// - Monitor program logs for token creation events
	// - Parse transaction data for new mint addresses
	// - Check for liquidity pool creation (Raydium, Orca)
	// - Extract initial liquidity and creator info
	
	log.Println("ChainScannerAgent: Scanning Solana for new tokens...")
	
	// Placeholder: This would connect to Solana RPC and monitor events
	// Example implementation would:
	// 1. Use getProgramAccounts to find new SPL Token mints
	// 2. Monitor Raydium/Orca factory contracts for pool creation
	// 3. Parse transaction signatures for relevant events
	
	// For now, this is a stub that would be replaced with actual implementation
}

// scanBaseNewTokens scans for new tokens on Base
func (s *ChainScannerAgent) scanBaseNewTokens() {
	// TODO: Implement actual Base (EVM) scanning
	// - Monitor Uniswap V2/V3 factory events
	// - Track PairCreated events
	// - Parse token metadata from ERC20 contracts
	// - Extract initial liquidity from pool reserves
	
	log.Println("ChainScannerAgent: Scanning Base for new tokens...")
	
	// Placeholder: This would connect to Base RPC and monitor events
	// Example implementation would:
	// 1. Subscribe to PairCreated events from Uniswap factory
	// 2. Parse event logs for new token addresses
	// 3. Query initial reserves from the pair contract
	// 4. Get token metadata (name, symbol, decimals)
	
	// For now, this is a stub that would be replaced with actual implementation
}

// emitTokenFound sends a discovered token to the channel
func (s *ChainScannerAgent) emitTokenFound(token models.TokenFound) {
	select {
	case s.tokenChannel <- token:
		log.Printf("ChainScannerAgent: Token found - %s on %s\n", token.TokenAddress, token.Chain)
	case <-s.ctx.Done():
		return
	default:
		log.Println("ChainScannerAgent: Warning - token channel full, dropping event")
	}
}
