package llm

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
)

const (
	AnthropicAPIURL = "https://api.anthropic.com/v1/messages"
	AnthropicVer    = "2023-06-01"

	ModelHaiku  = "claude-haiku-4-5-20251001"
	ModelSonnet = "claude-sonnet-4-6"
)

// Client is a thin wrapper around the Anthropic Messages API.
type Client struct {
	apiKey     string
	httpClient *http.Client

	// Cost tracking
	mu          sync.Mutex
	callCount   int
	totalInputTokens  int
	totalOutputTokens int
	estCostUSD  float64
}

func NewClient() *Client {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		log.Println("LLM: ANTHROPIC_API_KEY not set — LLM features disabled")
	}
	return &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) Enabled() bool {
	return c.apiKey != ""
}

// Messages API types

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type apiRequest struct {
	Model     string    `json:"model"`
	MaxTokens int       `json:"max_tokens"`
	System    string    `json:"system,omitempty"`
	Messages  []Message `json:"messages"`
}

type apiResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Complete calls the Anthropic API.
// model: ModelHaiku or ModelSonnet
// system: system prompt (optional)
// user: user message
// maxTokens: response cap
func (c *Client) Complete(ctx context.Context, model, system, user string, maxTokens int) (string, error) {
	if c.apiKey == "" {
		return "", fmt.Errorf("ANTHROPIC_API_KEY not set")
	}

	req := apiRequest{
		Model:     model,
		MaxTokens: maxTokens,
		System:    system,
		Messages:  []Message{{Role: "user", Content: user}},
	}

	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", AnthropicAPIURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", c.apiKey)
	httpReq.Header.Set("anthropic-version", AnthropicVer)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read: %w", err)
	}

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBytes))
	}

	var apiResp apiResponse
	if err := json.Unmarshal(respBytes, &apiResp); err != nil {
		return "", fmt.Errorf("unmarshal: %w (body: %s)", err, string(respBytes))
	}

	if apiResp.Error != nil {
		return "", fmt.Errorf("API error: %s — %s", apiResp.Error.Type, apiResp.Error.Message)
	}

	if len(apiResp.Content) == 0 {
		return "", fmt.Errorf("empty response")
	}

	text := ""
	for _, c := range apiResp.Content {
		if c.Type == "text" {
			text += c.Text
		}
	}

	// Track cost (rough estimates per 1M tokens)
	var inputCost, outputCost float64
	switch model {
	case ModelHaiku:
		inputCost = 1.0 / 1_000_000  // $1 per M
		outputCost = 5.0 / 1_000_000 // $5 per M
	case ModelSonnet:
		inputCost = 3.0 / 1_000_000   // $3 per M
		outputCost = 15.0 / 1_000_000 // $15 per M
	}
	estCost := float64(apiResp.Usage.InputTokens)*inputCost +
		float64(apiResp.Usage.OutputTokens)*outputCost

	c.mu.Lock()
	c.callCount++
	c.totalInputTokens += apiResp.Usage.InputTokens
	c.totalOutputTokens += apiResp.Usage.OutputTokens
	c.estCostUSD += estCost
	c.mu.Unlock()

	return text, nil
}

// Stats returns LLM usage statistics
type Stats struct {
	CallCount         int     `json:"call_count"`
	TotalInputTokens  int     `json:"total_input_tokens"`
	TotalOutputTokens int     `json:"total_output_tokens"`
	EstimatedCostUSD  float64 `json:"estimated_cost_usd"`
}

func (c *Client) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		CallCount:         c.callCount,
		TotalInputTokens:  c.totalInputTokens,
		TotalOutputTokens: c.totalOutputTokens,
		EstimatedCostUSD:  c.estCostUSD,
	}
}

// ─── TIER 1: Token sanity filter (Haiku, fast) ────────────────────────────────

// TokenFilterInput is the data we send for filtering
type TokenFilterInput struct {
	Mint        string `json:"mint"`
	Name        string `json:"name"`
	Symbol      string `json:"symbol"`
	Description string `json:"description"`
	HasTwitter  bool   `json:"has_twitter"`
	HasTelegram bool   `json:"has_telegram"`
	HasWebsite  bool   `json:"has_website"`
	DevPctHolds float64 `json:"dev_pct_holds"`
}

// TokenFilterResult is the LLM's verdict
type TokenFilterResult struct {
	Buy       bool   `json:"buy"`        // recommended action
	Score     int    `json:"score"`      // 0-100
	Reason    string `json:"reason"`     // short explanation
	RedFlags  []string `json:"red_flags,omitempty"`
}

const tokenFilterSystemPrompt = `You are a memecoin sanity filter for a Solana PumpFun trading bot.

Given basic token metadata, return a JSON verdict on whether to consider buying.

You CANNOT predict price action. You CAN identify:
- Low-effort spam (random characters, "test", obvious copy-paste names)
- AI-generated slop descriptions (vague platitudes, generic crypto words)
- Tokens with obviously rugged setups (dev holds >5% suggests likely dump)
- Tokens with no identity (no socials, generic name)

Respond with JSON only, no preamble:
{
  "buy": true|false,
  "score": 0-100,
  "reason": "<1-sentence justification>",
  "red_flags": ["flag1", "flag2"]
}

Be strict. Reject anything that looks low-effort.`

// FilterToken asks Claude Haiku for a quick sanity check on a token.
// Uses a short context to keep costs low (~$0.001 per call).
// Returns nil result on any error — caller should treat as "skip filter, proceed".
func (c *Client) FilterToken(ctx context.Context, input TokenFilterInput) (*TokenFilterResult, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("LLM not configured")
	}

	inputJSON, _ := json.Marshal(input)
	userMsg := fmt.Sprintf("Evaluate this token:\n%s", string(inputJSON))

	resp, err := c.Complete(ctx, ModelHaiku, tokenFilterSystemPrompt, userMsg, 200)
	if err != nil {
		return nil, err
	}

	// Strip markdown fences if present
	resp = strings.TrimSpace(resp)
	if strings.HasPrefix(resp, "```") {
		// remove ```json ... ```
		resp = strings.TrimPrefix(resp, "```json")
		resp = strings.TrimPrefix(resp, "```")
		resp = strings.TrimSuffix(resp, "```")
		resp = strings.TrimSpace(resp)
	}

	var result TokenFilterResult
	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, fmt.Errorf("parse LLM response: %w (raw: %s)", err, resp)
	}

	return &result, nil
}

// ─── TIER 2: Daily strategy reflection (Sonnet) ───────────────────────────────

const reflectionSystemPrompt = `You are an experienced quantitative trader analyzing memecoin trade outcome data.

Given a list of recent closed trades from a Solana PumpFun sniper bot, identify patterns
and recommend parameter adjustments.

The bot uses these tunable parameters:
- TP1_MULTIPLIER (default 2.0 = sell 50% at 2x)
- TP2_MULTIPLIER (default 3.0 = sell 50% of remainder at 3x)
- STOP_LOSS_MULT (default 0.5 = sell at -50%)
- MAX_HOLD_MINUTES (default 5 = force-sell after 5 min if no TP)
- TRAILING_STOP_PCT (default 0.7 = sell when price drops 30% from peak)
- WIN_PROBABILITY_THRESHOLD (default 0.75 = min score to buy)

Respond with concrete recommendations. Be honest about what's broken.
If trades are mostly losing, say so. Don't pretend success where there is none.

Format:
## Summary
<2-3 sentence summary of trading performance>

## Key Patterns
- <pattern 1>
- <pattern 2>

## Recommendations
- TP1_MULTIPLIER: <current> -> <new> because <reason>
- ... (only recommend changes you're confident about)

## Verdict
<honest assessment: keep tuning / change strategy / stop trading>`

func (c *Client) AnalyzeTrades(ctx context.Context, tradesJSON string) (string, error) {
	if c.apiKey == "" {
		return "", fmt.Errorf("LLM not configured")
	}
	userMsg := fmt.Sprintf("Here are the recent closed trades:\n\n%s", tradesJSON)
	return c.Complete(ctx, ModelSonnet, reflectionSystemPrompt, userMsg, 1500)
}

// ─── TIER 3: Dashboard chat (Sonnet) ──────────────────────────────────────────

const chatSystemPrompt = `You are an AI assistant embedded in a PumpFun trading bot dashboard.

The user can ask questions about their bot's behavior and trade data. You will receive
a JSON snapshot of current bot state (open positions, recent trades, risk status, metrics).

Be concise, direct, and honest. If the data shows the bot is losing money, say so.
Don't manufacture optimism. Quote specific numbers from the data.

If asked to make a trade decision or change settings, refuse — you're read-only.

Format responses as plain text, no markdown headers. Use short paragraphs.`

func (c *Client) Chat(ctx context.Context, userMessage, botStateJSON string) (string, error) {
	if c.apiKey == "" {
		return "", fmt.Errorf("LLM not configured")
	}
	combined := fmt.Sprintf("Bot state:\n%s\n\nUser question: %s", botStateJSON, userMessage)
	return c.Complete(ctx, ModelSonnet, chatSystemPrompt, combined, 600)
}
