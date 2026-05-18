package provider

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/superset-studio/majordomo-steward/internal/models"
)

type BedrockParser struct{}

type bedrockConverseResponse struct {
	Usage struct {
		InputTokens            int `json:"inputTokens"`
		OutputTokens           int `json:"outputTokens"`
		CacheReadInputTokens   int `json:"cacheReadInputTokens"`
		CacheWriteInputTokens  int `json:"cacheWriteInputTokens"`
	} `json:"usage"`
}

type bedrockStreamMetadataEvent struct {
	Usage struct {
		InputTokens            int `json:"inputTokens"`
		OutputTokens           int `json:"outputTokens"`
		CacheReadInputTokens   int `json:"cacheReadInputTokens"`
		CacheWriteInputTokens  int `json:"cacheWriteInputTokens"`
	} `json:"usage"`
}

func (p *BedrockParser) ParseResponse(body []byte) (*models.UsageMetrics, error) {
	if isEventStreamResponse(body) {
		return p.parseEventStreamResponse(body)
	}

	var resp bedrockConverseResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode bedrock converse response: %w", err)
	}

	return buildBedrockMetrics(resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.CacheReadInputTokens, resp.Usage.CacheWriteInputTokens), nil
}

// ExtractModel returns an empty string for Bedrock — the model is in the request
// path (/model/{modelId}/converse), not the body. Callers must extract it via
// ExtractBedrockModelFromPath.
func (p *BedrockParser) ExtractModel(requestBody []byte) string {
	return ""
}

// ExtractBedrockModelFromPath pulls the model ID out of a Bedrock invocation path
// of the form /model/{modelId}/converse or /model/{modelId}/converse-stream.
// Returns empty string if the path does not match.
func ExtractBedrockModelFromPath(path string) string {
	const prefix = "/model/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := path[len(prefix):]
	for _, suffix := range []string{"/converse-stream", "/converse"} {
		if idx := strings.LastIndex(rest, suffix); idx > 0 && idx+len(suffix) == len(rest) {
			return rest[:idx]
		}
	}
	return ""
}

// AWS eventstream binary frames begin with a 12-byte prelude:
//   [4 bytes total length BE][4 bytes headers length BE][4 bytes prelude CRC BE]
// followed by the headers block, the payload, and a 4-byte message CRC. We only
// need to walk frames and JSON-decode payloads — header parsing and CRC
// verification are unnecessary for usage extraction.
func (p *BedrockParser) parseEventStreamResponse(body []byte) (*models.UsageMetrics, error) {
	var inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens int
	var found bool

	offset := 0
	for offset < len(body) {
		if len(body)-offset < 12 {
			break
		}
		totalLen := binary.BigEndian.Uint32(body[offset : offset+4])
		headersLen := binary.BigEndian.Uint32(body[offset+4 : offset+8])

		if totalLen < 16 || int(totalLen) > len(body)-offset {
			return nil, errors.New("invalid eventstream frame length")
		}
		if headersLen > totalLen-16 {
			return nil, errors.New("invalid eventstream headers length")
		}

		payloadStart := offset + 12 + int(headersLen)
		payloadEnd := offset + int(totalLen) - 4
		if payloadStart > payloadEnd {
			return nil, errors.New("invalid eventstream payload bounds")
		}
		payload := body[payloadStart:payloadEnd]

		var event bedrockStreamMetadataEvent
		if err := json.Unmarshal(payload, &event); err == nil && event.Usage.InputTokens+event.Usage.OutputTokens > 0 {
			inputTokens = event.Usage.InputTokens
			outputTokens = event.Usage.OutputTokens
			cacheReadTokens = event.Usage.CacheReadInputTokens
			cacheWriteTokens = event.Usage.CacheWriteInputTokens
			found = true
		}

		offset += int(totalLen)
	}

	if !found {
		return &models.UsageMetrics{Provider: string(ProviderBedrock)}, nil
	}

	return buildBedrockMetrics(inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens), nil
}

// isEventStreamResponse checks whether the body looks like an AWS eventstream
// payload by inspecting the prelude bounds. Avoids ambiguity with JSON bodies
// (which start with '{' = 0x7B; eventstream's first 4 bytes are a length).
func isEventStreamResponse(body []byte) bool {
	if len(body) < 16 {
		return false
	}
	totalLen := binary.BigEndian.Uint32(body[0:4])
	headersLen := binary.BigEndian.Uint32(body[4:8])
	return totalLen >= 16 && int(totalLen) <= len(body) && headersLen <= totalLen-16
}

func buildBedrockMetrics(input, output, cacheRead, cacheWrite int) *models.UsageMetrics {
	// Match the convention used by AnthropicParser: InputTokens is the total
	// input including cache reads and cache writes.
	totalInput := input + cacheRead + cacheWrite
	return &models.UsageMetrics{
		Provider:            string(ProviderBedrock),
		InputTokens:         totalInput,
		OutputTokens:        output,
		CachedTokens:        cacheRead,
		CacheCreationTokens: cacheWrite,
	}
}
