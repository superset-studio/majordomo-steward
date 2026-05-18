package provider

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBedrockParser_ParseResponse_Converse(t *testing.T) {
	parser := &BedrockParser{}

	tests := []struct {
		name              string
		responseBody      []byte
		wantInput         int
		wantOutput        int
		wantCached        int
		wantCacheCreation int
		wantErr           bool
	}{
		{
			name: "converse standard response",
			responseBody: []byte(`{
				"output": {"message": {"role": "assistant", "content": [{"text": "hi"}]}},
				"stopReason": "end_turn",
				"usage": {"inputTokens": 12, "outputTokens": 15, "totalTokens": 27}
			}`),
			wantInput:  12,
			wantOutput: 15,
		},
		{
			name: "converse with cache reads and writes",
			responseBody: []byte(`{
				"usage": {
					"inputTokens": 100,
					"outputTokens": 50,
					"cacheReadInputTokens": 200,
					"cacheWriteInputTokens": 300
				}
			}`),
			wantInput:         600,
			wantOutput:        50,
			wantCached:        200,
			wantCacheCreation: 300,
		},
		{
			name:         "malformed json",
			responseBody: []byte(`{not json`),
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics, err := parser.ParseResponse(tt.responseBody)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if metrics.Provider != string(ProviderBedrock) {
				t.Errorf("provider: got %q want %q", metrics.Provider, ProviderBedrock)
			}
			if metrics.InputTokens != tt.wantInput {
				t.Errorf("input tokens: got %d want %d", metrics.InputTokens, tt.wantInput)
			}
			if metrics.OutputTokens != tt.wantOutput {
				t.Errorf("output tokens: got %d want %d", metrics.OutputTokens, tt.wantOutput)
			}
			if metrics.CachedTokens != tt.wantCached {
				t.Errorf("cached tokens: got %d want %d", metrics.CachedTokens, tt.wantCached)
			}
			if metrics.CacheCreationTokens != tt.wantCacheCreation {
				t.Errorf("cache creation tokens: got %d want %d", metrics.CacheCreationTokens, tt.wantCacheCreation)
			}
		})
	}
}

func TestBedrockParser_ParseResponse_EventStream(t *testing.T) {
	parser := &BedrockParser{}

	payload := []byte(`{"usage":{"inputTokens":42,"outputTokens":17,"cacheReadInputTokens":5,"cacheWriteInputTokens":3}}`)
	frame := buildEventStreamFrame(t, nil, payload)

	metrics, err := parser.ParseResponse(frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics.InputTokens != 42+5+3 {
		t.Errorf("input tokens: got %d want %d", metrics.InputTokens, 50)
	}
	if metrics.OutputTokens != 17 {
		t.Errorf("output tokens: got %d want %d", metrics.OutputTokens, 17)
	}
	if metrics.CachedTokens != 5 {
		t.Errorf("cached tokens: got %d want 5", metrics.CachedTokens)
	}
	if metrics.CacheCreationTokens != 3 {
		t.Errorf("cache creation tokens: got %d want 3", metrics.CacheCreationTokens)
	}
}

func TestBedrockParser_ParseResponse_EventStream_MultipleFrames(t *testing.T) {
	parser := &BedrockParser{}

	deltaFrame := buildEventStreamFrame(t, nil, []byte(`{"delta":{"text":"hello"}}`))
	metadataFrame := buildEventStreamFrame(t, nil, []byte(`{"usage":{"inputTokens":10,"outputTokens":20}}`))
	body := append(deltaFrame, metadataFrame...)

	metrics, err := parser.ParseResponse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics.InputTokens != 10 || metrics.OutputTokens != 20 {
		t.Errorf("got %d input %d output, want 10/20", metrics.InputTokens, metrics.OutputTokens)
	}
}

func TestExtractBedrockModelFromPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/model/anthropic.claude-opus-4-7-v1:0/converse", "anthropic.claude-opus-4-7-v1:0"},
		{"/model/anthropic.claude-sonnet-4-6-v1:0/converse-stream", "anthropic.claude-sonnet-4-6-v1:0"},
		{"/model/us.anthropic.claude-haiku-4-5-v1:0/converse", "us.anthropic.claude-haiku-4-5-v1:0"},
		{"/v1/messages", ""},
		{"/model/", ""},
		{"/model/foo/bar", ""},
	}
	for _, tt := range tests {
		if got := ExtractBedrockModelFromPath(tt.path); got != tt.want {
			t.Errorf("ExtractBedrockModelFromPath(%q): got %q want %q", tt.path, got, tt.want)
		}
	}
}

// buildEventStreamFrame constructs a minimal AWS eventstream frame:
//   [total_len 4B BE][headers_len 4B BE][prelude_crc 4B][headers][payload][message_crc 4B]
// CRC values are zero — the parser does not verify them.
func buildEventStreamFrame(t *testing.T, headers, payload []byte) []byte {
	t.Helper()
	totalLen := uint32(12 + len(headers) + len(payload) + 4)
	headersLen := uint32(len(headers))

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, totalLen)
	binary.Write(&buf, binary.BigEndian, headersLen)
	binary.Write(&buf, binary.BigEndian, uint32(0)) // prelude CRC
	buf.Write(headers)
	buf.Write(payload)
	binary.Write(&buf, binary.BigEndian, uint32(0)) // message CRC
	return buf.Bytes()
}
