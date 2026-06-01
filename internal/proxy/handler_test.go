package proxy

import (
	"net/http/httptest"
	"testing"
)

func TestExtractCustomMetadata(t *testing.T) {
	tests := []struct {
		name     string
		headers  map[string]string
		expected map[string]string
	}{
		{
			name: "regular metadata headers pass through",
			headers: map[string]string{
				"x-majordomo-developer": "alice",
				"x-majordomo-project":   "backend",
			},
			expected: map[string]string{
				"developer": "alice",
				"project":   "backend",
			},
		},
		{
			name: "session-id is no longer reserved and passes through as metadata",
			headers: map[string]string{
				"x-majordomo-session-id": "my-session-123",
				"x-majordomo-developer":  "alice",
			},
			expected: map[string]string{
				"session-id": "my-session-123",
				"developer":  "alice",
			},
		},
		{
			name: "x-majordomo-client is excluded",
			headers: map[string]string{
				"x-majordomo-client":    "claude-code",
				"x-majordomo-developer": "alice",
			},
			expected: map[string]string{
				"developer": "alice",
			},
		},
		{
			name: "x-majordomo-claudecode-session-id is excluded",
			headers: map[string]string{
				"x-majordomo-claudecode-session-id": "abc-123",
				"x-majordomo-developer":             "alice",
			},
			expected: map[string]string{
				"developer": "alice",
			},
		},
		{
			name: "all reserved headers are excluded",
			headers: map[string]string{
				"x-majordomo-key":                   "mdm_sk_test",
				"x-majordomo-provider":              "anthropic",
				"x-majordomo-provider-alias":        "prod-key",
				"x-majordomo-client":                "claude-code",
				"x-majordomo-claudecode-session-id": "abc-123",
				"x-majordomo-developer":             "alice",
			},
			expected: map[string]string{
				"developer": "alice",
			},
		},
		{
			name:     "empty headers returns empty map",
			headers:  map[string]string{},
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractCustomMetadata(tt.headers)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d entries, got %d: %v", len(tt.expected), len(result), result)
			}
			for key, expectedVal := range tt.expected {
				if gotVal, ok := result[key]; !ok {
					t.Errorf("expected key %q not found in result", key)
				} else if gotVal != expectedVal {
					t.Errorf("key %q: expected %q, got %q", key, expectedVal, gotVal)
				}
			}
		})
	}
}

func TestResolveBedrockRegion(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		headerVal  string
		wantRegion string
		wantOK     bool
	}{
		{
			name:       "header takes precedence over host",
			host:       "bedrock-runtime.us-east-1.amazonaws.com",
			headerVal:  "eu-west-2",
			wantRegion: "eu-west-2",
			wantOK:     true,
		},
		{
			name:       "header alone with arbitrary host",
			host:       "gateway.gomajordomo.com",
			headerVal:  "ap-southeast-1",
			wantRegion: "ap-southeast-1",
			wantOK:     true,
		},
		{
			name:       "host fallback when header absent",
			host:       "bedrock-runtime.us-west-2.amazonaws.com",
			headerVal:  "",
			wantRegion: "us-west-2",
			wantOK:     true,
		},
		{
			name:      "neither header nor valid host returns false",
			host:      "gateway.gomajordomo.com",
			headerVal: "",
			wantOK:    false,
		},
		{
			name:      "invalid header value rejected without falling back to host",
			host:      "bedrock-runtime.us-east-1.amazonaws.com",
			headerVal: "US-EAST-1",
			wantOK:    false,
		},
		{
			name:      "header with disallowed characters rejected",
			host:      "bedrock-runtime.us-east-1.amazonaws.com",
			headerVal: "us_east_1",
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/model/foo/converse", nil)
			req.Host = tt.host
			if tt.headerVal != "" {
				req.Header.Set(BedrockRegionHeader, tt.headerVal)
			}
			gotRegion, gotOK := resolveBedrockRegion(req)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotOK && gotRegion != tt.wantRegion {
				t.Errorf("region = %q, want %q", gotRegion, tt.wantRegion)
			}
		})
	}
}
