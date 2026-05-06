package deprecated

import (
	"encoding/json"
	"log/slog"
	"os"
	"sync"
)

// Service loads and serves the deprecated-model-to-replacement map.
// The map is loaded once at startup from a JSON file; it is read-only after that.
type Service struct {
	mu     sync.RWMutex
	models map[string]string // deprecated model ID -> recommended replacement
}

// NewService loads the deprecated models map from filePath.
// If the file does not exist or cannot be parsed, the service is initialised
// with an empty map so the proxy can still start.
func NewService(filePath string) *Service {
	s := &Service{
		models: make(map[string]string),
	}
	if filePath == "" {
		return s
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		slog.Warn("failed to load deprecated models file", "error", err, "file", filePath)
		return s
	}

	// Raw decode so we can skip the "_comment" key without a special struct.
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		slog.Error("failed to parse deprecated models file", "error", err, "file", filePath)
		return s
	}

	for k, v := range raw {
		if k == "_comment" {
			continue
		}
		s.models[k] = v
	}

	slog.Info("loaded deprecated models", "count", len(s.models), "file", filePath)
	return s
}

// Lookup returns the recommended replacement for a deprecated model ID.
// ok is false when the model is not in the deprecated list.
func (s *Service) Lookup(model string) (replacement string, ok bool) {
	s.mu.RLock()
	replacement, ok = s.models[model]
	s.mu.RUnlock()
	return
}
