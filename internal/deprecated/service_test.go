package deprecated

import (
	"os"
	"testing"
)

func TestNewService_LoadsModelsAndSkipsComment(t *testing.T) {
	data := `{
		"_comment": "this should be ignored",
		"gpt-3.5-turbo": "gpt-4o-mini",
		"claude-2": "claude-3-5-haiku-20241022"
	}`
	f := writeTempJSON(t, data)

	svc := NewService(f)

	tests := []struct {
		model           string
		wantReplacement string
		wantOk          bool
	}{
		{"gpt-3.5-turbo", "gpt-4o-mini", true},
		{"claude-2", "claude-3-5-haiku-20241022", true},
		{"gpt-4o", "", false},       // not in the map
		{"_comment", "", false},     // comment key must be skipped
		{"", "", false},             // empty string never matches
	}

	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			got, ok := svc.Lookup(tt.model)
			if ok != tt.wantOk {
				t.Errorf("Lookup(%q) ok = %v, want %v", tt.model, ok, tt.wantOk)
			}
			if got != tt.wantReplacement {
				t.Errorf("Lookup(%q) replacement = %q, want %q", tt.model, got, tt.wantReplacement)
			}
		})
	}
}

func TestNewService_MissingFile(t *testing.T) {
	svc := NewService("/nonexistent/path/deprecated_models.json")
	if _, ok := svc.Lookup("gpt-3.5-turbo"); ok {
		t.Error("expected no match from service loaded from missing file")
	}
}

func TestNewService_EmptyPath(t *testing.T) {
	svc := NewService("")
	if _, ok := svc.Lookup("anything"); ok {
		t.Error("expected no match from service initialised with empty path")
	}
}

func TestNewService_InvalidJSON(t *testing.T) {
	f := writeTempJSON(t, `not valid json {{{`)
	svc := NewService(f)
	if _, ok := svc.Lookup("anything"); ok {
		t.Error("expected no match from service loaded from invalid JSON")
	}
}

func TestNewService_EmptyMap(t *testing.T) {
	f := writeTempJSON(t, `{}`)
	svc := NewService(f)
	if _, ok := svc.Lookup("gpt-3.5-turbo"); ok {
		t.Error("expected no match from empty deprecated models file")
	}
}

// writeTempJSON writes content to a temp file and returns its path.
// The file is removed automatically when the test ends.
func writeTempJSON(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "deprecated-*.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}
