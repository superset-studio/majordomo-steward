package repositories

import "errors"

var (
	ErrAPIKeyNotFound              = errors.New("API key not found")
	ErrProxyKeyNotFound            = errors.New("proxy key not found")
	ErrProviderMappingNotFound     = errors.New("provider mapping not found")
	ErrClaudeSessionNotFound       = errors.New("claude session not found")
	ErrClaudeRequestDetailNotFound = errors.New("claude request detail not found")
	ErrMetadataKeyNotFound         = errors.New("metadata key not found")
	ErrProviderKeyNotFound         = errors.New("provider key not found")
	ErrReplayRunNotFound           = errors.New("replay run not found")
	ErrReplayResultNotFound        = errors.New("replay result not found")
	ErrEvalSetNotFound             = errors.New("eval set not found")
	ErrEvalRunNotFound             = errors.New("eval run not found")
	ErrEvalResultNotFound          = errors.New("eval result not found")
)
