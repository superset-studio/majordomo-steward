package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/superset-studio/majordomo-steward/internal/repositories"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
)

const (
	proxyKeyCacheSize = 4096
	provKeyCacheSize  = 16384 // 4096 keys × ~4 providers each
)

var (
	ErrProxyKeyNotFound   = errors.New("proxy key not found")
	ErrProxyKeyRevoked    = errors.New("proxy key has been revoked")
	ErrProxyKeyInactive   = errors.New("proxy key is not active")
	ErrProxyKeyWrongOwner = errors.New("proxy key does not belong to this Majordomo key")
	ErrNoProviderMapping  = errors.New("no provider key configured")
)

type cachedProxyKey struct {
	proxyKeyID        uuid.UUID
	majordomoAPIKeyID uuid.UUID
	isActive          bool
	revokedAt         *time.Time
	expiresAt         time.Time
}

// ProxyResolver validates proxy keys and resolves them to decrypted provider API keys.
type ProxyResolver struct {
	storage   repositories.ProxyKeyStorage
	secrets   secrets.SecretStore
	keyCache  *lru.Cache[string, *cachedProxyKey] // hash → proxy key metadata
	provCache *lru.Cache[string, string]           // hash:provider → decrypted provider key
	cacheTTL  time.Duration
}

// NewProxyResolver creates a new ProxyResolver.
func NewProxyResolver(storage repositories.ProxyKeyStorage, secretStore secrets.SecretStore) *ProxyResolver {
	keyCache, err := lru.New[string, *cachedProxyKey](proxyKeyCacheSize)
	if err != nil {
		panic(err) // only possible if proxyKeyCacheSize <= 0
	}
	provCache, err := lru.New[string, string](provKeyCacheSize)
	if err != nil {
		panic(err) // only possible if provKeyCacheSize <= 0
	}
	return &ProxyResolver{
		storage:   storage,
		secrets:   secretStore,
		keyCache:  keyCache,
		provCache: provCache,
		cacheTTL:  5 * time.Minute,
	}
}

// ResolveProxyKey validates a proxy key and returns the decrypted provider API key
// for the given provider. Returns ("", nil, nil) if the key is not a proxy key (no mdm_pk_ prefix).
func (r *ProxyResolver) ResolveProxyKey(ctx context.Context, authKey string, provider string, majordomoKeyID uuid.UUID) (providerKey string, proxyKeyID *uuid.UUID, err error) {
	if !strings.HasPrefix(authKey, ProxyKeyPrefix) {
		return "", nil, nil
	}

	hash := HashAPIKey(authKey)
	provCacheKey := hash + ":" + provider

	// Check provider key cache — valid only if the key metadata is also cached and not expired.
	if decrypted, ok := r.provCache.Get(provCacheKey); ok {
		if pkc, ok := r.keyCache.Get(hash); ok && time.Now().Before(pkc.expiresAt) {
			id := pkc.proxyKeyID
			return decrypted, &id, nil
		}
	}

	// DB lookup
	proxyKey, err := r.storage.GetProxyKeyByHash(ctx, hash)
	if err != nil {
		return "", nil, fmt.Errorf("failed to look up proxy key: %w", err)
	}
	if proxyKey == nil {
		return "", nil, ErrProxyKeyNotFound
	}

	if !proxyKey.IsActive {
		if proxyKey.RevokedAt != nil {
			return "", nil, ErrProxyKeyRevoked
		}
		return "", nil, ErrProxyKeyInactive
	}

	if proxyKey.MajordomoAPIKeyID != majordomoKeyID {
		return "", nil, ErrProxyKeyWrongOwner
	}

	// Look up provider mapping
	mapping, err := r.storage.GetProviderMapping(ctx, proxyKey.ID, provider)
	if err != nil {
		return "", nil, fmt.Errorf("failed to look up provider mapping: %w", err)
	}
	if mapping == nil {
		return "", nil, fmt.Errorf("%w for %s", ErrNoProviderMapping, provider)
	}

	// Decrypt provider API key
	decrypted, err := r.secrets.Decrypt(mapping.EncryptedKey)
	if err != nil {
		return "", nil, fmt.Errorf("failed to decrypt provider key: %w", err)
	}

	// Cache the results
	r.keyCache.Add(hash, &cachedProxyKey{
		proxyKeyID:        proxyKey.ID,
		majordomoAPIKeyID: proxyKey.MajordomoAPIKeyID,
		isActive:          proxyKey.IsActive,
		revokedAt:         proxyKey.RevokedAt,
		expiresAt:         time.Now().Add(r.cacheTTL),
	})
	r.provCache.Add(provCacheKey, decrypted)

	// Update last_used_at asynchronously
	go func() {
		if err := r.storage.UpdateProxyKeyLastUsed(context.Background(), proxyKey.ID); err != nil {
			slog.Warn("failed to update proxy key last_used_at", "error", err, "proxy_key_id", proxyKey.ID)
		}
	}()

	id := proxyKey.ID
	return decrypted, &id, nil
}

// InvalidateCache removes a specific proxy key from the cache (call after revocation).
// Orphaned provCache entries for this hash are effectively dead without a valid keyCache
// entry and will be evicted by LRU pressure.
func (r *ProxyResolver) InvalidateCache(hash string) {
	r.keyCache.Remove(hash)
}
