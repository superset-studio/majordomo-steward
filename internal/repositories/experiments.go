package repositories

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/models"
	"github.com/superset-studio/majordomo-steward/internal/stewardclient"
)

// ExperimentRepository handles local experiment cache operations.
// It satisfies stewardclient.ExperimentSyncStore (for sync) and
// proxy.ExperimentStore (for routing).
type ExperimentRepository struct {
	db *sqlx.DB
}

// NewExperimentRepository constructs an ExperimentRepository.
func NewExperimentRepository(db *sqlx.DB) *ExperimentRepository {
	return &ExperimentRepository{db: db}
}

// UpsertExperiments inserts or updates a batch of experiments and their arms
// received from Butler's experiment-sync endpoint.
func (r *ExperimentRepository) UpsertExperiments(ctx context.Context, records []stewardclient.ExperimentRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	const expQ = `
		INSERT INTO experiments (id, org_id, status, api_key_id, metadata_filters, sticky_key, starts_at, ends_at, updated_at, synced_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
		ON CONFLICT (id) DO UPDATE SET
			status           = EXCLUDED.status,
			api_key_id       = EXCLUDED.api_key_id,
			metadata_filters = EXCLUDED.metadata_filters,
			sticky_key       = EXCLUDED.sticky_key,
			starts_at        = EXCLUDED.starts_at,
			ends_at          = EXCLUDED.ends_at,
			updated_at       = EXCLUDED.updated_at,
			synced_at        = now()`

	const armQ = `
		INSERT INTO experiment_arms (id, experiment_id, name, model, weight, is_control)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET
			name       = EXCLUDED.name,
			model      = EXCLUDED.model,
			weight     = EXCLUDED.weight,
			is_control = EXCLUDED.is_control`

	const deleteOrphanArmsQ = `
		DELETE FROM experiment_arms
		WHERE experiment_id = $1 AND id NOT IN (SELECT unnest($2::uuid[]))`

	for i := range records {
		rec := &records[i]

		filtersJSON, err := json.Marshal(rec.MetadataFilters)
		if err != nil {
			return fmt.Errorf("marshal metadata_filters for experiment %s: %w", rec.ID, err)
		}

		if _, err := tx.ExecContext(ctx, expQ,
			rec.ID, rec.OrgID, rec.Status, rec.APIKeyID,
			string(filtersJSON), rec.StickyKey,
			rec.StartsAt, rec.EndsAt, rec.UpdatedAt,
		); err != nil {
			return fmt.Errorf("upsert experiment %s: %w", rec.ID, err)
		}

		armIDs := make([]uuid.UUID, 0, len(rec.Arms))
		for _, arm := range rec.Arms {
			if _, err := tx.ExecContext(ctx, armQ,
				arm.ID, arm.ExperimentID, arm.Name, arm.Model, arm.Weight, arm.IsControl,
			); err != nil {
				return fmt.Errorf("upsert arm %s: %w", arm.ID, err)
			}
			armIDs = append(armIDs, arm.ID)
		}

		// Remove arms that are no longer present in this experiment.
		if _, err := tx.ExecContext(ctx, deleteOrphanArmsQ, rec.ID, uuidArray(armIDs)); err != nil {
			return fmt.Errorf("delete orphan arms for experiment %s: %w", rec.ID, err)
		}
	}

	return tx.Commit()
}

// ListActiveExperiments returns all active experiments for an org whose time window
// contains now, with arms populated.
func (r *ExperimentRepository) ListActiveExperiments(ctx context.Context, orgID uuid.UUID, now time.Time) ([]models.LocalExperiment, error) {
	const q = `
		SELECT id, org_id, status, api_key_id, metadata_filters, sticky_key, starts_at, ends_at, updated_at
		FROM experiments
		WHERE org_id = $1
		  AND status = 'active'
		  AND starts_at <= $2
		  AND ends_at   >= $2`

	rows, err := r.db.QueryContext(ctx, q, orgID, now)
	if err != nil {
		return nil, fmt.Errorf("list active experiments: %w", err)
	}
	defer rows.Close()

	var exps []models.LocalExperiment
	var expIDs []uuid.UUID

	for rows.Next() {
		var exp models.LocalExperiment
		var filtersJSON []byte

		if err := rows.Scan(
			&exp.ID, &exp.OrgID, &exp.Status, &exp.APIKeyID,
			&filtersJSON, &exp.StickyKey,
			&exp.StartsAt, &exp.EndsAt, &exp.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan experiment: %w", err)
		}

		if len(filtersJSON) > 0 {
			if err := json.Unmarshal(filtersJSON, &exp.MetadataFilters); err != nil {
				return nil, fmt.Errorf("unmarshal metadata_filters for experiment %s: %w", exp.ID, err)
			}
		}

		exps = append(exps, exp)
		expIDs = append(expIDs, exp.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate experiments: %w", err)
	}

	if len(exps) == 0 {
		return exps, nil
	}

	// Fetch arms for all matching experiments.
	armMap, err := r.fetchArmsByExperimentIDs(ctx, expIDs)
	if err != nil {
		return nil, err
	}
	for i := range exps {
		exps[i].Arms = armMap[exps[i].ID]
	}

	return exps, nil
}

func (r *ExperimentRepository) fetchArmsByExperimentIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]models.LocalExperimentArm, error) {
	query, args, err := sqlx.In(`
		SELECT id, experiment_id, name, model, weight, is_control
		FROM experiment_arms
		WHERE experiment_id IN (?)
		ORDER BY is_control DESC, name ASC`, toInterfaceSlice(ids))
	if err != nil {
		return nil, fmt.Errorf("build arms query: %w", err)
	}
	query = r.db.Rebind(query)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch arms: %w", err)
	}
	defer rows.Close()

	result := make(map[uuid.UUID][]models.LocalExperimentArm)
	for rows.Next() {
		var arm models.LocalExperimentArm
		if err := rows.Scan(&arm.ID, &arm.ExperimentID, &arm.Name, &arm.Model, &arm.Weight, &arm.IsControl); err != nil {
			return nil, fmt.Errorf("scan arm: %w", err)
		}
		result[arm.ExperimentID] = append(result[arm.ExperimentID], arm)
	}
	return result, rows.Err()
}

// uuidArray wraps a []uuid.UUID for use as a PostgreSQL uuid[] parameter.
type uuidArray []uuid.UUID

func (a uuidArray) Value() (interface{}, error) {
	if len(a) == 0 {
		return "{}", nil
	}
	strs := make([]string, len(a))
	for i, id := range a {
		strs[i] = id.String()
	}
	result := "{"
	for i, s := range strs {
		if i > 0 {
			result += ","
		}
		result += s
	}
	return result + "}", nil
}

func toInterfaceSlice(ids []uuid.UUID) []interface{} {
	out := make([]interface{}, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}
