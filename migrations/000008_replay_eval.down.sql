DROP INDEX IF EXISTS idx_eval_result_scores_unsynced;
DROP INDEX IF EXISTS idx_eval_result_scores_name;
DROP INDEX IF EXISTS idx_eval_result_scores_result;
DROP TABLE IF EXISTS eval_result_scores;

DROP INDEX IF EXISTS idx_eval_results_unsynced;
DROP INDEX IF EXISTS idx_eval_results_run;
DROP TABLE IF EXISTS eval_results;

DROP INDEX IF EXISTS idx_eval_runs_unsynced;
DROP INDEX IF EXISTS idx_eval_runs_eval_set;
DROP INDEX IF EXISTS idx_eval_runs_pending;
DROP INDEX IF EXISTS idx_eval_runs_org_status;
DROP TABLE IF EXISTS eval_runs;

DROP INDEX IF EXISTS idx_eval_set_items_request;
DROP INDEX IF EXISTS idx_eval_set_items_set;
DROP TABLE IF EXISTS eval_set_items;

DROP INDEX IF EXISTS idx_eval_sets_org;
DROP TABLE IF EXISTS eval_sets;

DROP INDEX IF EXISTS idx_replay_results_unsynced;
DROP INDEX IF EXISTS idx_replay_results_run;
DROP TABLE IF EXISTS replay_results;

DROP INDEX IF EXISTS idx_replay_runs_unsynced;
DROP INDEX IF EXISTS idx_replay_runs_pending;
DROP INDEX IF EXISTS idx_replay_runs_org_status;
DROP TABLE IF EXISTS replay_runs;
