CREATE TABLE IF NOT EXISTS job_daily_metrics (
  dt DATE,
  state TEXT,
  role_family TEXT,
  jobs_posted INT,
  avg_salary NUMERIC,
  median_salary NUMERIC,
  PRIMARY KEY (dt, state, role_family)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id SERIAL PRIMARY KEY,
  pipeline_name TEXT,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status TEXT,
  rows_processed INT,
  error TEXT
);
