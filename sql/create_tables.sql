CREATE TABLE IF NOT EXISTS raw_job_postings (
  source TEXT NOT NULL,
  fetched_at TIMESTAMP NOT NULL,
  job_id TEXT NOT NULL,
  title TEXT,
  company TEXT,
  location TEXT,
  description TEXT,
  posted_at TIMESTAMP,
  url TEXT,
  salary_min NUMERIC,
  salary_max NUMERIC,
  raw_json JSONB,
  PRIMARY KEY (source, job_id)
);
