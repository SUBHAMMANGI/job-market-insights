CREATE TABLE IF NOT EXISTS job_postings_clean (
  job_id TEXT PRIMARY KEY,
  source TEXT,
  fetched_at TIMESTAMP,
  posted_at TIMESTAMP,
  title TEXT,
  company TEXT,
  location_raw TEXT,
  city TEXT,
  state TEXT,
  url TEXT,
  salary_min NUMERIC,
  salary_max NUMERIC,
  salary_mid NUMERIC,
  description_clean TEXT
);

-- Helpful indexes for faster Power BI / SQL
CREATE INDEX IF NOT EXISTS idx_clean_state ON job_postings_clean(state);
CREATE INDEX IF NOT EXISTS idx_clean_posted_at ON job_postings_clean(posted_at);
