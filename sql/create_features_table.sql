CREATE TABLE IF NOT EXISTS job_postings_features (
  job_id TEXT PRIMARY KEY,
  extracted_at TIMESTAMP,
  state TEXT,
  city TEXT,
  role_family TEXT,
  seniority TEXT,
  is_remote BOOLEAN,
  years_experience_min NUMERIC,
  skills JSONB,
  skills_count INT,
  top_skills JSONB
);
