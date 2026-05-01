-- Suivi incremental et historique des versions de regles.
-- A executer apres 2026-04-29_regulations_audit_model_v2.sql.

alter table public.reg_source_documents
  add column if not exists etag text,
  add column if not exists last_modified text,
  add column if not exists content_hash text,
  add column if not exists raw_storage_path text,
  add column if not exists first_seen_at timestamptz not null default now(),
  add column if not exists last_seen_at timestamptz,
  add column if not exists last_checked_at timestamptz;

create table if not exists public.reg_source_candidates (
  id uuid primary key default gen_random_uuid(),
  candidate_key text not null unique,
  source_url text not null,
  canonical_url text,
  source_type text,
  authority_name text,
  title text,
  kind text,
  status text not null default 'candidate',
  discovery_score numeric,
  matched_keywords text[] not null default array[]::text[],
  discovery_method text,
  seed_url text,
  ai_relevant boolean,
  ai_confidence numeric,
  ai_reason text,
  payload jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.reg_rule_versions (
  id uuid primary key default gen_random_uuid(),
  rule_id uuid references public.reg_rules(id) on delete set null,
  rule_key text not null,
  rule_fingerprint text not null,
  source_document_id uuid references public.reg_source_documents(id) on delete set null,
  document_hash text,
  extraction_run_id uuid references public.reg_extraction_runs(id) on delete set null,
  title text not null,
  description text not null,
  valid_from date,
  valid_to date,
  status text not null default 'candidate',
  confidence_score numeric,
  extracted_payload jsonb not null default '{}'::jsonb,
  citations jsonb not null default '[]'::jsonb,
  observed_from timestamptz not null default now(),
  observed_to timestamptz,
  last_seen_at timestamptz not null default now(),
  last_seen_run_id uuid references public.reg_extraction_runs(id) on delete set null,
  is_current boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(rule_key, rule_fingerprint)
);

create index if not exists reg_source_documents_content_hash_idx on public.reg_source_documents(content_hash);
create index if not exists reg_source_documents_raw_storage_path_idx on public.reg_source_documents(raw_storage_path);
create index if not exists reg_source_documents_last_checked_idx on public.reg_source_documents(last_checked_at desc);
create index if not exists reg_source_candidates_status_idx on public.reg_source_candidates(status);
create index if not exists reg_source_candidates_url_idx on public.reg_source_candidates(source_url);
create index if not exists reg_rule_versions_rule_key_idx on public.reg_rule_versions(rule_key);
create index if not exists reg_rule_versions_current_idx on public.reg_rule_versions(rule_key, is_current);
create index if not exists reg_rule_versions_document_hash_idx on public.reg_rule_versions(document_hash);
