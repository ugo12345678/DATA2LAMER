-- Modele v2 pour rendre la recuperation reglementaire auditable.
-- A executer apres 2026-04-22_regulations_geospatial_tables.sql.

create table if not exists public.reg_extraction_runs (
  id uuid primary key default gen_random_uuid(),
  status text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  source_count int not null default 0,
  document_count int not null default 0,
  candidate_count int not null default 0,
  published_rule_count int not null default 0,
  quality_issue_count int not null default 0,
  ai_audit_status text,
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.reg_source_documents (
  id uuid primary key default gen_random_uuid(),
  extraction_run_id uuid references public.reg_extraction_runs(id) on delete set null,
  document_hash text not null unique,
  source_url text not null,
  canonical_url text,
  source_type text,
  authority_name text,
  title text,
  document_type text,
  content_length int,
  fetched_at timestamptz,
  checked_at timestamptz not null default now(),
  extraction_status text not null default 'ok',
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.reg_document_chunks (
  id uuid primary key default gen_random_uuid(),
  source_document_id uuid not null references public.reg_source_documents(id) on delete cascade,
  chunk_index int not null,
  chunk_hash text not null unique,
  text_excerpt text not null,
  token_estimate int,
  page_number int,
  locator text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique(source_document_id, chunk_index)
);

alter table public.reg_rules
  add column if not exists status text not null default 'published',
  add column if not exists confidence_score numeric,
  add column if not exists valid_from date,
  add column if not exists valid_to date,
  add column if not exists published_at timestamptz,
  add column if not exists superseded_by_rule_id uuid references public.reg_rules(id) on delete set null,
  add column if not exists activity_type text,
  add column if not exists constraint_type text;

alter table public.reg_rule_zones
  add column if not exists jurisdiction_level text,
  add column if not exists geometry_geojson jsonb,
  add column if not exists geometry_source_url text;

create table if not exists public.reg_rule_candidates (
  id uuid primary key default gen_random_uuid(),
  extraction_run_id uuid references public.reg_extraction_runs(id) on delete set null,
  source_document_id uuid references public.reg_source_documents(id) on delete set null,
  rule_key text not null,
  candidate_key text not null unique,
  rule_type text not null,
  activity_type text,
  constraint_type text,
  title text not null,
  description text not null,
  extracted_payload jsonb not null,
  status text not null default 'candidate',
  confidence_score numeric,
  needs_manual_review boolean not null default true,
  quality_flags text[] not null default array[]::text[],
  ai_audit jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.reg_rule_citations (
  id uuid primary key default gen_random_uuid(),
  rule_id uuid references public.reg_rules(id) on delete cascade,
  candidate_id uuid references public.reg_rule_candidates(id) on delete cascade,
  source_document_id uuid references public.reg_source_documents(id) on delete set null,
  source_url text not null,
  source_title text,
  authority_name text,
  quote text not null,
  page_number int,
  locator text,
  document_hash text,
  confidence_score numeric,
  created_at timestamptz not null default now()
);

create table if not exists public.reg_species (
  id uuid primary key default gen_random_uuid(),
  canonical_name text not null unique,
  scientific_name text,
  taxon_group text,
  aliases text[] not null default array[]::text[],
  external_ids jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.reg_rule_species (
  rule_id uuid not null references public.reg_rules(id) on delete cascade,
  species_id uuid not null references public.reg_species(id) on delete cascade,
  relation_type text not null default 'applies_to',
  created_at timestamptz not null default now(),
  primary key (rule_id, species_id, relation_type)
);

create index if not exists reg_extraction_runs_started_at_idx on public.reg_extraction_runs(started_at desc);
create index if not exists reg_source_documents_url_idx on public.reg_source_documents(source_url);
create index if not exists reg_document_chunks_source_idx on public.reg_document_chunks(source_document_id);
create index if not exists reg_rule_candidates_status_idx on public.reg_rule_candidates(status);
create index if not exists reg_rule_candidates_rule_key_idx on public.reg_rule_candidates(rule_key);
create index if not exists reg_rule_citations_rule_idx on public.reg_rule_citations(rule_id);
create index if not exists reg_rule_citations_candidate_idx on public.reg_rule_citations(candidate_id);
create index if not exists reg_species_scientific_name_idx on public.reg_species(scientific_name);
create index if not exists reg_rules_status_idx on public.reg_rules(status);
create index if not exists reg_rules_validity_idx on public.reg_rules(valid_from, valid_to);
