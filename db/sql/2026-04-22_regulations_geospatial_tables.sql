-- Tables de liaison reglementaire geospatiale pour spots et zones.
-- A executer une fois dans la base Supabase (SQL editor).

create table if not exists public.reg_documents_sources (
  id uuid primary key default gen_random_uuid(),
  source_type text not null,
  source_priority int not null,
  authority_name text,
  title text not null,
  source_url text not null unique,
  legal_reference text,
  effective_date date,
  fetched_at timestamptz not null,
  checked_at timestamptz not null,
  needs_manual_review boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.reg_rules (
  id uuid primary key default gen_random_uuid(),
  rule_key text not null unique,
  rule_type text not null,
  title text not null,
  description text not null,
  legal_reference text,
  metric_type text,
  metric_value numeric,
  metric_unit text,
  species_common_name text,
  species_scientific_name text,
  source_document_id uuid not null references public.reg_documents_sources(id) on delete cascade,
  source_priority int not null,
  effective_date date,
  fetched_at timestamptz not null,
  checked_at timestamptz not null,
  needs_manual_review boolean not null default false,
  is_geospatial boolean not null default true,
  metadata jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.reg_rule_zones (
  id uuid primary key default gen_random_uuid(),
  rule_id uuid not null references public.reg_rules(id) on delete cascade,
  zone_code text not null,
  zone_name text not null,
  lat_min double precision not null,
  lat_max double precision not null,
  lon_min double precision not null,
  lon_max double precision not null,
  checked_at timestamptz not null,
  fetched_at timestamptz not null,
  needs_manual_review boolean not null default false,
  metadata jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(rule_id, zone_code)
);

create table if not exists public.reg_spot_assignments (
  id uuid primary key default gen_random_uuid(),
  spot_id uuid not null references public.spots(id) on delete cascade,
  rule_id uuid not null references public.reg_rules(id) on delete cascade,
  reg_zone_id uuid not null references public.reg_rule_zones(id) on delete cascade,
  app_zone_id uuid references public.zones(id) on delete set null,
  source_url text not null,
  source_priority int not null,
  match_type text not null,
  assigned_at timestamptz not null,
  checked_at timestamptz not null,
  fetched_at timestamptz not null,
  needs_manual_review boolean not null default false,
  metadata jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(spot_id, rule_id)
);

create table if not exists public.reg_zone_assignments (
  id uuid primary key default gen_random_uuid(),
  app_zone_id uuid not null references public.zones(id) on delete cascade,
  rule_id uuid not null references public.reg_rules(id) on delete cascade,
  reg_zone_id uuid not null references public.reg_rule_zones(id) on delete cascade,
  source_url text not null,
  source_priority int not null,
  match_type text not null,
  assigned_at timestamptz not null,
  checked_at timestamptz not null,
  fetched_at timestamptz not null,
  needs_manual_review boolean not null default false,
  metadata jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(app_zone_id, rule_id)
);

create table if not exists public.reg_assignment_runs (
  id uuid primary key default gen_random_uuid(),
  status text not null,
  started_at timestamptz not null,
  finished_at timestamptz,
  spots_count int not null default 0,
  zones_count int not null default 0,
  rules_count int not null default 0,
  spot_assignments_count int not null default 0,
  zone_assignments_count int not null default 0,
  warning_count int not null default 0,
  error_message text,
  metadata jsonb,
  created_at timestamptz not null default now()
);

create index if not exists reg_rules_checked_at_idx on public.reg_rules(checked_at desc);
create index if not exists reg_spot_assignments_spot_idx on public.reg_spot_assignments(spot_id);
create index if not exists reg_spot_assignments_app_zone_idx on public.reg_spot_assignments(app_zone_id);
create index if not exists reg_zone_assignments_zone_idx on public.reg_zone_assignments(app_zone_id);
create index if not exists reg_rule_zones_bbox_idx on public.reg_rule_zones(lat_min, lat_max, lon_min, lon_max);
