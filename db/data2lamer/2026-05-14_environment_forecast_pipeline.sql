-- DATA2LAMER technical database.
-- Stores source catalog, sync runs and normalized source values.

begin;

create extension if not exists pgcrypto;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.environment_sources (
  code text primary key,
  name text not null,
  provider text not null,
  kind text not null,
  enabled boolean not null default true,
  attribution text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.environment_sync_runs (
  id uuid primary key default gen_random_uuid(),
  source_code text not null references public.environment_sources(code) on delete restrict,
  status text not null default 'running',
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  window_start timestamptz not null,
  window_end timestamptz not null,
  rows_count integer not null default 0,
  parameters jsonb not null default '{}'::jsonb,
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint environment_sync_runs_status_check
    check (status in ('running', 'success', 'failed', 'partial'))
);

create table if not exists public.forecast_source_values (
  id uuid primary key default gen_random_uuid(),
  run_id uuid references public.environment_sync_runs(id) on delete set null,
  source_code text not null references public.environment_sources(code) on delete restrict,
  spot_id uuid not null,
  valid_time timestamptz not null,
  metric text not null,
  value double precision,
  unit text not null,
  raw_variable text,
  model text,
  resolution_minutes integer,
  grid_lat double precision,
  grid_lon double precision,
  quality_flags jsonb not null default '{}'::jsonb,
  fetched_at timestamptz not null,
  created_at timestamptz not null default now()
);

create table if not exists public.spot_source_grid_points (
  id uuid primary key default gen_random_uuid(),
  source_code text not null references public.environment_sources(code) on delete cascade,
  spot_id uuid not null,
  grid_lat double precision,
  grid_lon double precision,
  model text,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  unique (source_code, spot_id, model, grid_lat, grid_lon)
);

create index if not exists idx_forecast_source_values_lookup
  on public.forecast_source_values (spot_id, valid_time, metric);

create index if not exists idx_forecast_source_values_source_time
  on public.forecast_source_values (source_code, valid_time);

create index if not exists idx_forecast_source_values_run
  on public.forecast_source_values (run_id);

drop trigger if exists set_environment_sources_updated_at on public.environment_sources;
create trigger set_environment_sources_updated_at
before update on public.environment_sources
for each row execute function public.set_updated_at();

drop trigger if exists set_environment_sync_runs_updated_at on public.environment_sync_runs;
create trigger set_environment_sync_runs_updated_at
before update on public.environment_sync_runs
for each row execute function public.set_updated_at();

insert into public.environment_sources (code, name, provider, kind, attribution)
values
  ('open_meteo_weather', 'Open-Meteo Weather Best Match', 'open-meteo', 'weather', 'Attribution required by Open-Meteo and upstream weather providers.'),
  ('open_meteo_meteofrance', 'Open-Meteo Météo-France AROME/ARPEGE', 'open-meteo', 'weather', 'Attribution required by Open-Meteo and Météo-France.'),
  ('open_meteo_dwd_icon', 'Open-Meteo DWD ICON', 'open-meteo', 'weather', 'Attribution required by Open-Meteo and DWD.'),
  ('open_meteo_gfs', 'Open-Meteo NOAA GFS', 'open-meteo', 'weather', 'Attribution required by Open-Meteo and NOAA/NCEP.'),
  ('metno_locationforecast', 'MET Norway Locationforecast', 'met-norway', 'weather', 'MET Norway Locationforecast. User-Agent identification required.'),
  ('open_meteo_marine', 'Open-Meteo Marine Best Match', 'open-meteo', 'marine', 'Attribution required by Open-Meteo and upstream marine providers.'),
  ('open_meteo_marine_meteofrance_wave', 'Open-Meteo Marine Météo-France Wave', 'open-meteo', 'marine', 'Attribution required by Open-Meteo and Météo-France.'),
  ('open_meteo_marine_meteofrance_currents', 'Open-Meteo Marine Météo-France Currents', 'open-meteo', 'marine', 'Attribution required by Open-Meteo and Météo-France.'),
  ('open_meteo_marine_meteofrance_sst', 'Open-Meteo Marine Météo-France Sea Surface Temperature', 'open-meteo', 'marine', 'Attribution required by Open-Meteo and Météo-France.'),
  ('open_meteo_marine_dwd_ewam', 'Open-Meteo Marine DWD EWAM', 'open-meteo', 'marine', 'Attribution required by Open-Meteo and DWD.'),
  ('open_meteo_marine_dwd_gwam', 'Open-Meteo Marine DWD GWAM', 'open-meteo', 'marine', 'Attribution required by Open-Meteo and DWD.'),
  ('open_meteo_marine_gfs_wave', 'Open-Meteo Marine NOAA GFS Wave', 'open-meteo', 'marine', 'Attribution required by Open-Meteo and NOAA/NCEP.'),
  ('cmems_ibi_wav', 'Copernicus Marine IBI Waves', 'copernicus-marine', 'marine', 'Copernicus Marine Service. Free account credentials required.'),
  ('cmems_ibi_phy', 'Copernicus Marine IBI Physical', 'copernicus-marine', 'marine', 'Copernicus Marine Service. Free account credentials required.'),
  ('cmems_ibi_bgc', 'Copernicus Marine IBI Biogeochemistry', 'copernicus-marine', 'marine', 'Copernicus Marine Service. Free account credentials required.')
on conflict (code) do update set
  name = excluded.name,
  provider = excluded.provider,
  kind = excluded.kind,
  attribution = excluded.attribution,
  updated_at = now();

comment on table public.forecast_source_values is
  'Normalized long-format source values used to consolidate hourly app forecasts.';

commit;
