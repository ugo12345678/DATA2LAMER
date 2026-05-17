-- VU2LAMER application database.
-- Single app-facing forecast table: one consolidated hourly row per catalog spot.

begin;

create extension if not exists pgcrypto;

drop view if exists public.dive_visibility_dataset;
drop table if exists public.forecast_predictions;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.environment_forecasts (
  id uuid primary key default gen_random_uuid(),
  spot_id uuid not null references public.spots(id) on delete cascade,
  valid_time timestamptz not null,
  target_date date not null,
  forecast_run_at timestamptz not null,
  forecast_horizon_hours integer,

  source_count integer not null default 0,
  sources text[] not null default '{}',
  provenance jsonb not null default '{}'::jsonb,

  wind_speed_ms double precision,
  wind_gusts_ms double precision,
  wind_direction_deg double precision,

  air_temperature_c double precision,
  relative_humidity_pct double precision,
  dew_point_c double precision,
  pressure_msl_hpa double precision,
  surface_pressure_hpa double precision,
  cloud_cover_pct double precision,
  cloud_cover_low_pct double precision,
  cloud_cover_mid_pct double precision,
  cloud_cover_high_pct double precision,
  precipitation_mm double precision,
  weather_visibility_m double precision,

  wave_height_m double precision,
  wave_period_s double precision,
  wave_direction_deg double precision,
  wind_wave_height_m double precision,
  wind_wave_period_s double precision,
  wind_wave_direction_deg double precision,
  swell_wave_height_m double precision,
  swell_wave_period_s double precision,
  swell_wave_direction_deg double precision,
  secondary_swell_wave_height_m double precision,
  secondary_swell_wave_period_s double precision,
  secondary_swell_wave_direction_deg double precision,

  water_temperature_c double precision,
  sea_level_height_m double precision,
  tide_coefficient double precision,
  current_speed_ms double precision,
  current_direction_deg double precision,
  salinity_psu double precision,
  chlorophyll_mg_m3 double precision,
  light_attenuation_m1 double precision,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint environment_forecasts_source_count_check check (source_count >= 0),
  constraint environment_forecasts_wind_direction_check
    check (wind_direction_deg is null or (wind_direction_deg >= 0 and wind_direction_deg < 360)),
  constraint environment_forecasts_wave_direction_check
    check (wave_direction_deg is null or (wave_direction_deg >= 0 and wave_direction_deg < 360)),
  constraint environment_forecasts_current_direction_check
    check (current_direction_deg is null or (current_direction_deg >= 0 and current_direction_deg < 360))
);

alter table public.environment_forecasts
  add column if not exists target_date date,
  add column if not exists forecast_run_at timestamptz,
  add column if not exists forecast_horizon_hours integer,
  add column if not exists source_count integer not null default 0,
  add column if not exists sources text[] not null default '{}',
  add column if not exists provenance jsonb not null default '{}'::jsonb,
  add column if not exists wind_speed_ms double precision,
  add column if not exists wind_gusts_ms double precision,
  add column if not exists wind_direction_deg double precision,
  add column if not exists air_temperature_c double precision,
  add column if not exists relative_humidity_pct double precision,
  add column if not exists dew_point_c double precision,
  add column if not exists pressure_msl_hpa double precision,
  add column if not exists surface_pressure_hpa double precision,
  add column if not exists cloud_cover_pct double precision,
  add column if not exists cloud_cover_low_pct double precision,
  add column if not exists cloud_cover_mid_pct double precision,
  add column if not exists cloud_cover_high_pct double precision,
  add column if not exists precipitation_mm double precision,
  add column if not exists weather_visibility_m double precision,
  add column if not exists wave_height_m double precision,
  add column if not exists wave_period_s double precision,
  add column if not exists wave_direction_deg double precision,
  add column if not exists wind_wave_height_m double precision,
  add column if not exists wind_wave_period_s double precision,
  add column if not exists wind_wave_direction_deg double precision,
  add column if not exists swell_wave_height_m double precision,
  add column if not exists swell_wave_period_s double precision,
  add column if not exists swell_wave_direction_deg double precision,
  add column if not exists secondary_swell_wave_height_m double precision,
  add column if not exists secondary_swell_wave_period_s double precision,
  add column if not exists secondary_swell_wave_direction_deg double precision,
  add column if not exists water_temperature_c double precision,
  add column if not exists sea_level_height_m double precision,
  add column if not exists tide_coefficient double precision,
  add column if not exists current_speed_ms double precision,
  add column if not exists current_direction_deg double precision,
  add column if not exists salinity_psu double precision,
  add column if not exists chlorophyll_mg_m3 double precision,
  add column if not exists light_attenuation_m1 double precision,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

alter table public.environment_forecasts
  drop constraint if exists environment_forecasts_spot_id_fkey;

delete from public.environment_forecasts ef
where not exists (
  select 1
  from public.spots s
  where s.id = ef.spot_id
);

alter table public.environment_forecasts
  add constraint environment_forecasts_spot_id_fkey
  foreign key (spot_id) references public.spots(id) on delete cascade;

create unique index if not exists idx_environment_forecasts_spot_time
  on public.environment_forecasts (spot_id, valid_time);

create index if not exists idx_environment_forecasts_target_date
  on public.environment_forecasts (target_date);

create index if not exists idx_environment_forecasts_valid_time
  on public.environment_forecasts (valid_time);

drop trigger if exists set_environment_forecasts_updated_at on public.environment_forecasts;
create trigger set_environment_forecasts_updated_at
before update on public.environment_forecasts
for each row execute function public.set_updated_at();

comment on table public.environment_forecasts is
  'Consolidated hourly weather and marine forecasts for the VU2LAMER app. Raw source data lives in DATA2LAMER.';
comment on column public.environment_forecasts.provenance is
  'Per-metric source trace used for consolidation: source codes, values, units, model/grid metadata and run ids.';

do $$
begin
  if to_regclass('public.dive_outings') is not null then
    execute $view$
      create or replace view public.dive_visibility_dataset_hourly as
      select
        o.id as outing_id,
        o.spot_id,
        ds.name as spot_name,
        o.outing_date,
        o.observed_visibility_m,
        ef.valid_time,
        ef.forecast_run_at,
        ef.forecast_horizon_hours,
        ef.sources,
        ef.provenance,
        ef.wind_speed_ms,
        ef.wind_gusts_ms,
        ef.wind_direction_deg,
        ef.air_temperature_c,
        ef.relative_humidity_pct,
        ef.dew_point_c,
        ef.pressure_msl_hpa,
        ef.cloud_cover_pct,
        ef.cloud_cover_low_pct,
        ef.cloud_cover_mid_pct,
        ef.cloud_cover_high_pct,
        ef.precipitation_mm,
        ef.weather_visibility_m,
        ef.wave_height_m,
        ef.wave_period_s,
        ef.wave_direction_deg,
        ef.swell_wave_height_m,
        ef.swell_wave_period_s,
        ef.swell_wave_direction_deg,
        ef.water_temperature_c,
        ef.sea_level_height_m,
        ef.tide_coefficient,
        ef.current_speed_ms,
        ef.current_direction_deg,
        ef.salinity_psu,
        ef.chlorophyll_mg_m3,
        ef.light_attenuation_m1
      from public.dive_outings o
      left join public.spots ds
        on ds.id = o.spot_id
      left join public.environment_forecasts ef
        on ef.spot_id = o.spot_id
       and ef.target_date = o.outing_date
    $view$;
  end if;
end;
$$;

commit;
