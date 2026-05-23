-- VU2LAMER application database.
-- Single app-facing forecast table: one consolidated hourly row per catalog spot.

begin;

create extension if not exists pgcrypto;

drop view if exists public.dive_visibility_dataset;
drop view if exists public.dive_visibility_dataset_hourly;
drop view if exists public.dive_visibility_training_dataset;
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
  tide_coefficient_approx double precision,
  tide_min_height_m double precision,
  tide_max_height_m double precision,
  tide_range_m double precision,
  tide_phase text,
  next_tide_event_type text,
  next_tide_event_time timestamptz,
  next_tide_event_height_m double precision,
  current_speed_ms double precision,
  current_direction_deg double precision,
  salinity_psu double precision,
  chlorophyll_mg_m3 double precision,
  phytoplankton_carbon_mmol_m3 double precision,
  net_primary_production_mg_m3_day double precision,
  euphotic_depth_m double precision,
  algal_bloom_risk double precision,
  light_attenuation_m1 double precision,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint environment_forecasts_source_count_check check (source_count >= 0),
  constraint environment_forecasts_wind_direction_check
    check (wind_direction_deg is null or (wind_direction_deg >= 0 and wind_direction_deg < 360)),
  constraint environment_forecasts_wave_direction_check
    check (wave_direction_deg is null or (wave_direction_deg >= 0 and wave_direction_deg < 360)),
  constraint environment_forecasts_current_direction_check
    check (current_direction_deg is null or (current_direction_deg >= 0 and current_direction_deg < 360)),
  constraint environment_forecasts_tide_phase_check
    check (tide_phase is null or tide_phase in ('rising', 'falling', 'slack')),
  constraint environment_forecasts_next_tide_event_type_check
    check (next_tide_event_type is null or next_tide_event_type in ('high', 'low'))
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
  add column if not exists tide_coefficient_approx double precision,
  add column if not exists tide_min_height_m double precision,
  add column if not exists tide_max_height_m double precision,
  add column if not exists tide_range_m double precision,
  add column if not exists tide_phase text,
  add column if not exists next_tide_event_type text,
  add column if not exists next_tide_event_time timestamptz,
  add column if not exists next_tide_event_height_m double precision,
  add column if not exists current_speed_ms double precision,
  add column if not exists current_direction_deg double precision,
  add column if not exists salinity_psu double precision,
  add column if not exists chlorophyll_mg_m3 double precision,
  add column if not exists phytoplankton_carbon_mmol_m3 double precision,
  add column if not exists net_primary_production_mg_m3_day double precision,
  add column if not exists euphotic_depth_m double precision,
  add column if not exists algal_bloom_risk double precision,
  add column if not exists light_attenuation_m1 double precision,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

alter table public.environment_forecasts
  drop constraint if exists environment_forecasts_spot_id_fkey;

alter table public.environment_forecasts
  drop constraint if exists environment_forecasts_tide_phase_check,
  drop constraint if exists environment_forecasts_next_tide_event_type_check,
  add constraint environment_forecasts_tide_phase_check
    check (tide_phase is null or tide_phase in ('rising', 'falling', 'slack')),
  add constraint environment_forecasts_next_tide_event_type_check
    check (next_tide_event_type is null or next_tide_event_type in ('high', 'low'));

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
declare
  outing_time_col text;
  outing_updated_col text;
  visibility_col text;
  image_col text;
  spot_lat_col text;
  spot_lon_col text;
  spot_sector_col text;
  sector_rel text;
  sector_lat_col text;
  sector_lon_col text;
  sector_id_expr text;
  sector_join text := '';
  latitude_expr text;
  longitude_expr text;
  observed_at_expr text;
  outing_updated_expr text;
  visibility_expr text;
  image_expr text;
begin
  if to_regclass('public.dive_outings') is not null
     and to_regclass('public.spots') is not null
     and exists (
       select 1
       from information_schema.columns
       where table_schema = 'public'
         and table_name = 'dive_outings'
         and column_name = 'spot_id'
     ) then
    select column_name into outing_time_col
    from unnest(array['observed_at', 'dive_at', 'started_at', 'start_time', 'outing_at', 'outing_date']) as candidate(column_name)
    where exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'dive_outings'
        and column_name = candidate.column_name
    )
    limit 1;

    select column_name into visibility_col
    from unnest(array['observed_visibility_m', 'visibility_m', 'water_visibility_m', 'visibility']) as candidate(column_name)
    where exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'dive_outings'
        and column_name = candidate.column_name
    )
    limit 1;

    select column_name into outing_updated_col
    from unnest(array['updated_at', 'modified_at', 'last_modified_at']) as candidate(column_name)
    where exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'dive_outings'
        and column_name = candidate.column_name
    )
    limit 1;

    select column_name into image_col
    from unnest(array['visibility_image_url', 'image_visibility_url', 'visibility_image', 'image_visibility', 'photo_url']) as candidate(column_name)
    where exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'dive_outings'
        and column_name = candidate.column_name
    )
    limit 1;

    select column_name into spot_lat_col
    from unnest(array['latitude', 'lat']) as candidate(column_name)
    where exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'spots'
        and column_name = candidate.column_name
    )
    limit 1;

    select column_name into spot_lon_col
    from unnest(array['longitude', 'lon', 'lng']) as candidate(column_name)
    where exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'spots'
        and column_name = candidate.column_name
    )
    limit 1;

    select column_name into spot_sector_col
    from unnest(array['sector_id', 'zone_id']) as candidate(column_name)
    where exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'spots'
        and column_name = candidate.column_name
    )
    limit 1;

    select rel_name into sector_rel
    from unnest(array['sectors', 'spot_sectors', 'dive_sectors']) as candidate(rel_name)
    where to_regclass('public.' || candidate.rel_name) is not null
    limit 1;

    if sector_rel is not null then
      select column_name into sector_lat_col
      from unnest(array['latitude', 'lat']) as candidate(column_name)
      where exists (
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = sector_rel
          and column_name = candidate.column_name
      )
      limit 1;

      select column_name into sector_lon_col
      from unnest(array['longitude', 'lon', 'lng']) as candidate(column_name)
      where exists (
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = sector_rel
          and column_name = candidate.column_name
      )
      limit 1;
    end if;

    if outing_time_col is not null and visibility_col is not null then
      observed_at_expr := format('o.%I::timestamptz', outing_time_col);
      outing_updated_expr := case
        when outing_updated_col is not null then format('o.%I::timestamptz', outing_updated_col)
        else 'null::timestamptz'
      end;
      visibility_expr := format('o.%I::double precision', visibility_col);
      image_expr := case
        when image_col is not null then format('o.%I::text', image_col)
        else 'null::text'
      end;

      if exists (
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = 'dive_outings'
          and column_name = 'sector_id'
      ) then
        sector_id_expr := 'o.sector_id::text';
        if sector_rel is not null then
          sector_join := format('left join public.%I sec on sec.id = o.sector_id', sector_rel);
        end if;
      elsif spot_sector_col is not null then
        sector_id_expr := format('s.%I::text', spot_sector_col);
        if sector_rel is not null then
          sector_join := format('left join public.%I sec on sec.id = s.%I', sector_rel, spot_sector_col);
        end if;
      else
        sector_id_expr := 'null::text';
      end if;

      latitude_expr := case
        when spot_lat_col is not null and sector_lat_col is not null then format('coalesce(s.%I, sec.%I)::double precision', spot_lat_col, sector_lat_col)
        when spot_lat_col is not null then format('s.%I::double precision', spot_lat_col)
        when sector_lat_col is not null then format('sec.%I::double precision', sector_lat_col)
        else 'null::double precision'
      end;

      longitude_expr := case
        when spot_lon_col is not null and sector_lon_col is not null then format('coalesce(s.%I, sec.%I)::double precision', spot_lon_col, sector_lon_col)
        when spot_lon_col is not null then format('s.%I::double precision', spot_lon_col)
        when sector_lon_col is not null then format('sec.%I::double precision', sector_lon_col)
        else 'null::double precision'
      end;

    execute $view$
      create or replace view public.dive_visibility_training_dataset as
      select
        o.id::text as outing_id,
        o.spot_id::text as spot_id,
        $view$ || sector_id_expr || $view$ as sector_id,
        $view$ || longitude_expr || $view$ as longitude,
        $view$ || latitude_expr || $view$ as latitude,
        $view$ || observed_at_expr || $view$ as observed_at,
        $view$ || outing_updated_expr || $view$ as outing_updated_at,
        $view$ || visibility_expr || $view$ as observed_visibility_m,
        $view$ || image_expr || $view$ as visibility_image_url,
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
        ef.tide_coefficient_approx,
        ef.tide_min_height_m,
        ef.tide_max_height_m,
        ef.tide_range_m,
        ef.tide_phase,
        ef.next_tide_event_type,
        ef.next_tide_event_time,
        ef.next_tide_event_height_m,
        ef.current_speed_ms,
        ef.current_direction_deg,
        ef.salinity_psu,
        ef.chlorophyll_mg_m3,
        ef.phytoplankton_carbon_mmol_m3,
        ef.net_primary_production_mg_m3_day,
        ef.euphotic_depth_m,
        ef.algal_bloom_risk,
        ef.light_attenuation_m1
      from public.dive_outings o
      left join public.spots s
        on s.id = o.spot_id
      $view$ || sector_join || $view$
      join public.environment_forecasts ef
        on ef.spot_id = o.spot_id
       and date_trunc('hour', ef.valid_time) = date_trunc('hour', $view$ || observed_at_expr || $view$)
      where $view$ || observed_at_expr || $view$ is not null
        and $view$ || visibility_expr || $view$ is not null
        and $view$ || latitude_expr || $view$ is not null
        and $view$ || longitude_expr || $view$ is not null
    $view$;

      execute $view$
        create or replace view public.dive_visibility_dataset_hourly as
        select *
        from public.dive_visibility_training_dataset
      $view$;
    end if;
  end if;
end;
$$;

commit;
