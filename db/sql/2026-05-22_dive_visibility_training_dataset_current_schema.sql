-- VU2LAMER application database.
-- Compatibility view for the dive visibility training dataset using the
-- current dive log schema.

begin;

drop view if exists public.dive_visibility_dataset_hourly;
drop view if exists public.dive_visibility_training_dataset;

do $$
begin
  if to_regclass('public.dives') is not null
     and to_regclass('public.dive_spots') is not null
     and to_regclass('public.dive_spot_images') is not null
     and to_regclass('public.spots') is not null
     and to_regclass('public.environment_forecasts') is not null then
    execute $view$
      create or replace view public.dive_visibility_training_dataset as
      with observations as (
        select
          ds.id,
          ds.dive_id,
          case
            when ds.spot_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              then ds.spot_id::uuid
            else d.spot_id
          end as forecast_spot_id,
          ds.label,
          ds.estimated_visibility,
          ds.visited_at,
          ds.latitude,
          ds.longitude,
          ds.created_at,
          d.updated_at,
          d.cover_image_url
        from public.dive_spots ds
        join public.dives d
          on d.id = ds.dive_id
        where ds.visited_at is not null
          and ds.estimated_visibility is not null
      )
      select
        o.id::text as outing_id,
        o.dive_id::text as dive_id,
        o.id::text as dive_spot_id,
        o.forecast_spot_id::text as spot_id,
        o.label as spot_label,
        null::text as sector_id,
        coalesce(o.longitude, (s.longitude_min + s.longitude_max) / 2.0)::double precision as longitude,
        coalesce(o.latitude, (s.latitude_min + s.latitude_max) / 2.0)::double precision as latitude,
        o.visited_at as observed_at,
        o.updated_at as outing_updated_at,
        o.estimated_visibility::double precision as observed_visibility_m,
        coalesce(img.image_url, o.cover_image_url) as visibility_image_url,
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
        ef.surface_pressure_hpa,
        ef.cloud_cover_pct,
        ef.cloud_cover_low_pct,
        ef.cloud_cover_mid_pct,
        ef.cloud_cover_high_pct,
        ef.precipitation_mm,
        ef.weather_visibility_m,
        ef.wave_height_m,
        ef.wave_period_s,
        ef.wave_direction_deg,
        ef.wind_wave_height_m,
        ef.wind_wave_period_s,
        ef.wind_wave_direction_deg,
        ef.swell_wave_height_m,
        ef.swell_wave_period_s,
        ef.swell_wave_direction_deg,
        ef.secondary_swell_wave_height_m,
        ef.secondary_swell_wave_period_s,
        ef.secondary_swell_wave_direction_deg,
        ef.water_temperature_c,
        ef.sea_level_height_m,
        ef.tide_coefficient,
        ef.current_speed_ms,
        ef.current_direction_deg,
        ef.salinity_psu,
        ef.chlorophyll_mg_m3,
        ef.phytoplankton_carbon_mmol_m3,
        ef.net_primary_production_mg_m3_day,
        ef.euphotic_depth_m,
        ef.algal_bloom_risk,
        ef.light_attenuation_m1
      from observations o
      join public.spots s
        on s.id = o.forecast_spot_id
      left join lateral (
        select image_url
        from public.dive_spot_images img
        where img.dive_spot_id = o.id
        order by img.use_for_visibility desc, img.position asc, img.created_at asc
        limit 1
      ) img on true
      join public.environment_forecasts ef
        on ef.spot_id = o.forecast_spot_id
       and date_trunc('hour', ef.valid_time) = date_trunc('hour', o.visited_at)
      where o.forecast_spot_id is not null
        and coalesce(o.latitude, (s.latitude_min + s.latitude_max) / 2.0) is not null
        and coalesce(o.longitude, (s.longitude_min + s.longitude_max) / 2.0) is not null
    $view$;

    execute $view$
      create or replace view public.dive_visibility_dataset_hourly as
      select *
      from public.dive_visibility_training_dataset
    $view$;
  end if;
end;
$$;

commit;
