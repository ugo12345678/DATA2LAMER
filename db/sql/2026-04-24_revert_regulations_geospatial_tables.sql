-- Annule la migration 2026-04-22_regulations_geospatial_tables.sql
-- A executer une fois dans la base Supabase (SQL editor).

begin;

drop table if exists public.reg_spot_assignments;
drop table if exists public.reg_zone_assignments;
drop table if exists public.reg_rule_zones;
drop table if exists public.reg_assignment_runs;
drop table if exists public.reg_rules;
drop table if exists public.reg_documents_sources;

commit;
