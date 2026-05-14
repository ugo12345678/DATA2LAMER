-- Supprime les associations materialisees entre reglementations et entites applicatives.
-- Les regles et leurs zones reglementaires restent dans reg_rules/reg_rule_zones.
-- A executer une fois dans la base Supabase apres les migrations reglementaires precedentes.

begin;

drop table if exists public.reg_spot_assignments;
drop table if exists public.reg_zone_assignments;

commit;
