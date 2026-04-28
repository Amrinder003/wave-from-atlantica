alter table if exists public.shops
  add column if not exists trust_flags jsonb not null default '[]'::jsonb,
  add column if not exists risk_score integer not null default 0,
  add column if not exists risk_level text not null default 'low';

update public.shops
set trust_flags = '[]'::jsonb
where trust_flags is null;

update public.shops
set risk_score = 0
where risk_score is null;

update public.shops
set risk_level = 'low'
where coalesce(risk_level, '') = '';
