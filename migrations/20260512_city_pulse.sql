create table if not exists public.city_pulse_batches (
  batch_id text primary key,
  city_key text not null,
  city text not null,
  region text default '',
  country_code text default '',
  country_name text default '',
  center_lat numeric,
  center_lng numeric,
  provider text not null default 'gdelt',
  status text not null default 'ready',
  refreshed_at timestamptz not null default now(),
  fresh_until timestamptz not null,
  stale_until timestamptz,
  article_count integer not null default 0,
  card_count integer not null default 0,
  model_used text default '',
  error_message text default '',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists city_pulse_batches_city_key_refreshed_idx
  on public.city_pulse_batches(city_key, refreshed_at desc);

create index if not exists city_pulse_batches_city_key_fresh_until_idx
  on public.city_pulse_batches(city_key, fresh_until desc);

create table if not exists public.city_pulse_cards (
  card_id text primary key,
  batch_id text not null references public.city_pulse_batches(batch_id) on delete cascade,
  city_key text not null,
  rank integer not null default 0,
  category text not null default 'news',
  hook_title text not null,
  headline text not null default '',
  brief text not null default '',
  location_label text not null default '',
  latitude numeric,
  longitude numeric,
  location_precision text not null default 'city',
  location_confidence numeric not null default 0,
  importance_score numeric not null default 0,
  published_at timestamptz,
  source_count integer not null default 0,
  sources jsonb not null default '[]'::jsonb,
  article_fingerprints text[] not null default '{}',
  metadata jsonb not null default '{}'::jsonb,
  visible boolean not null default true,
  created_at timestamptz not null default now()
);

create index if not exists city_pulse_cards_city_rank_idx
  on public.city_pulse_cards(city_key, visible, rank);

create index if not exists city_pulse_cards_batch_rank_idx
  on public.city_pulse_cards(batch_id, rank);

create table if not exists public.city_pulse_sources (
  source_id text primary key,
  card_id text not null references public.city_pulse_cards(card_id) on delete cascade,
  batch_id text not null references public.city_pulse_batches(batch_id) on delete cascade,
  publisher text not null default '',
  title text not null default '',
  url text not null,
  published_at timestamptz,
  language text default '',
  source_country text default '',
  created_at timestamptz not null default now(),
  unique(card_id, url)
);

create index if not exists city_pulse_sources_card_idx
  on public.city_pulse_sources(card_id);

create index if not exists city_pulse_sources_batch_idx
  on public.city_pulse_sources(batch_id);
