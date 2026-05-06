create table if not exists public.business_audit_events (
  event_id text primary key,
  shop_id text not null references public.shops(shop_id) on delete cascade,
  event_type text not null,
  actor_user_id text not null default '',
  actor_email text not null default '',
  actor_display_name text not null default '',
  actor_role text not null default '',
  summary text not null default '',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists business_audit_events_shop_created_idx
  on public.business_audit_events(shop_id, created_at desc);

create index if not exists business_audit_events_type_created_idx
  on public.business_audit_events(event_type, created_at desc);
