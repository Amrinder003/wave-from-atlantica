create table if not exists public.business_claims (
  claim_id text primary key,
  shop_id text not null references public.shops(shop_id) on delete cascade,
  claimant_user_id text not null,
  claimant_display_name text not null default '',
  claimant_email text not null default '',
  note text not null default '',
  status text not null default 'pending',
  review_note text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists business_claims_claimant_status_idx
  on public.business_claims(claimant_user_id, status, created_at desc);

create index if not exists business_claims_shop_status_idx
  on public.business_claims(shop_id, status, created_at desc);

create unique index if not exists business_claims_pending_unique_idx
  on public.business_claims(shop_id, claimant_user_id)
  where status = 'pending';
