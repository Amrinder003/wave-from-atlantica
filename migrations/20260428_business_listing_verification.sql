alter table if exists public.shops
  add column if not exists listing_status text,
  add column if not exists owner_contact_name text,
  add column if not exists verification_method text,
  add column if not exists verification_evidence text,
  add column if not exists verification_submitted_at timestamptz,
  add column if not exists verified_at timestamptz,
  add column if not exists verification_rejection_reason text;

update public.shops
set listing_status = 'verified'
where coalesce(listing_status, '') = '';

alter table if exists public.shops
  alter column listing_status set default 'draft';

create index if not exists shops_listing_status_created_at_idx
  on public.shops(listing_status, created_at desc);
