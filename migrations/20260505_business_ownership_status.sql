alter table public.shops
  add column if not exists listing_source text,
  add column if not exists ownership_status text,
  add column if not exists claimed_at timestamptz;

update public.shops
set listing_source = 'platform_import',
    ownership_status = 'platform_managed'
where lower(trim(coalesce(owner_contact_name, ''))) = 'platform imported listing';

update public.shops
set listing_source = 'owner_created'
where coalesce(listing_source, '') = '';

update public.shops
set ownership_status = 'claimed'
where coalesce(ownership_status, '') = '';

create index if not exists shops_ownership_status_created_at_idx
  on public.shops(ownership_status, created_at desc);
