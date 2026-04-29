alter table if exists public.shops
  add column if not exists phone_public boolean;

update public.shops
set phone_public = true
where phone_public is null
  and coalesce(phone, '') <> '';

update public.shops
set phone_public = false
where phone_public is null;

alter table if exists public.shops
  alter column phone_public set default false;

alter table if exists public.shops
  alter column phone_public set not null;
