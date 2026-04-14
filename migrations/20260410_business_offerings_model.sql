alter table if exists public.shops
  add column if not exists business_type text,
  add column if not exists location_mode text,
  add column if not exists service_area text;

alter table if exists public.products
  add column if not exists offering_type text,
  add column if not exists price_mode text,
  add column if not exists availability_mode text,
  add column if not exists duration_minutes integer,
  add column if not exists capacity integer;

update public.shops
set business_type = coalesce(nullif(business_type, ''), 'retail')
where coalesce(business_type, '') = '';

update public.shops
set location_mode = coalesce(nullif(location_mode, ''), 'storefront')
where coalesce(location_mode, '') = '';

update public.shops
set service_area = coalesce(service_area, '')
where service_area is null;

update public.products
set offering_type = coalesce(nullif(offering_type, ''), 'product')
where coalesce(offering_type, '') = '';

update public.products
set price_mode = coalesce(nullif(price_mode, ''), 'fixed')
where coalesce(price_mode, '') = '';

update public.products
set availability_mode = coalesce(nullif(availability_mode, ''), 'in_stock')
where coalesce(availability_mode, '') = '';

alter table if exists public.shops
  drop constraint if exists shops_business_type_check;

alter table if exists public.shops
  add constraint shops_business_type_check
  check (business_type in ('retail', 'service', 'professional', 'education', 'creator', 'other'));

alter table if exists public.shops
  drop constraint if exists shops_location_mode_check;

alter table if exists public.shops
  add constraint shops_location_mode_check
  check (location_mode in ('storefront', 'service_area', 'hybrid', 'online'));

alter table if exists public.products
  drop constraint if exists products_offering_type_check;

alter table if exists public.products
  add constraint products_offering_type_check
  check (offering_type in ('product', 'service', 'class', 'event', 'portfolio', 'offering'));

alter table if exists public.products
  drop constraint if exists products_price_mode_check;

alter table if exists public.products
  add constraint products_price_mode_check
  check (price_mode in ('fixed', 'starting_at', 'inquiry', 'custom', 'free'));

alter table if exists public.products
  drop constraint if exists products_availability_mode_check;

alter table if exists public.products
  add constraint products_availability_mode_check
  check (availability_mode in ('in_stock', 'available', 'scheduled', 'limited', 'on_request', 'unavailable'));
