alter table if exists public.shops
  add column if not exists formatted_address text,
  add column if not exists hours_structured jsonb,
  add column if not exists timezone_name text,
  add column if not exists country_code text,
  add column if not exists country_name text,
  add column if not exists region text,
  add column if not exists city text,
  add column if not exists postal_code text,
  add column if not exists street_line1 text,
  add column if not exists street_line2 text,
  add column if not exists currency_code text,
  add column if not exists latitude double precision,
  add column if not exists longitude double precision,
  add column if not exists supports_pickup boolean default true,
  add column if not exists supports_delivery boolean default false,
  add column if not exists supports_walk_in boolean default true,
  add column if not exists delivery_radius_km numeric(8,2),
  add column if not exists delivery_fee numeric(12,2),
  add column if not exists pickup_notes text;

alter table if exists public.products
  add column if not exists price_amount numeric(12,2),
  add column if not exists stock_quantity integer,
  add column if not exists variant_data jsonb,
  add column if not exists variant_matrix jsonb,
  add column if not exists attribute_data jsonb,
  add column if not exists currency_code text;

update public.shops
set formatted_address = coalesce(nullif(formatted_address, ''), address)
where coalesce(formatted_address, '') = '' and coalesce(address, '') <> '';

update public.shops
set currency_code = coalesce(nullif(currency_code, ''), 'USD')
where coalesce(currency_code, '') = '';

update public.products
set currency_code = coalesce(nullif(currency_code, ''), 'USD')
where coalesce(currency_code, '') = '';

update public.products
set price_amount = nullif(regexp_replace(coalesce(price, ''), '[^0-9.]', '', 'g'), '')::numeric
where price_amount is null
  and coalesce(price, '') <> ''
  and regexp_replace(coalesce(price, ''), '[^0-9.]', '', 'g') <> '';

create table if not exists public.order_requests (
  id bigserial primary key,
  request_id text unique,
  shop_id text not null,
  fulfillment_type text not null,
  customer_name text not null,
  phone text not null,
  customer_email text,
  note text,
  preferred_time text,
  delivery_address text,
  items jsonb not null default '[]'::jsonb,
  total_amount numeric(12,2),
  currency_code text,
  status text not null default 'new',
  status_history jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists order_requests_shop_id_created_at_idx
  on public.order_requests(shop_id, created_at desc);

alter table if exists public.order_requests
  add column if not exists customer_email text;
