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
  add column if not exists longitude double precision;

alter table if exists public.products
  add column if not exists price_amount numeric(12,2),
  add column if not exists stock_quantity integer,
  add column if not exists variant_data jsonb,
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
