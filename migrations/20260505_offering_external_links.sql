alter table if exists public.products
  add column if not exists external_links jsonb;

update public.products
set external_links = '[]'::jsonb
where external_links is null;

alter table if exists public.products
  alter column external_links set default '[]'::jsonb;

comment on column public.products.external_links is
  'Optional named external action links for this offering, such as booking pages, official product pages, forms, or external checkout.';
