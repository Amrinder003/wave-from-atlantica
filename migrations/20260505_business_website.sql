alter table if exists public.shops
  add column if not exists website text;

comment on column public.shops.website is
  'Optional public business website or external booking/platform URL shown on the business page.';
