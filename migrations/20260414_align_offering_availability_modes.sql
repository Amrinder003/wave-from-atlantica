update public.products
set availability_mode = 'unavailable'
where availability_mode = 'booked_out';

alter table if exists public.products
  drop constraint if exists products_availability_mode_check;

alter table if exists public.products
  add constraint products_availability_mode_check
  check (availability_mode in ('in_stock', 'available', 'scheduled', 'limited', 'on_request', 'unavailable'));
