create index if not exists products_shop_id_updated_at_idx
  on public.products(shop_id, updated_at desc);

create index if not exists products_shop_id_stock_updated_at_idx
  on public.products(shop_id, stock, updated_at desc);

create index if not exists products_updated_at_idx
  on public.products(updated_at desc);

create index if not exists products_stock_updated_at_idx
  on public.products(stock, updated_at desc);

create index if not exists analytics_shop_event_created_at_idx
  on public.analytics(shop_id, event, created_at desc);

create index if not exists analytics_shop_product_event_created_at_idx
  on public.analytics(shop_id, product_id, event, created_at desc);

create index if not exists reviews_shop_id_idx
  on public.reviews(shop_id);

create index if not exists reviews_shop_product_idx
  on public.reviews(shop_id, product_id);

create index if not exists favourites_user_shop_product_idx
  on public.favourites(user_id, shop_id, product_id);
