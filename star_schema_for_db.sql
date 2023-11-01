ALTER TABLE public.orders_table 
ADD CONSTRAINT fk_users_orders FOREIGN KEY (user_uuid) REFERENCES public.dim_users (user_uuid),
ADD CONSTRAINT fk_store_orders FOREIGN KEY (store_code) REFERENCES public.dim_store_details (store_code),
ADD CONSTRAINT fk_products_orders FOREIGN KEY (product_code) REFERENCES public.dim_products (product_code),
ADD CONSTRAINT fk_datetime_orders FOREIGN KEY (date_uuid) REFERENCES public.dim_date_times (date_uuid),
ADD CONSTRAINT fk_card_details_orders FOREIGN KEY (card_number) REFERENCES public.dim_card_details (card_number);
