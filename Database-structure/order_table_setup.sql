ALTER TABLE public.orders_table 
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid, 
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(20) using card_number::varchar(20),
ALTER COLUMN store_code TYPE VARCHAR(15) using store_code::varchar(15),
ALTER COLUMN product_code TYPE VARCHAR(15) using product_code::varchar(15),
ALTER COLUMN product_quantity TYPE SMALLINT using product_quantity::smallint;