ALTER TABLE public.dim_products
ADD COLUMN weight_class varchar(20);

UPDATE public.dim_products
SET removed = 
            CASE
                    WHEN removed = 'Removed' THEN FALSE
                    WHEN removed = 'Still_avaliable' THEN TRUE
            END,
product_price = REPLACE(product_price, '£', ''),
weight = REPLACE(weight, 'kg', '');
        
ALTER TABLE public.dim_products
RENAME COLUMN removed to still_available;

UPDATE public.dim_products
SET weight_class = 
       CASE 
           WHEN weight::float < 2 THEN 'Light'
           WHEN weight::float >= 2 AND weight::float < 41 THEN 'Mid_Sized'
           WHEN weight::float >= 41 AND weight::float < 141 THEN 'HEAVY'
           WHEN weight::float >= 141 THEN 'Truck_Required'
       END;

ALTER TABLE public.dim_products
RENAME COLUMN product_price to product_price_in_£;

ALTER TABLE public.dim_products
RENAME COLUMN weight to weight_in_kg;

ALTER TABLE public.dim_products
ALTER COLUMN product_price_in_£ TYPE FLOAT USING product_price_in_£::float,
ALTER COLUMN weight_in_kg TYPE FLOAT USING weight_in_kg::float,
ALTER COLUMN "EAN" TYPE VARCHAR(14) USING "EAN"::varchar(14),
ALTER COLUMN product_code TYPE VARCHAR(14) USING product_code::varchar(14),
ALTER COLUMN date_added TYPE DATE USING date_added::date,
ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOL using still_available::bool,
ALTER COLUMN weight_class TYPE VARCHAR(20) USING weight_class::varchar(20),
ADD PRIMARY KEY (product_code);

