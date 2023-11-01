UPDATE public.dim_store_details
SET longitude = NULL
WHERE store_type = 'Web Portal';

ALTER TABLE public.dim_store_details ALTER COLUMN longitude TYPE FLOAT USING longitude::float,
ALTER COLUMN locality TYPE VARCHAR(255) USING locality::varchar(255),
ALTER COLUMN store_code TYPE VARCHAR(15) USING store_code::varchar(15),
ADD PRIMARY KEY (store_code),
ALTER COLUMN staff_numbers TYPE  SMALLINT USING staff_numbers::smallint,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::varchar(255),
ALTER COLUMN store_type DROP DEFAULT,
ALTER COLUMN latitude TYPE FLOAT USING latitude::float,
ALTER COLUMN country_code TYPE VARCHAR(5) USING country_code::varchar(5),
ALTER COLUMN continent TYPE VARCHAR(255) USING continent::varchar(255);

