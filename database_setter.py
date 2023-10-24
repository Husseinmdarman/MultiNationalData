from sqlalchemy import create_engine
import math

class data_setter:
    def __init__(self) -> None:
        self.engine = create_engine('postgresql://postgres:Barcemo123@localhost:5432/Sales_Data')
    def send_query(self,query: str):
        with self.engine.connect() as con:
         con.execute(f'{query}')
         

update_dim_products = """
                    UPDATE public.dim_products
                    SET product_price = REPLACE(product_price, '£', ''), weight = REPLACE(weight, 'kg', '');"""

update_long_statement = f"""UPDATE public.dim_store_details
SET longitude = '{math.nan}'
WHERE store_type = 'Web Portal';"""
dim_products_set_still_available_to_bool = f"""
UPDATE public.dim_products
SET removed = 
                CASE
                    WHEN removed = 'Removed' THEN {False}
                    WHEN removed = 'Still_avaliable' THEN {True}
                END;
"""
dim_products_create_weight_class_col = """
ALTER TABLE public.dim_products
ADD COLUMN weight_class varchar(20);
"""
dim_products_set_weight_class_col = """
UPDATE public.dim_products
SET weight_class = 
       CASE 
           WHEN weight_kg::float < 2 THEN 'Light'
           WHEN weight_kg::float >= 2 AND weight_kg::float < 41 THEN 'Mid_Sized'
           WHEN weight_kg::float >= 41 AND weight_kg::float < 141 THEN 'HEAVY'
           WHEN weight_kg::float >= 141 THEN 'Truck_Required'
       END;
"""

dim_products_rename_product_price_col = """
                    ALTER TABLE public.dim_products
                    RENAME COLUMN product_price to product_price_£;"""

dim_products_rename_weight_col = """
                    ALTER TABLE public.dim_products
                    RENAME COLUMN weight to weight_kg;"""

dim_products_rename_removed_col = """
                    ALTER TABLE public.dim_products
                    RENAME COLUMN removed to still_available;"""
dim_products_table_setter = """
                            ALTER TABLE public.dim_products
                            ALTER COLUMN product_price_£ TYPE FLOAT USING product_price_£::float,
                            ALTER COLUMN weight_kg TYPE FLOAT USING weight_kg::float,
                            ALTER COLUMN "EAN" TYPE VARCHAR(14) USING "EAN"::varchar(14),
                            ALTER COLUMN product_code TYPE VARCHAR(14) USING product_code::varchar(14),
                            ALTER COLUMN date_added TYPE DATE USING date_added::date,
                            ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
                            ALTER COLUMN still_available TYPE BOOL using still_available::bool,
                            ALTER COLUMN weight_class TYPE VARCHAR(20) USING weight_class::varchar(20);

"""

orders_table_setter = """ALTER TABLE public.orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid, 
        ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
        ALTER COLUMN card_number TYPE VARCHAR(20) using card_number::varchar(20),
        ALTER COLUMN store_code TYPE VARCHAR(15) using store_code::varchar(15),
        ALTER COLUMN product_code TYPE VARCHAR(15) using product_code::varchar(15),
        ALTER COLUMN product_quantity TYPE SMALLINT using product_quantity::smallint,
        DROP COLUMN level_0,
        DROP COLUMN index;"""

update_long_statement = f"""UPDATE public.dim_store_details
SET longitude = '{math.nan}'
WHERE store_type = 'Web Portal';"""

dim_store_details_setter = """ALTER TABLE public.dim_store_details ALTER COLUMN longitude TYPE FLOAT USING longitude::float,
                    ALTER COLUMN locality TYPE VARCHAR(255) USING locality::varchar(255),
                    ALTER COLUMN store_code TYPE VARCHAR(15) USING store_code::varchar(15),
                    ALTER COLUMN staff_numbers TYPE  SMALLINT USING staff_numbers::smallint,
                    ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
                    ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::varchar(255),
                    ALTER COLUMN store_type DROP DEFAULT,
                    ALTER COLUMN latitude TYPE FLOAT USING latitude::float,
                    ALTER COLUMN country_code TYPE VARCHAR(5) USING country_code::varchar(5),
                    ALTER COLUMN continent TYPE VARCHAR(255) USING continent::varchar(255),
                    DROP COLUMN index;"""

setter = data_setter()
setter.send_query(dim_products_table_setter)