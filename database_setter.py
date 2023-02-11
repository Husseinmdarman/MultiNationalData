from sqlalchemy import create_engine
import math


class data_setter:
    def __init__(self) -> None:
        self.engine = create_engine('postgresql://postgres:Barcemo123@localhost:5432/Sales_Data')
    def send_query(self,query: str):
        with self.engine.connect() as con:
         con.execute(f'{query}')
         

set_long_statement = f"""UPDATE public.dim_store_details
SET longitude = '{math.nan}'
WHERE store_type = 'Web Portal';"""
         
orders_table_setter = """ALTER TABLE public.orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid, 
        ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
        ALTER COLUMN card_number TYPE VARCHAR(20) using card_number::varchar(20),
        ALTER COLUMN store_code TYPE VARCHAR(15) using store_code::varchar(15),
        ALTER COLUMN product_code TYPE VARCHAR(15) using product_code::varchar(15),
        ALTER COLUMN product_quantity TYPE SMALLINT using product_quantity::smallint,
        DROP COLUMN level_0,
        DROP COLUMN index;"""
user_dim_setter = """ALTER TABLE public.dim_users ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::varchar(255),
                    ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::varchar(255),
                    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
                    ALTER COLUMN country_code TYPE VARCHAR(5) USING country_code::varchar(5),
                    ALTER COLUMN join_date TYPE DATE USING join_date::date,
                    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;"""
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
setter.send_query(dim_store_details_setter)