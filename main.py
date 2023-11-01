import data_cleaning
import data_extractor
import database_utils

def order_table_init():
    cred = database_utils.DatabaseConnector.read_db_creds()
    engine = database_utils.DatabaseConnector.init_db_engine(cred)
    list_of_tables = database_utils.DatabaseConnector.list_db_tables(engine)
    order_table = data_extractor.DataExtractor.extract_rds_table(engine, 'orders_table')
    cleaned_order_table = data_cleaning.Dataclean.clean_orders_data(order_table)
    database_utils.DatabaseConnector.upload_to_db(cleaned_order_table, 'orders_table')

def datetime_table_init():
    date_time_df = data_extractor.DataExtractor.extract_from_request()
    cleaned_date_time_df =data_cleaning.Dataclean.clean_dateTime_data(date_time_df)
    database_utils.DatabaseConnector.upload_to_db(cleaned_date_time_df, 'dim_date_times')

def user_table_init():
    cred = database_utils.DatabaseConnector.read_db_creds()
    engine = database_utils.DatabaseConnector.init_db_engine(cred)
    list_of_tables = database_utils.DatabaseConnector.list_db_tables(engine)
    pandas_user = data_extractor.DataExtractor.extract_rds_table(engine, list_of_tables)
    cleaned_user_data = data_cleaning.Dataclean.clean_user_data(pandas_user['legacy_users'])
    database_utils.DatabaseConnector.upload_to_db(cleaned_user_data, 'dim_users')

def card_details_table_init():
    df2 = data_extractor.DataExtractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    cleaned_card_data = data_cleaning.Dataclean.clean_card_data(df2)
    print(cleaned_card_data)
    database_utils.DatabaseConnector.upload_to_db(cleaned_card_data, 'dim_card_details')

def store_details_init():
    print('start number of stores')
    data_extractor.DataExtractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')
    print('done number of stores')
    print('dyart retirinbing number of stores')
    store_data = data_extractor.DataExtractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')
    print('done retirinbing number of stores')
    store_data = data_cleaning.Dataclean.clean_store_data(store_data)
    database_utils.DatabaseConnector.upload_to_db(store_data, 'dim_store_details')

def product_details_init():
    products_data = data_extractor.DataExtractor.extract_from_s3('s3://data-handling-public/products.csv')
    cleaned_products = data_cleaning.Dataclean.clean_product_details(products_data)
    database_utils.DatabaseConnector.upload_to_db(cleaned_products, 'dim_products')

def main():
    order_table_init()

if __name__ == "__main__":
    main()
    
    
    
    