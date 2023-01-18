import database_utils
import data_cleaning
import pandas as pd
from typing import Union
import tabula
import yaml
import requests

class DataExtractor:

    def retrieve_stores_data(api_endpoint: str, headers = None):
        
        number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        stores_data = []

        if(headers == None):
            with open('API_Keys.yaml', 'r') as stream:
                headers = yaml.safe_load(stream)
        
        number_of_stores = DataExtractor.list_number_of_stores(number_of_stores_endpoint, headers)
        
        for store_id in range(0, number_of_stores):
            url = api_endpoint + f'{store_id}'
            
            response = requests.get(url = url, headers = headers)
            stores_data.append(response.json())
        store_data = pd.DataFrame(stores_data)
        
        return store_data

    def list_number_of_stores(api_endpoint: str, headers = None):
        """
         returns the number of stores to extract from

         input: API endpoint (str)
            API endpoint which locates the number of stores to extract from
         headers: Headers needed for API endpoint

         Ouput: number of stores   
        """
        if(headers == None):
            with open('API_Keys.yaml', 'r') as stream:
                headers = yaml.safe_load(stream)
        
        
        response = requests.get(url = api_endpoint, headers= headers)
        
        if response.status_code == 200:
            
            number_of_stores = response.json()['number_stores']
            return number_of_stores
        else: 
            return response.status_code    
        
    def retrieve_pdf_data(link: str):
        """
         takes in a link as an argument then uses 
         tabula to extract all pages from the pdf document 
         and returns a pandas DataFrame.

         input: Link (str) Link to the pdf file
         Output: Dataframe (Dataframe) DataFrame of the extracted data
        """
        # Read pdf into a list of DataFrame
        list_of_pdf = tabula.read_pdf(link, pages='all', output_format= 'dataframe')
        
        return (list_of_pdf[0])
            
        

    def extract_rds_table(engine, table_name: Union[str, list]):
        """
        Extracts the table from the engine and reads it into a pandas Dataframe to return it
        Once given multiple table names as input, it will return a dictionary Key, Value pairs
        of table_name and dataframe

        Parameters
            input: Engine (Sqlalchemy.engine)
            input: Table/Tables (str | list)
            Output: Pandas Dataframe of the table in question/ Dictionary of dataframes
        
        """
        if (type(table_name) == str):
            
            # returns a single dataframe of the table name in question
            return pd.read_sql_table(f'{table_name}', engine)
        
        elif (type(table_name) == list):
            
            # returns a dictionary of key, dataFrame pairs such as User: Dataframe
            return dict((table, pd.read_sql_table(f'{table}', engine)) 
                        for table in table_name)
      
        

# cred = database_utils.DatabaseConnector.read_db_creds()
# engine = database_utils.DatabaseConnector.init_db_engine(cred)
# list_of_tables = database_utils.DatabaseConnector.list_db_tables(engine)
# pandas_user = DataExtractor.extract_rds_table(engine, list_of_tables)
# cleaned_user_data = data_cleaning.Dataclean.clean_user_data(pandas_user['legacy_users'])
# database_utils.DatabaseConnector.upload_to_db(cleaned_user_data, 'dim_users')
#cleaned_user_data.to_csv('legacy_users.csv', encoding= 'utf-8-sig')
# df2 = DataExtractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# cleaned_card_data = data_cleaning.Dataclean.clean_card_data(df2)
# database_utils.DatabaseConnector.upload_to_db(cleaned_card_data, 'dim_card_details')
#cleaned_card_data.to_csv('card_details.csv')
#DataExtractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')
DataExtractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')