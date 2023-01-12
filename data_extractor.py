import database_utils
import data_cleaning
import pandas as pd
from typing import Union

class DataExtractor:
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
      
        

cred = database_utils.DatabaseConnector.read_db_creds()
engine = database_utils.DatabaseConnector.init_db_engine(cred)
list_of_tables = database_utils.DatabaseConnector.list_db_tables(engine)
pandas_user = DataExtractor.extract_rds_table(engine, list_of_tables)
cleaned_user_data = data_cleaning.Dataclean.clean_user_data(pandas_user['legacy_users'])
database_utils.DatabaseConnector.upload_to_db(cleaned_user_data, 'dim_users')
#cleaned_user_data.to_csv('legacy_users.csv', encoding= 'utf-8-sig')