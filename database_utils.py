import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect

class DatabaseConnector:

    
   def upload_to_db(dataframe: pd.DataFrame, table_name: str):
    """
    Creates a SQLalchmey engine to a postgres database then uploads the dataframe

    Input: Data to be uploaded (DataFrame)
    Input: Table_Name to be saved (String)
    """
    engine = create_engine('postgresql://postgres:Barcemo123@localhost:5432/Sales_Data')
    dataframe.to_sql(table_name, engine, if_exists= 'replace', index= False)
    with engine.connect() as con:
        con.execute('ALTER TABLE public.dim_date_times ALTER COLUMN month TYPE INT USING month::integer;')
    
    
    pass
   def list_db_tables(db_engine):
    """
    Using the SQLalchemy Engine, it returns a list of all db tables

    Parameters:
        Input: (sqlalchemy.engine) db-engine
        Output: List of tables in Engine
    """
    list_of_table_names = {}
    inspector = inspect(db_engine)
    list_of_table_names = inspector.get_table_names()
    return list(list_of_table_names)

   def init_db_engine(database_credentials: dict):
    """
    Using the DB crediantials, it initiliase the sqlalchemy engine

    Parameters: (dict) Database Crediantials

    Output: (sqlalchemy.engine) SQLalchemy engine
    """
    RDS_USER=  database_credentials['RDS_USER']
    RDS_PASSWORD =  database_credentials['RDS_PASSWORD']
    RDS_HOST =  database_credentials['RDS_HOST']
    RDS_PORT = database_credentials['RDS_PORT']
    RDS_DATABASE =  database_credentials['RDS_DATABASE']
    connection_string = f"postgresql+psycopg2://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
    engine = create_engine(connection_string)
    return engine
   
   def read_db_creds():
    """
    Reads the DB creds from a Yaml File by opening a reader stream
    and safely loading it using yaml.safe_load

    Parameters: None

    Output: (dict) A dictionary containing the Database Credentials 
    """
    with open('db_creds.yaml', 'r') as stream:
            data_loaded = yaml.safe_load(stream)
           
    return data_loaded

dictionary_cred = DatabaseConnector.read_db_creds()
engine = DatabaseConnector.init_db_engine(database_credentials=dictionary_cred)
DatabaseConnector.list_db_tables(engine)