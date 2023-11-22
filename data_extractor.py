import pandas as pd
from typing import Union
import tabula
import yaml
import requests
import boto3
import io
import concurrent.futures

class DataExtractor:
    def extract_from_request():
        response = requests.get('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        
        
        return(pd.DataFrame(response.json()))
    
    def extract_from_s3(s3_address: str):
        """
        Takes a s3 bucket and returns a dataframe of the contents

        Inputs: 
            s3_address(str): the address of the s3 buckets
        Ouput: 
            extract_s3_dataframe(Dataframe): returns the s3 bucket in a dataframe
        """
        
        client = boto3.client('s3')
        bucket, key = s3_address.split('/',2)[-1].split('/',1)
        
        object_response = client.get_object(Bucket=bucket, Key=key)
        
        
        product_dataframe = pd.read_csv(io.BytesIO(object_response['Body'].read()))
        
        return product_dataframe
        
    def requestHandler(url: str, headers = None):
        """
        Takes a url request returns a response

        Inputs: 
            url(str): the address of the request
        Ouput: 
            response(Response): returns the fetched result from the query
        
        """
        
        response = requests.get(url = url, headers = headers)
        
        return response
    
    def retrieve_stores_data(api_endpoint: str, headers = None):
        """
        Takes the api endpoint for the stores and returns a dataframe of every store owned by the multinational company


        Inputs: 
            api_endpoint(str): the address of where the store data is store
        Ouput: 
            stores data (Dataframe): returns the store details in a dataframe
        
        """
     
        
        number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        stores_data = []
        url_data = []

        if(headers == None):
            with open('API_Keys.yaml', 'r') as stream:
                headers = yaml.safe_load(stream)
        
        number_of_stores = DataExtractor.list_number_of_stores(number_of_stores_endpoint, headers) # gets the interger number of the stores total

        for store_id in range(0, number_of_stores):  #appends each store api to the store number
            url = api_endpoint + f'{store_id}'
            url_data.append(url)
            
        
        with concurrent.futures.ThreadPoolExecutor() as executor: #send each request to a executor 
            futures = []
            for url in url_data:
                futures.append(executor.submit(DataExtractor.requestHandler,url,headers))
            for future in concurrent.futures.as_completed(futures):
                stores_data.append(future.result().json())
           
        
        store_data = pd.DataFrame(stores_data) #append the completed store_date to the dataframe
        
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
