Multinational Data Centralisation

This Project aims is that it mimics that i work for a multinational company that sells various goods across the globe.
Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, the organisation would like to make its sales data accessible from one centralised location.

Milestone 1:

My first aim will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. Currently this data is spread under a number of sources, other relation tables, pdf files, across APIs, S3 buckets and finally AWS tables.

The technology that i've decided to use to retrieve this data is; 
1. for the pdf files: tabula.py which will enable to extract information from the pdf files
2. Accessing APIs is done through sending requests and makes use of the request package in python.
3. The S3 buckets using boto3 but before that you will have to be logging into the AWS client line interface
4. Finally AWS Databases accessed through sqlalchemy

Which lead to the creation of the data_extractor which acts as avenue for accessing the data from the various sources

Milestone 2: 

Now once known which sources of data are needed to be centralised, all this data is then aggregated into their own tables. Which will be the product table, date-time table, user table, order table and store details table. The order table will act as a single source of truth for the sales data.

However, this data will need to be cleaned and extracted which lead to the creation of data_extractor and the data_cleaning. Whereas the data_extractor acts as avenue for accessing data, This data will need to be cleaned and duplicates removed. Each cleaning method introduced helps to convert the tables mentioned above into their rightful demeanors so that it is ready to upload to the database

This is where the database_utils file comes into play, it will upload any of the finished tables to the local database


Milestone 3: 

Now that every table has been created, the next milestone is to create the single source of truth database which will be needed by the metrics used by the business, ensuring to develop the star-based schema of the database and that the columns are of the right types. This is where the folders database-structure and database-schema comes in handy. 

The database-structure, contains a derived setup for every table needed by the business however through the use of sqlalchemy postgres only executes one statement at a time therefore instead i've opted for making use of SQLTOOLS. Where I create an active connection and then run each setup file individually. Finally the database-schema folder has the sql file which finiliase all the relationships between the tables

Milestone 4:

Now that I have the schema for the database and all the sales data centralised to one location. The business can start making more data-drive decisions and get a better understanding of the underlying sales of the business which was answered with the Questions-Answers folders


To Run this project take the following steps, recreate the env with the file named env_multinational. 

1. Change the database_utils file to reflect where you would like to store your database 
2. install AWS CLI and have a access key ready to provide