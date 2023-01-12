import pandas as pd
import re

class Dataclean:
   
    def clean_user_data(user_dataframe : pd.DataFrame):
        """
        Takes the legacy_users dataframe and cleans the columns for any null or dirty data
        finally sets the correct datatypes for the user dataframe

        Input: User dataframe (dataframe)
        Output: Cleaned user dataframe (Dataframe)
        """
        # Drop Missing values from DataFrame
        user_dataframe.dropna(axis=0, inplace= True)
        # Drop the Unnamed and Index column from the dataframe
        user_dataframe.drop(columns=['index'], inplace= True)
        
        #Set the user_uuid as the key since its value is unique
        user_dataframe.set_index('user_uuid', inplace = True)

        #check for duplicates
        sum_of_duplicates = user_dataframe.duplicated().sum()

        #if sum of duplicates is greater than 0 then drop duplicates
        
        if(sum_of_duplicates > 0):
            user_dataframe.drop_duplicates(inplace= True)
        
        
        
        #Sets the type of columns

        user_dataframe['first_name'] = user_dataframe['first_name'].astype('string')
        user_dataframe['last_name'] = user_dataframe['last_name'].astype('string')
        user_dataframe['company'] = user_dataframe['company'].astype('string')
        user_dataframe['address'] = user_dataframe['address'].astype('string')
        user_dataframe['email_address'] = user_dataframe['email_address'].astype('string')
        user_dataframe['country'] = user_dataframe['country'].astype('string')
        user_dataframe['country_code'] = user_dataframe['country_code'].astype('string')

        
        #remove Null records from dataframe
        user_dataframe = user_dataframe[user_dataframe['first_name'].str.contains('NULL') == False]
        #remove records containing numbers from dataframe
        user_dataframe = user_dataframe[user_dataframe['first_name'].str.contains("\d", regex=True) == False]

        #Set the correct Date/time format for DOB and join date
        user_dataframe['date_of_birth'] = pd.to_datetime(user_dataframe['date_of_birth']).dt.strftime('%d-%m-%Y')
        user_dataframe['join_date'] = pd.to_datetime(user_dataframe['join_date']).dt.strftime('%d-%m-%Y')

        
        #fix the typo GGB to GB country code
        user_dataframe['country_code'] = user_dataframe['country_code'].str.replace("GGB", "GB")
        user_dataframe['phone_number']= user_dataframe[['country_code', 'phone_number']].apply(correct_phone_number, axis = 1)
        
       # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        #    print(user_dataframe['corrected_phone_number'])
        
        return user_dataframe


def correct_phone_number(row):

  isd_code_map = { "GB": "+44", "DE": "+49", "US": "+1" }
  
  # Remove special chars other than digits, `+` and letters used for extension e.g. `x`, `ext` (following keeps all alphabets).
  result = re.sub("[^A-Za-z\d\+]", "", row["phone_number"])
  
  # Prefix ISD code by matching country code.
  if not result.startswith(isd_code_map[row["country_code"]]):
    result = isd_code_map[row["country_code"]] + result

  # Remove `0` that follows ISD code.
  if result.startswith(isd_code_map[row["country_code"]] + "0"):
    result = result.replace(isd_code_map[row["country_code"]] + "0", isd_code_map[row["country_code"]])
  return result