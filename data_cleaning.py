import pandas as pd
import re
import sys

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
        
        user_dataframe['date_of_birth'] = pd.to_datetime(user_dataframe['date_of_birth'], format='mixed').dt.strftime('%d-%m-%Y')
        user_dataframe['join_date'] = pd.to_datetime(user_dataframe['join_date'], format='mixed').dt.strftime('%d-%m-%Y')
        
        #fix the typo GGB to GB country code
        user_dataframe['country_code'] = user_dataframe['country_code'].str.replace("GGB", "GB")
        user_dataframe['phone_number']= user_dataframe[['country_code', 'phone_number']].apply(correct_phone_number, axis = 1)
        
       # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        #    print(user_dataframe['corrected_phone_number'])
        
        return user_dataframe
    
    def clean_dateTime_data(dateTime_dataframe: pd.DataFrame):
      #remove any digits from timePeriod which includes duaod8a8d
      dateTime_dataframe = dateTime_dataframe[dateTime_dataframe['time_period'].str.contains("\d", regex=True) == False]
      dateTime_dataframe = dateTime_dataframe[dateTime_dataframe['time_period'].str.contains('NULL') == False]
      
      return dateTime_dataframe
    
    def clean_orders_data(orders_dataframe: pd.DataFrame):
      orders_dataframe.drop(columns=['first_name','last_name','1', 'level_0', 'index'], inplace= True)
      return orders_dataframe
    def clean_product_details(product_dataframe: pd.DataFrame):
      print(product_dataframe.columns)
      product_dataframe = product_dataframe[product_dataframe['removed'].str.contains('NULL') == False]
      searchfor = ['Still_avaliable', 'Removed']
      product_dataframe = product_dataframe[product_dataframe['removed'].str.contains('|'.join(searchfor))]
      product_dataframe.to_csv('weight_cleaned.csv')
      
      product_dataframe['weight']= product_dataframe['weight'].apply(Dataclean.convert_product_weights)
      product_dataframe.drop(columns=['Unnamed: 0'], inplace= True)
      
      return product_dataframe

    def convert_product_weights(row):
      #weights will be given one by one first we check if it containst a kg then we do nothing
      
      if('kg' in row):
         return row
      elif(('g' in row and "x" in row)): #if the row contains an X, we will have to multiply it to get the correct weight then
       data = '10 x 2g'
       
       row = re.sub("[g,ml]","",row)
       
       #row= row.str.replace("[^a-z]\S", "", regex=True)
       split = row.split('x')
       new_weight= int(split[0]) * float(split[-1])
       weight_converted_kg = str(new_weight / 1000) + 'kg'
       row = weight_converted_kg
       
       return row
      elif(('g' in row) or ('ml' in row)):
       row = re.sub("[g,ml]","",row)
       row=row.rstrip('.')
       converted_weight = str(float(row)/1000) + 'kg'
       row = converted_weight
       return row

      

    def clean_store_data(store_dataframe: pd.DataFrame):
      """
      Takes the store dataframe and cleans the dataframe and
      then returns the dataframe

      input: 
          Store_dataframe (pd.Dataframe)
      Output: 
          Cleaned_store_dataframe (pd.Dataframe)
      
      """
      #remove Null records from dataframe
      store_dataframe = store_dataframe[store_dataframe['store_type'].str.contains('NULL') == False]
      #remove records containing numbers from dataframe
      store_dataframe = store_dataframe[store_dataframe['store_type'].str.contains("\d", regex=True) == False]
      
      #Opening date in corrected format of day, month, year
      store_dataframe['opening_date'] = pd.to_datetime(store_dataframe['opening_date'], format='mixed').dt.strftime('%d-%m-%Y')
      
      #remove letters from staff number column
      store_dataframe['staff_numbers'] = store_dataframe['staff_numbers'].astype('string')
      store_dataframe['staff_numbers'] = store_dataframe['staff_numbers'].str.replace("[a-z, A-Z]", "", regex=True)
      
      #Drop the duplicated lat column from dataframe 
      store_dataframe.drop('lat', axis= 1, inplace = True)

      #replace eeAmerica and eeEurope in the continent column with Europe and America respectively
      store_dataframe['continent'] = store_dataframe['continent'].str.replace("eeAmerica", "America", regex=False)
      store_dataframe['continent'] = store_dataframe['continent'].str.replace("eeEurope", "Europe", regex=False)

      
      return store_dataframe
      
    
    
    def clean_card_data(user_card_dataframe : pd.DataFrame):
      """
        Takes the legacy_users dataframe and cleans the columns for any null or dirty data
        finally sets the correct datatypes for the user dataframe

        Input: User dataframe (dataframe)
        Output: Cleaned user dataframe (Dataframe)
        """

      #change the data type to string then search for any letters and special characters in the card number and drop the rows
      
      user_card_dataframe['card_number'] = user_card_dataframe['card_number'].astype('string')
      user_card_dataframe = user_card_dataframe[user_card_dataframe['card_number'].str.contains("[a-z, A-Z]", regex=True) == False]
      user_card_dataframe['card_number'] = user_card_dataframe['card_number'].str.replace("[?]", "", regex=True)
    
      # set date of  payment using standard format 
      user_card_dataframe['date_payment_confirmed'] = pd.to_datetime(user_card_dataframe['date_payment_confirmed'], format='mixed').dt.strftime('%d-%m-%Y')
      
      #drop na, null records
      user_card_dataframe.dropna(axis=0, inplace= True)

      #set the column types
      user_card_dataframe['card_provider'] = user_card_dataframe['card_provider'].astype('string')
      
      return user_card_dataframe
      
    


def correct_phone_number(row):
  """
  Takes in a country code and phone number from the record then cleans the phone number
  by removing special chars other than digits then removes any trailing zero

  Input: Row (Dataframe)
  Row is a dataframe containing columns country code and phone number

  Output: Corrected Phone number 
  Corrected phone number returned after all the cleaning steps were done
  """



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
