from datetime import datetime
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from Code.logs import *
from Utilities.Encryption import *

class Transformer:
    def __init__(self):
        self.processing_time = datetime.now()
        self.partition_date = self.processing_time.strftime("%Y-%m-%d")
        self.partition_hour = self.processing_time.strftime("%H")
    
    def transform_billing_fully_paid(self, dataframe):

        dataframe['fully_paid'] = dataframe['amount_due'] == dataframe['amount_paid']
        dataframe['payment_status'] = dataframe['fully_paid'].apply(lambda x: 'Paid' if x == True else 'Unpaid')
        dataframe.drop(columns=['fully_paid'], inplace=True)
        return dataframe
    
    def transform_billing_dept(self, dataframe):
        dataframe['debt'] = dataframe['amount_due'] - dataframe['amount_paid']
        return dataframe
    
    def transform_billing_late_days(self, dataframe):
        days = dataframe["payment_date"].str.split("-").str[2].astype(int)
        dataframe['late_days'] = (days - 1)
        dataframe['fine'] = dataframe['late_days'] * 5.15
        dataframe['fine'] = dataframe['fine'].round(2)
        return dataframe
    
    def transform_billing_total(self, dataframe):
        dataframe['total'] = dataframe['amount_due'] + dataframe['fine']
        return dataframe
    
    @log_start_end
    def transform_billing(self, dataframe):
        dataframe = self.transform_billing_fully_paid(dataframe)
        dataframe = self.transform_billing_dept(dataframe)
        dataframe = self.transform_billing_late_days(dataframe)
        dataframe = self.transform_billing_total(dataframe)
        logging.info("Billing Data is Transformed Successfully")
        return dataframe
    
    def add_data_quality_columns(self, df)-> pd.DataFrame:
        """Add common data quality columns to all dataframes"""
        df['processing_time'] = self.processing_time
        df['partition_date'] = self.partition_date
        df['partition_hour'] = self.partition_hour
        return df
    
    @log_start_end
    def customer_transformations(self,df:pd.DataFrame)-> pd.DataFrame:
        df["tenure"]=self.processing_time.year - pd.to_datetime(df["account_open_date"]).dt.year

        def loyality(value):
            if value>5:
                return "Loyal"
            elif value >1:
                return "Newcomer"
            else:
                return "Normal"
        df["customer_segment"]=df["tenure"].map(loyality)
        logging.info("Customer Data is Transformed Successfully")
        df=self.add_data_quality_columns(df)
        logging.info("Quality Column is added to Customer Successfully")
        return df
    
    @log_start_end
    def tickets_transformations(self,df:pd.DataFrame)-> pd.DataFrame:
       df["age"]=self.processing_time - pd.to_datetime(df["complaint_date"])
       logging.info("Tickets Data is Transformed Successfully")
       df=self.add_data_quality_columns(df)

       df["age"]= df["age"].dt.days
       return df

    @log_start_end
    def transactions_transformations(self,df:pd.DataFrame)-> pd.DataFrame:
       def safe_int(x):
        try:
            return int(x)
        except:
            return -1
        
       df["cost"]=(df["transaction_amount"].apply(safe_int)*0.001)+0.50
       df["total_amount"]=df["transaction_amount"]+df["cost"]
       logging.info("transactions Data is Transformed Successfully")
       df=self.add_data_quality_columns(df)
       logging.info("Quality Column is added to transactions Successfully")


       return df
    
    @log_start_end
    def loans_transformations(self,df:pd.DataFrame)-> pd.DataFrame:
       encrypt=Encryption()
       df["age"]=self.processing_time-pd.to_datetime(df["utilization_date"])
       df["age"]=df["age"].dt.days
       df["annual_cost"]=(df["amount_utilized"]/5)+1000
       logging.info("loans Data is Transformed Successfully")
       df=self.add_data_quality_columns(df)
       logging.info("Quality Column is added to loans Successfully")
       encrypt.encrypt(df,"loan_reason") 
       logging.info("loan reason in encyrpted")
       return df
    


    