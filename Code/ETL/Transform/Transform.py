from datetime import datetime
import pandas as pd
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
    
    def transform_billing(self, dataframe):
        dataframe = self.transform_billing_fully_paid(dataframe)
        dataframe = self.transform_billing_dept(dataframe)
        dataframe = self.transform_billing_late_days(dataframe)
        dataframe = self.transform_billing_total(dataframe)
        return dataframe
    
    def add_data_quality_columns(self, df)-> pd.DataFrame:
        """Add common data quality columns to all dataframes"""
        df['processing_time'] = self.processing_time
        df['partition_date'] = self.partition_date
        df['partition_hour'] = self.partition_hour
        return df
    
    
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
        df=self.add_data_quality_columns(df)
        return df
    
    def tickets_transformations(self,df:pd.DataFrame)-> pd.DataFrame:
       df["age"]=self.processing_time - pd.to_datetime(df["complaint_date"])
       df=self.add_data_quality_columns(df)

       df["age"]= df["age"].dt.days
       return df

    def transactions_transformations(self,df:pd.DataFrame)-> pd.DataFrame:
       def safe_int(x):
        try:
            return int(x)
        except:
            return -1
        
       df["cost"]=(df["transaction_amount"].apply(safe_int)*0.001)+0.50
       df["total_amount"]=df["transaction_amount"]+df["cost"]
       df=self.add_data_quality_columns(df)

       return df
 
    def loans_transformations(self,df:pd.DataFrame)-> pd.DataFrame:
       encrypt=Encryption()
       df["age"]=self.processing_time-pd.to_datetime(df["utilization_date"])
       df["age"]=df["age"].dt.days
       df["annual_cost"]=(df["amount_utilized"]/5)+1000
       df=self.add_data_quality_columns(df)
       encrypt.encrypt(df,"loan_reason") 
       return df
    



    