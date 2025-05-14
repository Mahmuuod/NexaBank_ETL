from datetime import datetime
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from Code.logs import *
from Code.ETL.Extract.ExtractCSV import *
from Code.Utilities.Encryption import *


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


def main():
    transformer = Transformer()

    # === Billing Data ===

    file_path = "E:\\ITI 9 Months\\Python\\NexaBank_ETL\\incoming_data\\2025-04-18\\14\\credit_cards_billing.csv"  
    extractor = ExtractCSV()
    df = extractor.extract(file_path)
    print("Extracted Data:")
    print(df.head())  
    billing_df = pd.DataFrame(df)
    billing_df = transformer.transform_billing(billing_df)

    # # === Customer Data ===
    # customer_data = [
    #     {"customer_id": "CUST000001", "name": "Matthew Crawford", "gender": "Male", "age": 19, "city": "Dubai", "account_open_date": "2021-06-22", "product_type": "CreditCard", "customer_tier": "Gold"},
    #     {"customer_id": "CUST000002", "name": "Chris Thompson", "gender": "Male", "age": 65, "city": "Alexandria", "account_open_date": "2016-10-03", "product_type": "PremiumAccount", "customer_tier": "Silver"},
    #     {"customer_id": "CUST000003", "name": "Madeline Hernandez", "gender": "Male", "age": 55, "city": "Casablanca", "account_open_date": "2023-09-27", "product_type": "CreditCard", "customer_tier": "Gold"},
    #     {"customer_id": "CUST000004", "name": "Kurt Ross", "gender": "Male", "age": 31, "city": "Jeddah", "account_open_date": "2021-06-23", "product_type": "PremiumAccount", "customer_tier": "Silver"},
    # ]
    # customers_df = pd.DataFrame(customer_data)
    # customers_df = transformer.customer_transformations(customers_df)
    # print("==== Customers Transformed ====")
    # print(customers_df, "\n")

    # # === Loans Data ===
    # loans_data = [
    #     {"customer_id": "CUST026688", "loan_type": "Top-Up Loan", "amount_utilized": 172000, "utilization_date": "2025-03-02", "loan_reason": "Let me know if we could spend a lazy Sunday just binge-watching old shows"},
    #     {"customer_id": "CUST037961", "loan_type": "Credit Card Loan", "amount_utilized": 838000, "utilization_date": "2025-01-18", "loan_reason": "You should definitely join we could go on a spontaneous road trip without much planning"},
    #     {"customer_id": "CUST051615", "loan_type": "Loan Against Deposit", "amount_utilized": 968000, "utilization_date": "2025-04-12", "loan_reason": "Let me know if we could go on a spontaneous road trip without much planning"},
    #     {"customer_id": "CUST003387", "loan_type": "Auto Loan", "amount_utilized": 627000, "utilization_date": "2024-11-08", "loan_reason": "I am writing to follow up that there are no pending items before the closure of this phase"},
    # ]
    # loans_df = pd.DataFrame(loans_data)
    # loans_df = transformer.loans_transformations(loans_df)
    # print("==== Loans Transformed ====")
    # print(loans_df, "\n")

    # # === Tickets Data ===
    # tickets_data = [
    #     {"ticket_id": "TICKET000001", "customer_id": "CUST092633", "complaint_category": "ATM Withdrawal Failed", "complaint_date": "2025-03-03", "severity": 4},
    #     {"ticket_id": "TICKET000002", "customer_id": "CUST084501", "complaint_category": "Card Not Working", "complaint_date": "2024-05-16", "severity": 7},
    #     {"ticket_id": "TICKET000003", "customer_id": "CUST008074", "complaint_category": "Unauthorized Transaction", "complaint_date": "2024-10-29", "severity": 4},
    #     {"ticket_id": "TICKET000004", "customer_id": "CUST017990", "complaint_category": "Mobile App Issues", "complaint_date": "2025-03-15", "severity": 5},
    # ]
    # tickets_df = pd.DataFrame(tickets_data)
    # tickets_df = transformer.tickets_transformations(tickets_df)
    # print("==== Tickets Transformed ====")
    # print(tickets_df, "\n")

    # # === Transactions Data ===
    # transactions_data = [
    #     {"sender": "CUST000001", "receiver": "CUST015796", "transaction_amount": 96, "transaction_date": "2024-12-13"},
    #     {"sender": "CUST000002", "receiver": "CUST000861", "transaction_amount": 81, "transaction_date": "2024-06-07"},
    # ]
    # transactions_df = pd.DataFrame(transactions_data)
    # transactions_df = transformer.transactions_transformations(transactions_df)
    # print("==== Transactions Transformed ====")
    # print(transactions_df)

# if __name__ == "__main__":
#     main()
