import logging
# from .SchemaCheck import SchemaCheck
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from Code.logs import *
from Code.send_email import *
from Code.ETL.Extract.ExtractCSV import *
from Code.ETL.ValidationCheck.SchemaCheck import *

class CardCheck(SchemaCheck):

    @log_start_end
    def check(self):
        if self.df.empty:
            logging.error("Credit card billing DataFrame is empty - rejecting file")
            return False
        
        expected_columns = ['bill_id', 'customer_id', 'month','amount_due', 'amount_paid', 'payment_date']
        
        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        
        if missing_columns:
            logging.error(f"Missing required columns in credit card data: {missing_columns} - rejecting file")
            return False
        
        if len(self.df) < 1:
            logging.error("Credit card billing DataFrame has no rows - rejecting file")
            return False
            
        expected_dtypes = {
            'bill_id': 'object',
            'customer_id': 'object',
            'month': 'object',
            'amount_due': 'float64',
            'amount_paid': 'float64',
            'payment_date': 'object'
        }
        
        type_errors = []
        for col, dtype in expected_dtypes.items():
            if col in self.df.columns and self.df[col].dtype != dtype:
                type_errors.append(f"{col} (expected {dtype}, got {self.df[col].dtype})")
        
        if type_errors:
            logging.error(f"Type mismatches in credit card data: {', '.join(type_errors)} - rejecting file")
            return False
            
        logging.info("Credit card billing schema validation passed successfully")
        return True
    

# import pandas as pd
# def main():
#     file_path = "E:\\ITI 9 Months\\Python\\NexaBank_ETL\\incoming_data\\2025-04-18\\14\\credit_cards_billing.csv"  
#     extractor = ExtractCSV()
#     df = extractor.extract(file_path)
#     print("Extracted Data:")
#     print(df.head())  
#     # Run CardCheck
#     checker = CardCheck(df)
#     result = checker.check()

#     print("Check passed:", result)

# if __name__ == "__main__":
#     main()