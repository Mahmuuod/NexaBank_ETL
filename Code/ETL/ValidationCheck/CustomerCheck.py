import logging
# from .SchemaCheck import SchemaCheck
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from Code.logs import *
from Code.send_email import *
from Code.ETL.Extract.ExtractCSV import *
from Code.ETL.ValidationCheck.SchemaCheck import *

class CustomerCheck(SchemaCheck):

    @log_start_end 
    def check(self):

        if self.df.empty:
            logging.error("DataFrame is empty - rejecting file")
            return False
        
        expected_columns = ['customer_id', 'name', 'gender', 'age', 'city','account_open_date', 'product_type', 'customer_tier']
        
        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns} - rejecting file")
            return False
        
        if len(self.df) < 1:
            logging.error("DataFrame has no rows - rejecting file")
            return False
            
        expected_dtypes = {
            'customer_id': 'object',
            'name': 'object',
            'gender': 'object',
            'age': 'int64',
            'city': 'object',
            'account_open_date': 'object',
            'product_type': 'object',
            'customer_tier': 'object'
        }
        
        type_errors = []
        for col, dtype in expected_dtypes.items():
            if self.df[col].dtype != dtype:
                type_errors.append(f"{col} (expected {dtype}, got {self.df[col].dtype})")
        
        if type_errors:
            logging.error(f"Type mismatches: {', '.join(type_errors)} - rejecting file")
            return False
            
        logging.info("Schema validation passed successfully")
        return True

# import pandas as pd
# def main():
#     file_path = "E:\\ITI 9 Months\\Python\\NexaBank_ETL\\incoming_data\\2025-04-18\\14\\customer_profiles.csv"  
#     extractor = ExtractCSV()
#     df = extractor.extract(file_path)
#     print("Extracted Data:")
#     print(df.head())  
#     # Run CardCheck
#     checker = CustomerCheck(df)
#     result = checker.check()

#     print("Check passed:", result)

# if __name__ == "__main__":
#     main()