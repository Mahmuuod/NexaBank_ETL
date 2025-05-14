import logging
from .SchemaCheck import SchemaCheck
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import logs 
from logs import log_start_end
from Code.send_email import *

class CustomerCheck(SchemaCheck):

    @log_start_end 
    def check(self):
        email = EmailSender()

        if self.df.empty:
            email.send_email(
                subject="Customer Check Validation",
                body="Customer profiles DataFrame is empty - rejecting file"
            )
            logging.error("DataFrame is empty - rejecting file")
            return False
        
        expected_columns = ['customer_id', 'name', 'gender', 'age', 'city','account_open_date', 'product_type', 'customer_tier']
        
        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        
        if missing_columns:
            email.send_email(
                subject="Customer Check Validation",
                body="Missing required columns - rejecting file"
            )
            logging.error(f"Missing required columns: {missing_columns} - rejecting file")
            return False
        
        if len(self.df) < 1:
            email.send_email(
                subject="Customer Check Validation",
                body="DataFrame has no rows - rejecting file"
            )
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
            email.send_email(
                subject="Customer Check Validation",
                body="Type mismatches: check logs - rejecting file"
            )
            logging.error(f"Type mismatches: {', '.join(type_errors)} - rejecting file")
            return False
            
        logging.info("Schema validation passed successfully")
        return True