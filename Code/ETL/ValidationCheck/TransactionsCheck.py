import logging
from .SchemaCheck import SchemaCheck
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import logs 
from logs import log_start_end
from Code.send_email import *


class TransactionsCheck(SchemaCheck):


    @log_start_end
    def check(self):
        email = EmailSender()

        if self.df.empty:
            email.send_email(
                subject="Transactions Check Validation",
                body="Transactions DataFrame is empty - rejecting file"
            )
            logging.error("Transactions DataFrame is empty - rejecting file")
            return False
        
        expected_columns = ['sender', 'receiver', 'transaction_amount', 'transaction_date']

        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        
        if missing_columns:
            email.send_email(
                subject="Transactions Check Validation",
                body="Missing required columns in transactions check logs - rejecting file"
            )
            logging.error(f"Missing required columns in transactions: {missing_columns} - rejecting file")
            return False
        
        if len(self.df) < 1:
            email.send_email(
                subject="Transactions Check Validation",
                body="Transactions DataFrame has no rows - rejecting file"
            )
            logging.error("Transactions DataFrame has no rows - rejecting file")
            return False
            
        expected_dtypes = {
            'sender': 'object',  
            'receiver': 'object', 
            'transaction_amount': 'int64',
            'transaction_date': 'object'
        }
        
        type_errors = []
        for col, dtype in expected_dtypes.items():
            if col in self.df.columns and self.df[col].dtype != dtype:
                type_errors.append(f"{col} (expected {dtype}, got {self.df[col].dtype})")
        
        if type_errors:
            email.send_email(
                subject="Transactions Check Validation",
                body="Type mismatches in transactions check logs - rejecting file"
            )
            logging.error(f"Type mismatches in transactions: {', '.join(type_errors)} - rejecting file")
            return False
            
        logging.info("Transactions schema validation passed successfully")
        return True