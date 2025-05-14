import logging
from .SchemaCheck import SchemaCheck
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import logs 
from logs import log_start_end
from Code.send_email import *

class CardCheck(SchemaCheck):

    @log_start_end
    def check(self):
        email = EmailSender()
  
        if self.df.empty:
            logging.error("Credit card billing DataFrame is empty - rejecting file")
            email.send_email(
                subject="Card Check Validation",
                body="Credit card billing DataFrame is empty - rejecting file"
            )
            return False
        
        expected_columns = ['bill_id', 'customer_id', 'month','amount_due', 'amount_paid', 'payment_date']
        
        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        
        if missing_columns:
            email.send_email(
                subject="Card Check Validation",
                body="Missing required columns in credit card data - rejecting file"
            )
            logging.error(f"Missing required columns in credit card data: {missing_columns} - rejecting file")
            return False
        
        if len(self.df) < 1:
            logging.error("Credit card billing DataFrame has no rows - rejecting file")
            email.send_email(
                subject="Card Check Validation",
                body="Credit card billing DataFrame has no rows - rejecting file"
            )
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
            email.send_email(
                subject="Card Check Validation",
                body="Type mismatches in credit card data: - rejecting file"
            )
            logging.error(f"Type mismatches in credit card data: {', '.join(type_errors)} - rejecting file")
            return False
            
        logging.info("Credit card billing schema validation passed successfully")
        return True
    

