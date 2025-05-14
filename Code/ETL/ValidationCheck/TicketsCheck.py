import logging
from .SchemaCheck import SchemaCheck
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import logs 
from logs import log_start_end
from Code.send_email import *

class TicketsCheck(SchemaCheck):

    @log_start_end
    def check(self):
        email = EmailSender()

        if self.df.empty:
            logging.error("Support tickets DataFrame is empty - rejecting file")
            email.send_email(
                subject="Tickets Check Validation",
                body="Tickets DataFrame is empty - rejecting file"
            )
            return False
        
        expected_columns = ['ticket_id', 'customer_id', 'complaint_category','complaint_date', 'severity']

        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        
        if missing_columns:
            email.send_email(
                subject="Tickets Check Validation",
                body="Missing required columns in support tickets check logs- rejecting file"
            )
            logging.error(f"Missing required columns in support tickets: {missing_columns} - rejecting file")
            return False
        
        if len(self.df) < 1:
            email.send_email(
                subject="Tickets Check Validation",
                body="Support tickets DataFrame has no rows - rejecting file"
            )
            logging.error("Support tickets DataFrame has no rows - rejecting file")
            return False
            
        expected_dtypes = {
            'ticket_id': 'object',
            'customer_id': 'object',
            'complaint_category': 'object',
            'complaint_date': 'object',
            'severity': 'int64' 
        }
        
        type_errors = []
        for col, dtype in expected_dtypes.items():
            if col in self.df.columns and self.df[col].dtype != dtype:
                type_errors.append(f"{col} (expected {dtype}, got {self.df[col].dtype})")
        
        if type_errors:
            email.send_email(
                subject="Tickets Check Validation",
                body="Type mismatches in support tickets - rejecting file"
            )
            logging.error(f"Type mismatches in support tickets: {', '.join(type_errors)} - rejecting file")
            return False
            
        logging.info("Support tickets schema validation passed successfully")
        return True