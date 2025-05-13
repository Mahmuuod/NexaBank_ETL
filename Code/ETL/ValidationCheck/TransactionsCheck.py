from ValidationCheck.SchemaCheck import SchemaCheck
from logs import log_start_end
import logging

class TransactionsCheck(SchemaCheck):


    @log_start_end
    def check(self):
        if self.df.empty:
            logging.error("Transactions DataFrame is empty - rejecting file")
            return False
        
        expected_columns = ['sender', 'receiver', 'transaction_amount', 'transaction_date']

        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        
        if missing_columns:
            logging.error(f"Missing required columns in transactions: {missing_columns} - rejecting file")
            return False
        
        if len(self.df) < 1:
            logging.error("Transactions DataFrame has no rows - rejecting file")
            return False
            
        expected_dtypes = {
            'sender': 'object',  
            'receiver': 'object', 
            'transaction_amount': 'float64',
            'transaction_date': 'datetime64[ns]'
        }
        
        type_errors = []
        for col, dtype in expected_dtypes.items():
            if col in self.df.columns and self.df[col].dtype != dtype:
                type_errors.append(f"{col} (expected {dtype}, got {self.df[col].dtype})")
        
        if type_errors:
            logging.error(f"Type mismatches in transactions: {', '.join(type_errors)} - rejecting file")
            return False
            
        logging.info("Transactions schema validation passed successfully")
        return True