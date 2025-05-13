from ValidationCheck.SchemaCheck import SchemaCheck
from logs import log_start_end
import logging

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
            'bill_id': 'int64',
            'customer_id': 'int64',
            'month': 'object',
            'amount_due': 'float64',
            'amount_paid': 'float64',
            'payment_date': 'datetime64[ns]'
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