from ValidationCheck.SchemaCheck import SchemaCheck
from logs import log_start_end
import logging

class LoansCheck(SchemaCheck):

    @log_start_end
    def check(self):
        if self.df.empty:
            logging.error("Loans DataFrame is empty - rejecting file")
            return False
        
        expected_columns = ['customer_id', 'loan_type', 'amount_utilized','utilization_date', 'loan_reason']

        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        
        if missing_columns:
            logging.error(f"Missing required columns in loans data: {missing_columns} - rejecting file")
            return False
        
        if len(self.df) < 1:
            logging.error("Loans DataFrame has no rows - rejecting file")
            return False
            
        expected_dtypes = {
            'customer_id': 'int64',
            'loan_type': 'object',
            'amount_utilized': 'float64',
            'utilization_date': 'datetime64[ns]',
            'loan_reason': 'object'
        }
        
        type_errors = []
        for col, dtype in expected_dtypes.items():
            if col in self.df.columns and self.df[col].dtype != dtype:
                type_errors.append(f"{col} (expected {dtype}, got {self.df[col].dtype})")
        
        if type_errors:
            logging.error(f"Type mismatches in loans data: {', '.join(type_errors)} - rejecting file")
            return False
            
        logging.info("Loans schema validation passed successfully")
        return True