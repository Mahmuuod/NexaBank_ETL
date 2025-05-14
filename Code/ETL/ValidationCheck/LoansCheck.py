import logging
from .SchemaCheck import SchemaCheck
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from Code.logs import *
from Code.send_email import *
from Code.ETL.ValidationCheck.SchemaCheck import *


class LoansCheck(SchemaCheck):

    @log_start_end
    def check(self):
        email = EmailSender()

        if self.df.empty:
            logging.error("Loans DataFrame is empty - rejecting file")
            email.send_email(
                subject="Loans Check Validation",
                body="Loans DataFrame is empty - rejecting file"
            )
            return False
        self.df.columns = self.df.columns.str.strip()

        expected_columns = ['customer_id', 'loan_type', 'amount_utilized', 'utilization_date', 'loan_reason']

        missing_columns = [col for col in expected_columns if col not in self.df.columns]
        if missing_columns:
            email.send_email(
                subject="Loans Check Validation",
                body="Missing required columns in loans data - rejecting file"
            )
            logging.error(
                f"Missing required columns in loans data: {missing_columns} - rejecting file\n"
                f"Actual columns found: {self.df.columns.tolist()}"
            )
            return False

        if len(self.df) < 1:
            email.send_email(
                subject="Loans Check Validation",
                body="Loans DataFrame has no rows  - rejecting file"
            )
            logging.error("Loans DataFrame has no rows - rejecting file")
            return False

        expected_dtypes = {
            'customer_id': 'object',
            'loan_type': 'object',
            'amount_utilized': 'int64',
            'utilization_date': 'object',
            'loan_reason': 'object'
        }

        type_errors = []
        for col, expected_dtype in expected_dtypes.items():
            if col in self.df.columns and self.df[col].dtype != expected_dtype:
                type_errors.append(f"{col} (expected {expected_dtype}, got {self.df[col].dtype})")

        if type_errors:
            email.send_email(
                subject="Loans Check Validation",
                body="Type mismatches in loans data - rejecting file"
            )
            logging.error(f"Type mismatches in loans data: {', '.join(type_errors)} - rejecting file")
            return False

        logging.info("Loans schema validation passed successfully")
        return True