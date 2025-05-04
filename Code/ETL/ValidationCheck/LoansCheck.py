from ValidationCheck.SchemaCheck import SchemaCheck

class LoansCheck(SchemaCheck):
    """Abstract Class To Check loans Tables Schemas
    
    customer_id, loan_type, amount_utilized, utilization_date, loan_reason. 

    """

    def check(self):
        return super().check()