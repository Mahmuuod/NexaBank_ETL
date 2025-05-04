from ValidationCheck.SchemaCheck import SchemaCheck

class TransactionsCheck(SchemaCheck):
    """Abstract Class To Check transactions Tables Schemas
    
    sender, receiver, transaction_amount, transaction_date
    
    """

    def check(self):
        return super().check()