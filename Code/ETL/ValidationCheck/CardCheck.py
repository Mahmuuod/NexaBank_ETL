from ValidationCheck.SchemaCheck import SchemaCheck

class CardCheck(SchemaCheck):
    """Abstract Class To Check credit_cards_billing Tables Schemas
    
       bill_id, customer_id, month, amount_due, amount_paid, payment_date
    
    """

    def check(self):
        return super().check()