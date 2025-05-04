from ValidationCheck.SchemaCheck import SchemaCheck

class CustomerCheck(SchemaCheck):
    """Abstract Class To Check Tables Schemas

        customer_id, name, gender, age, city, account_open_date, product_type, 
        customer_tier. 
    
    """
    def check(self):
        return super().check()