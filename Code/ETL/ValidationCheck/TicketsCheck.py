from ValidationCheck.SchemaCheck import SchemaCheck

class TicketsCheck(SchemaCheck):
    """Abstract Class To Check support_tickets Tables Schemas

     ticket_id, customer_id, complaint_category, complaint_date, severity.  
    """
    def check(self):
        return super().check()