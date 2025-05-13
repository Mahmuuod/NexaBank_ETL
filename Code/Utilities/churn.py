import pandas as pd
from datetime import datetime
class Churn:
    """No credit card payment for 90+ days

    No transactions (money transfers/purchases) for 90+ days

    No loan activity for 90+ days

    No support tickets filed for 90+ days"""

    def __init__(self,loan_df:pd.DataFrame,card_df:pd.DataFrame,ticket_df:pd.DataFrame,transaction_df:pd.DataFrame,customer_df:pd.DataFrame):
        self.loan_df=loan_df
        self.card_df=card_df
        self.ticket_df=ticket_df
        self.transaction_df=transaction_df
        self.customer_df=customer_df

    
    def formatting_credit_card(self):
        ticket_check_df = pd.merge(self.customer_df[["customer_id","customer_segment"]],self.card_df[["customer_id", "payment_date"]],on="customer_id",how="left")
        ticket_check_df["payment_date"] = pd.to_datetime(ticket_check_df["payment_date"])

        current_date = pd.to_datetime(datetime.now().date())

        result_df = ticket_check_df.groupby(["customer_id","customer_segment"], as_index=False).agg(last_payment_date=("payment_date", "max"))

        result_df["days_since_payment_date"] = (current_date - result_df["last_payment_date"]).dt.days

        return result_df[["customer_id","customer_segment" ,"days_since_payment_date"]]

    def formatting_ticket(self):
        ticket_check_df = pd.merge(self.customer_df[["customer_id","customer_segment"]],self.ticket_df[["customer_id", "complaint_date"]],on="customer_id",how="left")
        ticket_check_df["complaint_date"] = pd.to_datetime(ticket_check_df["complaint_date"])

        current_date = pd.to_datetime(datetime.now().date())

        result_df = ticket_check_df.groupby(["customer_id","customer_segment"], as_index=False).agg(last_complaint_date=("complaint_date", "max"))

        result_df["days_since_last_complaint"] = (current_date - result_df["last_complaint_date"]).dt.days

        return result_df[["customer_id", "customer_segment" ,"days_since_last_complaint"]]

    def formatting_loan(self):
        ticket_check_df = pd.merge(self.customer_df[["customer_id","customer_segment"]],self.loan_df[["customer_id", "utilization_date"]],on="customer_id",how="left")
        ticket_check_df["utilization_date"] = pd.to_datetime(ticket_check_df["utilization_date"])

        current_date = pd.to_datetime(datetime.now().date())

        result_df = ticket_check_df.groupby(["customer_id","customer_segment"], as_index=False).agg(utilization_date=("utilization_date", "max"))

        result_df["days_since_utilization_date"] = (current_date - result_df["utilization_date"]).dt.days

        return result_df[["customer_id", "customer_segment" ,"days_since_utilization_date"]]
    
    def formatting_transaction(self):
        ticket_check_df = pd.merge(self.customer_df[["customer_id","customer_segment"]],self.transaction_df[["sender", "transaction_date"]],left_on="customer_id",right_on="sender",how="left")
        ticket_check_df["transaction_date"] = pd.to_datetime(ticket_check_df["transaction_date"])

        current_date = pd.to_datetime(datetime.now().date())

        result_df = ticket_check_df.groupby(["customer_id","customer_segment"], as_index=False).agg(transaction_date=("transaction_date", "max"))

        result_df["days_since_transaction_date"] = (current_date - result_df["transaction_date"]).dt.days

        return result_df[["customer_id", "customer_segment" ,"days_since_transaction_date"]]
      
    def ticket_check(self):
        df=self.formatting_ticket()
        df=df[df["days_since_last_complaint"]>90]
        return df[["customer_id","customer_segment"]]

    def credit_card_check(self):
        df=self.formatting_credit_card()
        df=df[df["days_since_payment_date"]>90]
        return df[["customer_id","customer_segment"]]
    
    def loan_check(self):
        df=self.formatting_loan()
        df=df[df["days_since_utilization_date"]>90]
        return df[["customer_id","customer_segment"]]
    
    def transaction_check(self):
        df=self.formatting_transaction()
        df=df[df["days_since_transaction_date"]>90]
        return df[["customer_id","customer_segment"]]




    def churned(self):
        transact_df=self.transaction_check()
        loan_df=self.loan_check()
        credit_card_df=self.credit_card_check()
        ticket_df=self.ticket_check()
        result_df=transact_df.merge(loan_df).merge(credit_card_df).merge(ticket_df)
        return result_df


    def count_charners(self):
        charners_df=self.churned()
        return charners_df.groupby("customer_segment",as_index=False).agg(count=("customer_segment", "count"))