import time
from ETL import *
from Watch_Dog import *
from ETL.Transform.Transform import *
import pandas as pd
from Utilities.churn import *

transform=Transformer()
cards_df=pd.read_csv("./incoming_data/2025-04-18/14/credit_cards_billing.csv")
loans_df=pd.read_csv("./incoming_data/2025-04-18/14/loans.txt",sep="|")

tickets_df=pd.read_csv("./incoming_data/2025-04-18/14/support_tickets.csv")
transacts_df=pd.read_json("./incoming_data/2025-04-18/14/transactions.json")

customer_df=pd.read_csv("./incoming_data/2025-04-18/14/customer_profiles.csv")
customer_df=transform.customer_transformations(customer_df)
ticket_check_df = pd.merge(
       customer_df[["customer_id", "customer_segment"]],
        cards_df[["customer_id", "payment_date"]],
        on="customer_id",
        how="left"
    )

cahrn=Churn(loans_df,cards_df,tickets_df,transacts_df,customer_df)
print(cahrn.count_charners())
x=ss
landing=os.getcwd()+'/Final Project/incoming_data'

def main():
    observer,event_handler=watchDir(landing)
    while True:
        if  event_handler.captured_path :
            print(event_handler.captured_path.get())
        time.sleep(1)




if __name__=="__main__":
    main()



