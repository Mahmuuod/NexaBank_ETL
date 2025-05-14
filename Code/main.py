import time
from ETL import *
from Watch_Dog import *
from ETL.Transform.Transform import *
import pandas as pd
from Utilities.churn import *
from ETL.Extract.ExtractCSV import *
from ETL.Extract.ExtractTXT import *
from ETL.Extract.ExtractJSON import *
from ETL.Load.LoadCSV import *
from ETL.ValidationCheck.CardCheck import *
from ETL.ValidationCheck.CustomerCheck import *
from ETL.ValidationCheck.LoansCheck import *
from ETL.ValidationCheck.TicketsCheck import *
from ETL.ValidationCheck.TransactionsCheck import *
import threading
from pathlib import Path


write=LoadCSV()
csv_extractor=ExtractCSV()
txt_extractor=ExtractTXT()
json_extractor=ExtractJSON()

def general_extractor(dir:str):

    file_format=dir.split(".")[1]
    print(file_format)
    if file_format=="csv":
        return csv_extractor.extract(dir)
    elif file_format=="json":
        return json_extractor.extract(dir)
    elif file_format=="txt":
        return txt_extractor.extract(dir,sep='|')
    else:
        print("===========Wrong File Format=============")

def app(dir: str, df=None):
    transform = Transformer()
    Encrypt = Encryption()
    write = LoadCSV()
    encrypt_state_dir = "./encrypt_state/states.csv"
    transform_state_dir = "./Transform_state/states.csv"
    dir_lookup = pd.DataFrame()
    file_name = None
    state = None
    key = None
    
    # check if i worked on this file before
    if os.path.exists(transform_state_dir):
        tmp_df = pd.read_csv(transform_state_dir)
        dir_lookup = tmp_df[tmp_df["dir"] == dir]

    if dir_lookup.empty:
        new_df = general_extractor(dir)
        if CardCheck(new_df).check():
            new_df = transform.transform_billing(new_df)
            file_name = "Card"
        elif LoansCheck(new_df).check():
            new_df = transform.loans_transformations(new_df)
            state, key = Encrypt.encrypt(new_df, "loan_reason")  # encrypt
            file_name = "Loan"
            print(new_df)
        elif CustomerCheck(new_df).check():
            new_df = transform.customer_transformations(new_df)
            file_name = "Customer"
        elif TicketsCheck(new_df).check():
            new_df = transform.tickets_transformations(new_df)
            file_name = "Ticket"
        elif TransactionsCheck(new_df).check():
            new_df = transform.transactions_transformations(new_df)
            file_name = "Transaction"
        else:
            new_df = pd.DataFrame()
            print("Invalid Schema")

        if not new_df.empty:
            def thread_func():  # Fixed the function name here
                path_array=dir.split("/")
                first_dir=path_array[-3]
                second_dir=path_array[-2]

                if not os.path.exists(f"./Transformed_csv/{first_dir}/{second_dir}"):
                    os.makedirs(f"./Transformed_csv/{first_dir}/{second_dir}")

                new_df.to_csv(f"./Transformed_csv/{first_dir}/{second_dir}/{file_name}.csv", index=False)
                write.load(transform_state_dir, pd.DataFrame([{"dir": dir}]))  # load file dir to avoid re transform it
                if state:  # save key
                    encrypt_state = pd.DataFrame([{"dir": dir, "key": key}])
                    write.load(encrypt_state_dir, encrypt_state)
            
            load_thread = threading.Thread(target=thread_func)  # Now matches the defined function
            load_thread.start()


landing=os.getcwd()+'/incoming_data'
def main():
    observer,event_handler=watchDir(landing)
    try :
        while True:
            if event_handler.captured_path:
                file = event_handler.captured_path.get()
                print(f"Detected file path: {file}")
                time.sleep(3)  # Wait for 1 second to ensure the file is fully written
                changed_file = Path(file).as_posix()
                print(f"Normalized file path: {changed_file}")
                
                app(changed_file)
            time.sleep(1)
    except:
        while True:
            if event_handler.captured_path:
                file = event_handler.captured_path.get()
                print(f"Detected file path: {file}")
                time.sleep(3)  # Wait for 1 second to ensure the file is fully written
                changed_file = Path(file).as_posix()
                print(f"Normalized file path: {changed_file}")
                
                app(changed_file)
            time.sleep(1)





if __name__=="__main__":
    main()





"""
    transform=Transformer()
    cards_df=general_extractor("./incoming_data/2025-04-18/14/credit_cards_billing.csv")
    cards_df=transform.transform_billing(cards_df)


    loans_df=general_extractor("./incoming_data/2025-04-18/14/loans.txt")
    loans_df=transform.loans_transformations(loans_df)
    #write=LoadCSV()
    #write.load(dir,df)  store state of encryption

    tickets_df=general_extractor("./incoming_data/2025-04-18/14/support_tickets.csv")
    tickets_df=transform.tickets_transformations(tickets_df)

    transacts_df=general_extractor("./incoming_data/2025-04-18/14/transactions.json")
    transacts_df=transform.transactions_transformations(transacts_df)

    customer_df=general_extractor("./incoming_data/2025-04-18/14/customer_profiles.csv")
    customer_df=transform.customer_transformations(customer_df)


    transform=Transformer()
cards_df=general_extractor("./incoming_data/2025-04-18/14/credit_cards_billing.csv")
check_card=CardCheck(cards_df)
if check_card.check():
    print("true")
else:
    print("false")
cards_df=transform.transform_billing(cards_df)


loans_df=general_extractor("./incoming_data/2025-04-18/14/loans.txt")
loans_df=transform.loans_transformations(loans_df)
#write=LoadCSV()
#write.load(dir,df)  store state of encryption

tickets_df=general_extractor("./incoming_data/2025-04-18/14/support_tickets.csv")
tickets_df=transform.tickets_transformations(tickets_df)

transacts_df=general_extractor("./incoming_data/2025-04-18/14/transactions.json")
transacts_df=transform.transactions_transformations(transacts_df)

customer_df=general_extractor("./incoming_data/2025-04-18/14/customer_profiles.csv")
customer_df=transform.customer_transformations(customer_df)
"""