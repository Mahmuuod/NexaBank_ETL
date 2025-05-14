import time
import sys
import os
from logs import *
from ETL import *
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
from Code.Watch_Dog import *


landing=os.getcwd()+'/incoming_data'
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
            state, key = Encrypt.encrypt(new_df, "loan_reason") 
            file_name = "Loan"

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
            email = EmailSender()
            email.send_email(
                subject="Schema Check Validation",
                body="Schema doesnt match - rejecting file"
            )
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

def get_files(first_dir,second_dir)->pd.DataFrame:
    result_df=[]
    parent_dir = f"./Transformed_csv/{first_dir}/{second_dir}"
    files=os.listdir(parent_dir)
    files_count=len(files)


    if files_count==5:
        for i in range(files_count):
            files[i]=parent_dir+"/"+files[i]
        for file in files:
            result_df.append(pd.read_csv(file))
    return result_df

def analysis(list:list[pd.DataFrame]):
    count=Churn(card_df=list[0],customer_df=list[1],loan_df=list[2],ticket_df=list[3],transaction_df=list[4])
    print(count.count_charners())
def main():
    observer,event_handler=watchDir(landing)
    try :
        while True:
            if event_handler.captured_path:
                file = event_handler.captured_path.get()

                print(f"Detected file path: {file}")
                time.sleep(3)  # Wait for 1 second to ensure the file is fully written
                changed_file = Path(file).as_posix()
                path_array=changed_file.split("/")
                first_dir=path_array[-3]
                second_dir=path_array[-2]
                print(f"Normalized file path: {changed_file}")
                
                app(changed_file)
                time.sleep(2)
                transformed_files=get_files(first_dir,second_dir)

                files_count=len(transformed_files)
                print(files_count)
                if files_count==5 :
                    analysis(transformed_files)
            time.sleep(1)


    except:
        while True:
            if event_handler.captured_path:
                file = event_handler.captured_path.get()
                email.send_email(
                subject="Pipline Failed",
                body="Pipline failed - retrying"
                )
                print(f"Detected file path: {file}")
                time.sleep(3)  # Wait for 1 second to ensure the file is fully written
                changed_file = Path(file).as_posix()
                path_array=changed_file.split("/")
                first_dir=path_array[-3]
                second_dir=path_array[-2]
                print(f"Normalized file path: {changed_file}")
                
                app(changed_file)
                time.sleep(2)
                transformed_files=get_files(first_dir,second_dir)

                files_count=len(transformed_files)
                print(files_count)
                if files_count==5 :
                    analysis(transformed_files)
            time.sleep(1)


if __name__=="__main__":
    main()


