import pandas as pd
from Code.Utilities.Encryption import Encryption
from Code.Utilities.Decryption import Decryption
from Code.ETL.Load.LoadCSV import LoadCSV

# vars 
file_dir='./incoming_data/2025-04-18/14/loans.txt'
encrypt_state_dir='./encrypt_state/states.csv'
encrypt_var=Encryption()
write=LoadCSV()
decrypt_var=Decryption()

df = pd.read_csv(file_dir, sep='|') # read the txt file 

encrypt_state,key=Encryption().encrypt(df,"loan_reason")  # encrypt the df
encrypt_state=pd.DataFrame([{"encrypred_dir":file_dir,"key":key}]) # store state
write.load(encrypt_state_dir,encrypt_state)

df2=pd.read_csv("./encrypt_state/states.csv") # read stete

key=df2["key"].loc[df2['encrypred_dir'] == file_dir].iloc[-1] # get the last encryption key for that file
decrypt_state=decrypt_var.decrypt(df,"loan_reason",key) #decrypt the file

print(df)



