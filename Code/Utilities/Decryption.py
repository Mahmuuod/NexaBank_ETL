import random as rn
import string as s
import pandas as pd
class Decryption:

    """You Can use this class if you want to encrypt a text with ceaser ciprher"""
    def __init__(self):
        self.letters=s.ascii_lowercase
    def old_index(self,letter:str,key:int) -> int:
        """ This Func gets the the index of the encrypted char """

        index=self.letters.find(letter)
        new_index=(index-key)
        if new_index <0:
            new_index+=26
        return new_index

    def get_letters(self,df:pd.DataFrame) -> list[str]:
        return df["loan_reason"].astype(str).to_list()
    
    def get_encrypted_letters(self,encrypted:list[str]) -> pd.DataFrame:
        return pd.Series(encrypted)
    def check_values(self,col:str,df:pd.DataFrame)-> bool:
        df[col].map(type).eq(str).all()

    def decrypt(self,df:pd.DataFrame,col:str,key:int)-> bool:
        """This func encrypts the df col"""
        if(not self.check_values(col,df)):
            False

        letters=self.get_letters(df)
        encrypted_letter=''
        encrypted_letters=[]
       

        for letter in letters:
            for char in letter.lower():
                if char in self.letters:
                    new_index=self.old_index(char,key)
                    encrypted_letter+=self.letters[new_index]
                else:
                    encrypted_letter+= char
            encrypted_letters.append(encrypted_letter)
            encrypted_letter=''
        df[col]=self.get_encrypted_letters(encrypted_letters)
        return True


