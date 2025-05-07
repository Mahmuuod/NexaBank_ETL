import random as rn
import string as s
import pandas as pd
class Encryption:

    """You Can use this class if you want to encrypt a dataFrame with ceaser ciprher"""
    def __init__(self):
        self.letters=s.ascii_lowercase

    def new_index(self,char:str,key:int) -> int:
        """ This Func gets the the index of the encrypted char """
        
        index=self.letters.find(char)
        new_index=(index+key) % 26
        return new_index
    
    def get_letters(self,df:pd.DataFrame) -> list[str]:
        """takes series and returns list of strings"""
        return df["loan_reason"].astype(str).to_list()
    
    def get_encrypted_letters(self,encrypted:list[str]) -> pd.DataFrame:
        """takes list of strings and returns df"""
        return pd.Series(encrypted)
    def check_values(self,col:str,df:pd.DataFrame)-> bool:
        """checks if the col values are string"""
        df[col].map(type).eq(str).all()

    def encrypt(self,df:pd.DataFrame,col:str)-> tuple[bool,int]:
        """This func encrypts the df col"""
        if(not self.check_values(col,df)):
            False

        letters=self.get_letters(df)
        encrypted_letter=''
        encrypted_letters=[]
        encrypt_key=rn.randint(1,25)
       

        for letter in letters:
            for char in letter.lower():
                if char in self.letters:
                    new_index=self.new_index(char,encrypt_key)
                    encrypted_letter+=self.letters[new_index]
                else:
                    encrypted_letter+= char
            encrypted_letters.append(encrypted_letter)
            encrypted_letter=''
        df[col]=self.get_encrypted_letters(encrypted_letters)
        return True,encrypt_key

