import pandas as pd
from abc import ABC,abstractmethod
from datetime import datetime
import json
import time
import csv
import os
import shutil


"""first we have landing data , we check it and add time stamp then we move it to staging then we apply transformations """



class Extractor(ABC):
    """Abstract Class For Extraction"""
    @abstractmethod
    def extract(self) ->pd.DataFrame :
        """Abstract Method For Extracting Data"""
        pass

class ExtractJSON(Extractor):
    """Class For Extracting JSON"""
    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        return super().extract()
    
class ExtractCSV(Extractor):
    """Class For Extracting CSV"""
    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        return super().extract()
    

class ExtractTXT(Extractor):
    """Class For Extracting TXT"""

    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        return super().extract()
    

class Loader(ABC):
    """Abstract Class For Extraction"""
    @abstractmethod
    def load(self) -> bool :
        """Abstract Method For Extracting Data"""
        pass

class LoadHDFS(Loader):

    """Class To Load Data To HDFS"""

    def __init__(self,hdfsPath):
        self.hdfsPath=hdfsPath

    def load(self) -> bool :
        try:
        
            return True
        except Exception as e :
            print(f"Couldn't Load Your Data Into HDFS -> {e}")
            return False

class LoadParquet(Loader):

    """Class To Export Data As Parquet"""

    def __init__(self,dir):
        self.dir=dir

    def load(self) -> bool :
        try:
            return True
        except Exception as e :
            print(f"Couldn't Load Your Data Into HDFS -> {e}")
            return False
        

class Transformer:
    def __init__(self):
        pass

class SchemaCheck(ABC):
    """ Abstract Class Checks The Schema Validation
       o Corrupted or invalid data files. 
       o Files with incorrect schemas. 
       o Extra/unnecessary data files. """

    def __init__(self, df:pd.DataFrame):
        self.df=df
    @abstractmethod
    def check(self) -> bool:
        """Check Mehod"""
        pass

class CustomerCheck(SchemaCheck):
    """Abstract Class To Check Tables Schemas

        customer_id, name, gender, age, city, account_open_date, product_type, 
        customer_tier. 
    
    """
    def check(self):
        return super().check()
    

class TicketsCheck(SchemaCheck):
    """Abstract Class To Check support_tickets Tables Schemas

     ticket_id, customer_id, complaint_category, complaint_date, severity.  
    """
    def check(self):
        return super().check()

class CardCheck(SchemaCheck):
    """Abstract Class To Check credit_cards_billing Tables Schemas
    
       bill_id, customer_id, month, amount_due, amount_paid, payment_date
    
    """

    def check(self):
        return super().check()
    
class LoansCheck(SchemaCheck):
    """Abstract Class To Check loans Tables Schemas
    
    customer_id, loan_type, amount_utilized, utilization_date, loan_reason. 

    """

    def check(self):
        return super().check()
    
class TransactionsCheck(SchemaCheck):
    """Abstract Class To Check transactions Tables Schemas
    
    sender, receiver, transaction_amount, transaction_date
    
    """

    def check(self):
        return super().check()
    







