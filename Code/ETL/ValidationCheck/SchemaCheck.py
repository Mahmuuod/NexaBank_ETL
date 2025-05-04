from abc import ABC,abstractmethod
import pandas as pd

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