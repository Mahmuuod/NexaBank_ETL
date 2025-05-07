from .Loader import Loader
import pandas as pd
import os
class LoadCSV(Loader):

    """Class To Export Data As CSV"""

    def __init__(self):
        pass

    def load(self,dir:str,df:pd.DataFrame) -> bool :
        try:
            if not os.path.exists(dir):
                df.to_csv(dir,index=False)
                return True
            else:
                return self.append(dir,df)
            
        except Exception as e :
            print(f"Couldn't Load Your Data Into CSV -> {e}")
            return False
    def append(self,dir:str,df:pd.DataFrame) -> bool:
        try:
            df.to_csv(dir,mode='a',index=False,header=False)
            return True
        except Exception as e :
            print(f"Couldn't Load Your Data Into CSV -> {e}")
            return False