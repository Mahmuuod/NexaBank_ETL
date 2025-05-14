from .Loader import Loader
import pandas as pd
import os
from Code.logs import *
class LoadCSV(Loader):

    """Class To Export Data As CSV"""

    def __init__(self):
        pass

    @log_start_end
    def load(self,dir:str,df:pd.DataFrame) -> bool :
        try:
            if not os.path.exists(dir):
                df.to_csv(dir,index=False)
                logging.info("Data Loaded Successfully")
                return True
            else:
                return self.append(dir,df)
            
        except Exception as e :
            logging.error(f"Couldn't Load Your Data Into CSV -> {e}")
            return False
        
    @log_start_end
    def append(self,dir:str,df:pd.DataFrame) -> bool:
        try:
            df.to_csv(dir,mode='a',index=False,header=False)
            logging.info("Data Loaded Successfully")
            return True
        except Exception as e :
            logging.error(f"Couldn't Load Your Data Into CSV -> {e}")
            print(f"Couldn't Load Your Data Into CSV -> {e}")
            return False