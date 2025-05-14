from Load.Loader import *

class LoadParquet(Loader):

    """Class To Export Data As Parquet"""

    def __init__(self):
        pass

    def load(self,dir:str,df:pd.DataFrame) -> bool :
        try:
            if not os.path.exists(dir):
                df.to_parquet(dir,index=False)
                return True
            else:
                return False
        except Exception as e :
            print(f"Couldn't Load Your Data Into Parquet -> {e}")
            return False