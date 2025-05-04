from Load.Loader import Loader

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