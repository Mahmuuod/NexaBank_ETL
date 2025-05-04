from Load.Loader import Loader

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