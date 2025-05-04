import pandas as pd
from Extract.Extractor import Extractor

class ExtractTXT(Extractor):
    """Class For Extracting TXT"""

    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        return super().extract()