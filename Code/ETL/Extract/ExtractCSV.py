import pandas as pd
from Extract.Extractor import Extractor

class ExtractCSV(Extractor):
    """Class For Extracting CSV"""
    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        return super().extract()