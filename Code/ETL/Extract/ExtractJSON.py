import pandas as pd
from Extract.Extractor import Extractor

class ExtractJSON(Extractor):
    """Class For Extracting JSON"""
    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        return super().extract()