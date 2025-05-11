import pandas as pd
from Extract.Extractor import Extractor

class ExtractCSV(Extractor):
    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(self.fileDir)
            
            if df.empty:
                raise ValueError("No data found in the CSV file")
                
            return df
            
        except Exception as e:
            print(f"Error extracting CSV data: {str(e)}")
            raise
