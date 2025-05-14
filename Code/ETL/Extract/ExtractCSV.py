import pandas as pd
from .Extractor import Extractor

class ExtractCSV(Extractor):
    def __init__(self):
        pass

    def extract(self,fileDir:str) -> pd.DataFrame:
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(fileDir)
            
            if df.empty:
                raise ValueError("No data found in the CSV file")
                
            return df
            
        except Exception as e:
            print(f"Error extracting CSV data: {str(e)}")
            raise
