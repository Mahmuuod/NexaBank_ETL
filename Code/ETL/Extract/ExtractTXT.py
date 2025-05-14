import pandas as pd
from .Extractor import Extractor

class ExtractTXT(Extractor):
    """Class For Extracting TXT"""

    def __init__(self):
        pass

    def extract(self,fileDir:str,sep:str) -> pd.DataFrame:
        try:
            # Read the txt file into a DataFrame
            df = pd.read_csv(fileDir,sep=sep)
            
            if df.empty:
                raise ValueError("No data found in the txt file")
                
            return df
            
        except Exception as e:
            print(f"Error extracting txt data: {str(e)}")
            raise