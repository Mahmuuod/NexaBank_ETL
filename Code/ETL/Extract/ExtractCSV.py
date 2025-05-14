import pandas as pd
from .Extractor import Extractor
from Code.logs import *

class ExtractCSV(Extractor):
    def __init__(self):
        pass

    @log_start_end
    def extract(self,fileDir:str) -> pd.DataFrame:
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(fileDir)
            
            if df.empty:
                logging.error("No data found in the CSV file")
                raise ValueError("No data found in the CSV file")
            
            logging.info("Data is Extracted Successfully")
            return df
            
        except Exception as e:
            logging.error(f"Error extracting CSV data: {str(e)}")
            print(f"Error extracting CSV data: {str(e)}")
            raise
