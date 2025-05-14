import pandas as pd
from .Extractor import Extractor
from Code.logs import *

class ExtractTXT(Extractor):
    """Class For Extracting TXT"""

    def __init__(self):
        pass

    @log_start_end
    def extract(self,fileDir:str,sep:str) -> pd.DataFrame:
        try:
            # Read the txt file into a DataFrame
            df = pd.read_csv(fileDir,sep=sep)
            
            if df.empty:
                logging.error("No data found in the Loans txt file")
                raise ValueError("No data found in the txt file")
            
            logging.info("Loans Data is Extracted Successfully")
            return df
            
        except Exception as e:
            logging.error(f"Error extracting txt data: {str(e)}")

            print(f"Error extracting txt data: {str(e)}")
            raise