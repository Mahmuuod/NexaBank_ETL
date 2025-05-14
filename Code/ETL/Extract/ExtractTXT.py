import time
import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from Code.ETL.Extract.Extractor import Extractor
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

# def main():
#     file_path = "E:\\ITI 9 Months\\Python\\NexaBank_ETL\\incoming_data\\2025-04-18\\14\\loans.txt" 
#     separator = "|"  

#     extractor = ExtractTXT()
#     try:
#         df = extractor.extract(file_path, separator)
#         print("Extracted Data:")
#         print(df.head())
#     except Exception as e:
#         print(f"Failed to extract TXT data: {e}")

# if __name__ == "__main__":
#     main()