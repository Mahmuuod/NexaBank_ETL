import os
import sys

import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from Code.ETL.Extract.Extractor import Extractor
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

def main():
    file_path = "E:\\ITI 9 Months\\Python\\NexaBank_ETL\\incoming_data\\2025-04-18\\14\\credit_cards_billing.csv"  

    extractor = ExtractCSV()
    df = extractor.extract(file_path)
    print("Extracted Data:")
    print(df.head())  

if __name__ == "__main__":
    main()