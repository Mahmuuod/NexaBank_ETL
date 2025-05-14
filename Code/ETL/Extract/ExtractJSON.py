import time
import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from Code.ETL.Extract.Extractor import Extractor
from Code.logs import *

class ExtractJSON(Extractor):
    def __init__(self):
        pass

    @log_start_end
    def extract(self, fileDir: str) -> pd.DataFrame:
        try:

            if not os.path.exists(fileDir):
                logging.error(f"The file {fileDir} does not exist.")
                raise FileNotFoundError(f"The file {fileDir} does not exist.")

            # Check if the file is locked or in use (retry with delay)
            retries = 5
            for _ in range(retries):
                try:
                    # Try reading the JSON file
                    df = pd.read_json(fileDir)
                    if df.empty:
                        logging.error("No data found in the JSON file")
                        raise ValueError("No data found in the JSON file")
                    
                    logging.info("Transaction Data is Extracted Successfully")
                    return df
                except ValueError:
                    logging.error("No data found in the JSON file")
                    print(f"ValueError: No data found in the JSON file.")
                    raise
                except Exception as e:
                    logging.error(f"Error: {str(e)}. Retrying...")
                    print(f"Error: {str(e)}. Retrying...")
                    time.sleep(1)  # Wait before retrying

            # If file cannot be read after retries
            logging.error(f"Error: {str(e)}. Retrying...")
            
            raise Exception(f"Failed to read the file {fileDir} after multiple attempts.")

        except Exception as e:
            logging.error(f"Error extracting JSON data: {str(e)}")
            print(f"Error extracting JSON data: {str(e)}")
            raise

# def main():
#     file_path = "E:\\ITI 9 Months\\Python\\NexaBank_ETL\\incoming_data\\2025-04-18\\14\\transactions.json"  # Replace with actual JSON file path

#     extractor = ExtractJSON()
#     try:
#         df = extractor.extract(file_path)
#         print("Extracted Data:")
#         print(df.head())
#     except Exception as e:
#         print(f"Extraction failed: {e}")

# if __name__ == "__main__":
#     main()