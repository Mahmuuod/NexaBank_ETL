import pandas as pd
from .Extractor import *
import os
import time

class ExtractJSON(Extractor):
    def __init__(self):
        pass


    def extract(self, fileDir: str) -> pd.DataFrame:
        try:
            # Check if the file exists before attempting to read it
            if not os.path.exists(fileDir):
                raise FileNotFoundError(f"The file {fileDir} does not exist.")

            # Check if the file is locked or in use (retry with delay)
            retries = 5
            for _ in range(retries):
                try:
                    # Try reading the JSON file
                    df = pd.read_json(fileDir)
                    if df.empty:
                        raise ValueError("No data found in the JSON file")
                    return df
                except ValueError:
                    print(f"ValueError: No data found in the JSON file.")
                    raise
                except Exception as e:
                    print(f"Error: {str(e)}. Retrying...")
                    time.sleep(1)  # Wait before retrying

            # If file cannot be read after retries
            raise Exception(f"Failed to read the file {fileDir} after multiple attempts.")

        except Exception as e:
            print(f"Error extracting JSON data: {str(e)}")
            raise