import pandas as pd
import json
from Extract.Extractor import Extractor

class ExtractJSON(Extractor):
    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        try:
            # Read the JSON file
            with open(self.fileDir, 'r') as file:
                try:
                    # Try to load as a single JSON object or array
                    data = json.loads(file.read())
                    
                    # Convert to DataFrame based on data type
                    if isinstance(data, list):
                        return pd.DataFrame(data)
                    else:
                        return pd.DataFrame([data])
                        
                except json.JSONDecodeError:
                    # If single load fails, try reading line by line
                    file.seek(0)
                    json_objects = []
                    for line in file:
                        try:
                            json_obj = json.loads(line.strip())
                            json_objects.append(json_obj)
                        except json.JSONDecodeError:
                            continue
                    
                    if json_objects:
                        return pd.DataFrame(json_objects)
                    else:
                        raise ValueError("No valid JSON data found in the file")
                        
        except Exception as e:
            print(f"Error extracting JSON data: {str(e)}")
            raise
