import pandas as pd
from Extract.Extractor import Extractor

class ExtractTXT(Extractor):
    """Class For Extracting TXT"""

    def __init__(self,fileDir):
        self.fileDir=fileDir

    def extract(self) -> pd.DataFrame:
        try:
            # Read the TXT file
            with open(self.fileDir, 'r') as file:
                lines = file.readlines()
            
            # Process lines into a list of dictionaries
            data = []
            headers = lines[0].strip().split() if lines else []
            
            for line in lines[1:]:
                values = line.strip().split()
                if len(values) == len(headers):
                    row_dict = dict(zip(headers, values))
                    data.append(row_dict)
            
            # Convert to DataFrame
            if data:
                return pd.DataFrame(data)
            else:
                raise ValueError("No valid data found in the TXT file")
                
        except Exception as e:
            print(f"Error extracting TXT data: {str(e)}")
            raise
