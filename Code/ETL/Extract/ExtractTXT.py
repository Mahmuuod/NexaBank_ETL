import pandas as pd
from Extract.Extractor import Extractor

class ExtractTXT(Extractor):

    def __init__(self, fileDir, delimiter=None, encoding='utf-8'):

        self.fileDir = fileDir
        self.delimiter = delimiter
        self.encoding = encoding

    def extract(self) -> pd.DataFrame:
        try:
            # Try to read the file with pandas
            if self.delimiter:
                # If delimiter is specified, use it
                df = pd.read_csv(self.fileDir, delimiter=self.delimiter, encoding=self.encoding)
            else:
                # Try to detect the delimiter automatically
                df = pd.read_csv(self.fileDir, sep=None, engine='python', encoding=self.encoding)
            
            return df
            
        except Exception as e:
            # If pandas reading fails, try reading as fixed-width format
            try:
                df = pd.read_fwf(self.fileDir, encoding=self.encoding)
                return df
            except Exception as e:
                raise Exception(f"Failed to extract data from text file: {str(e)}")
