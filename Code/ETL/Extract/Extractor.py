import pandas as pd
from abc import ABC,abstractmethod

class Extractor(ABC):
    """Abstract Class For Extraction"""
    @abstractmethod
    def extract(self) ->pd.DataFrame :
        """Abstract Method For Extracting Data"""
        pass