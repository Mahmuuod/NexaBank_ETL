from abc import ABC,abstractmethod
import pandas as pd
import os
class Loader(ABC):
    """Abstract Class For Extraction"""
    
    @abstractmethod
    def load(self) -> bool :
        """Abstract Method For Extracting Data"""
        pass
