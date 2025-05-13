from abc import ABC,abstractmethod

class Loader(ABC):
    """Abstract Class For Extraction"""
    
    @abstractmethod
    def load(self) -> bool :
        """Abstract Method For Extracting Data"""
        pass
