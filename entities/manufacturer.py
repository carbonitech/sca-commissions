from abc import ABC, abstractmethod

class Manufacturer(ABC):
    """
    defines manufacturer-specific attributes
    and handles report processing execution

    This is a base class for creating manufacturers
    """

    name: str = None # defined by subclasses, manufacturer's db name

    @abstractmethod
    def preprocess():
        pass