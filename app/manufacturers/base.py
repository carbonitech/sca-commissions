"""Defines manufacturer and submission base classes"""

class Manufacturer:
    """
    defines manufacturer-specific attributes
    and handles report processing execution
    """
    pass
    

class Submission:
    """
    handles report processing:
    tracks the state of submission attributes such as id,
    errors, processing steps, etc. to use in post-processing
    database operations

    takes a Manufacturer object in the constructor to access
    manufacturer attributes and report processing procedures
    """
    id = None
    errors = None
    processing_steps = None

    def __init__(self, manufacturer: Manufacturer, file: bytes) -> None:
        self.manufacturer = manufacturer
        self.file = file

    def process(self):
        return

