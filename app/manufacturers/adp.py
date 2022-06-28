"""
Manufacturer report processing definition
for Advanced Distributor Products (ADP)
"""
from app.manufacturers.base import Manufacturer

class AdvancedDistributorProducts(Manufacturer):

    reports = {
        "standard": {"sheet_name": "Shupe Carboni"}
    }

    def __repr__(self):
        return "ADP"

    def set_report(self, report_name):
        pass

    def standard_report_processing(self):
        pass

    def process(self):
        pass