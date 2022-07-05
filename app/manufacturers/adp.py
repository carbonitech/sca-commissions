"""
Manufacturer report processing definition
for Advanced Distributor Products (ADP)
"""
from app.manufacturers.base import Manufacturer
from app.db.db_services import get_mappings

class AdvancedDistributorProducts(Manufacturer):

    reports_by_sheet = {
        'standard': {'sheet_name': 'Detail'},
        'RE Michel POS': {'sheet_name': 'RE Michel', 'skiprows': 2},
        'Coburn POS': {'sheet_name': 'Coburn'},
        'Lennox POS': [{'sheet_name': 'Marshalltown'},
            {'sheet_name': 'Houston'},
            {'sheet_name': 'Carrollton'}]
    }

    def __repr__(self):
        return "ADP"

    def set_report(self, *args, **kwargs): # not sure if this is needed
        pass

    ## these report processing procedures should all run together in a 'default' run
    ## but able to be run independently, not failing on 'missing' sheets
    def process_standard_report(self):
        pass

    def process_coburn_report(self):
        pass

    def process_re_michel_report(self):
        pass

    def process_lennox_report(self):
        pass

    def process_all_reports(self):
        """runs all reports, ignoring errors from 'missing' reports
        and recording errors for unexpected sheets
        returns final commission data"""