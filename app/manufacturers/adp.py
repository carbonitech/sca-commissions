"""
Manufacturer report processing definition
for Advanced Distributor Products (ADP)
"""
from app.manufacturers.base import Manufacturer

class AdvancedDistributorProducts(Manufacturer):

    reports_by_sheet = {
        'standard': {'sheet_name': 'Detail'},
        'RE Michel POS': {'sheet_name': 'RE Michel', 'skiprows': 2},
        'Coburn POS': {'sheet_name': 'Coburn'},
        'Lennox POS': {'sheet_name': 'Marshalltown'}

    }

    def __repr__(self):
        return "ADP"

    def set_report(self, *args, **kwargs): # not sure if this is needed
        pass

    ## these report processing procedures should all run together in a 'default' run
    ## but able to be run independently. (Maybe by not failing on 'missing' sheets)
    def standard_report_processing(self):
        pass

    def coburn_report_processing(self):
        pass

    def re_michel_report_processing(self):
        pass

    def lennox_report_processing(self):
        pass

    def combined_report_processing(self):
        """runs all reports, ignoring errors from 'missing' reports"""