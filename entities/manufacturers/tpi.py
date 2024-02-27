"""
Manufacturer report preprocessing definition for TPI Corp.
"""

from numpy import sign
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):
    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        customer = 'customername'
        city = 'shiptocity'
        state = 'shiptostate'
        sales = 'invoiceamt.'
        commissions = 'commissionamt.'

        data = self.check_headers_and_fix(cols=[customer,city,state,sales,commissions], df=data)
        data = data.dropna(subset=data.columns[0])
        data = data.dropna(how='all')
        data = data.apply(self.upper_all_str)
        data.loc[:, sales] *= 100
        data['sales_sign'] = data[sales].apply(sign)
        data.loc[:, commissions] = data[commissions].str.strip()
        data.loc[:, commissions] = data[commissions].str.replace(r'[^0-9.]','',regex=True).astype(float)
        data.loc[:, commissions] *= 100
        data.loc[:, commissions] *= data['sales_sign']
        data['id_string'] = data[[customer,city,state]].apply("_".join, axis=1)
        result = data[['id_string', sales, commissions]]
        result = result.rename(columns={sales: 'inv_amt', commissions: 'comm_amt'})
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return