"""
Manufacturer report preprocessing definition
for Famco
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Famco standard report"""

        customer: str = 'shiptoname'
        city: str = 'shiptocity'
        inv_amt: str = 'sales'
        comm_amt: str = 'commission'
        invoice_date: str = 'invoicedate'

        data = self.check_headers_and_fix([customer, city, invoice_date, inv_amt, comm_amt], data)
        if not set([inv_amt,comm_amt]).intersection(set(data.columns.to_list())):
            inv_amt = 'extsales'
            comm_amt: str = 'extcomm'
        data = data.dropna(how="all",axis=1).dropna(how='all').dropna(subset=comm_amt) # commissions blank for "misc" invoices
        # all prior data is included as of 2024, so filtering needs to be done to get most recent data
        data = data[data[invoice_date].dt.year == data[invoice_date].dt.year.max()]
        data = data[data[invoice_date].dt.month == data[invoice_date].dt.month.max()]
        result = data[[customer, city, inv_amt, comm_amt]]
        result.loc[:,inv_amt] *= 100.
        result.loc[:,comm_amt] *= 100.
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[[customer,city]].apply("_".join, axis=1)
        result = result[["id_string", inv_amt, comm_amt]]
        result.columns = ['id_string', 'inv_amt', 'comm_amt']
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)


    def _johnstone_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Famco Johnstone report"""

        customer: str = self.get_customer(**kwargs)
        city: str = "storename"
        state: str = "storestate"
        inv_amt: str = "lastmocogs"
        comm_rate = kwargs.get("standard_commission_rate",0)

        data = self.check_headers_and_fix(cols=[city,state,inv_amt], df=data)
        if inv_amt not in data.columns:
            inv_amt: str = "lastmosales"
        # top line sales and sales detail are on the same tab and separated by a blank column
        # let's use sales detail, since we want to start grabbing product detail anyway
        data = data.iloc[:,15:]
        data.loc[:,inv_amt] *= 100
        data.loc[:,"comm_amt"] = data[inv_amt]*comm_rate
        data["customer"] = customer
        data = data.apply(self.upper_all_str)
        data["id_string"] = data[['customer',city,state]].apply("_".join, axis=1)
        result_cols = ["id_string", "inv_amt", "comm_amt"]
        result = data[['id_string', inv_amt, 'comm_amt']]
        result.columns = result_cols
        # since for now we're not getting product detail, recreate the top line table by summing
        result = result.groupby('id_string').sum().reset_index()
        result = result[result['inv_amt'] !=0]
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "johnstone_pos": self._johnstone_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return