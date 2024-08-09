"""
Manufacturer report preprocessing definition
for Famco
"""

import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """processes the Famco standard report"""

        data, cols = self.use_column_options(data, **kwargs)

        customer: str = cols["customer"]
        city: str = cols["city"]
        state: str = cols["state"]
        inv_amt: str = cols["sales"]
        comm_amt: str = cols["commissions"]
        invoice_date: str = "invoicedate"

        all_cols = [customer, city, state, inv_amt, comm_amt]
        all_cols = [c for c in all_cols if c]
        id_cols = [c for c in all_cols[:-2] if c]

        data = (
            data.dropna(how="all", axis=1).dropna(how="all").dropna(subset=comm_amt)
        )  # commissions blank for "misc" invoices
        # all prior data is included as of 2024, so filtering needs to be done to get most recent data
        data = data[data[invoice_date].dt.year == data[invoice_date].dt.year.max()]
        data = data[data[invoice_date].dt.month == data[invoice_date].dt.month.max()]
        result = data[all_cols]
        result.loc[:, inv_amt] *= 100.0
        result.loc[:, comm_amt] *= 100.0
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[id_cols].apply("_".join, axis=1)
        result = result[["id_string", inv_amt, comm_amt]]
        result.columns = ["id_string", "inv_amt", "comm_amt"]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def _johnstone_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """processes the Famco Johnstone report"""

        data, cols = self.use_column_options(data, **kwargs)

        customer: str = self.get_customer(**kwargs)
        city: str = cols["city"]
        state: str = cols["state"]
        inv_amt: str = cols["sales"]
        comm_rate = kwargs.get("standard_commission_rate", 0)

        # top line sales and sales detail are on the same tab and separated by a blank column
        # let's use sales detail, since we want to start grabbing product detail anyway
        data = data.iloc[:, 1:].loc[
            :, "vendno":
        ]  # find where sales detail columns start
        data.loc[:, inv_amt] *= 100
        data.loc[:, "comm_amt"] = data[inv_amt] * comm_rate
        data["customer"] = customer
        data = data.apply(self.upper_all_str)
        data["id_string"] = data[["customer", city, state]].apply("_".join, axis=1)
        result_cols = ["id_string", "inv_amt", "comm_amt"]
        result = data[["id_string", inv_amt, "comm_amt"]]
        result.columns = result_cols
        # since for now we're not getting product detail, recreate the top line table by summing
        result = result.groupby("id_string").sum().reset_index()
        result = result[result["inv_amt"] != 0]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "johnstone_pos": self._johnstone_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return
