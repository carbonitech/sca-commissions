"""
Manufacturer report preprocessing definition
for Tjernlund
"""

import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """processes the Tjernlund standard report"""

        customer_col: str = "address1"
        invoice_num: str = "invoiceno"
        inv_col: str = "extension"
        comm_col_i: int = -1  # depends on removing trailing columns
        comm_col: str = "comm_amt"

        data = self.check_headers_and_fix([customer_col, inv_col], data)
        print(data.columns)
        data.iloc[:, 5:] = data.iloc[:, 5:].shift(
            -1
        )  # line up customer names with values
        data = data.dropna(subset=invoice_num).dropna(axis=1, how="all")
        data[customer_col] = data[customer_col].ffill()
        data = data.rename(columns={data.columns[comm_col_i]: comm_col})
        data[comm_col] = data[comm_col].astype(float)
        data["id_string"] = data[customer_col]
        result = data.loc[:, ["id_string", inv_col, comm_col]]
        result[inv_col] *= 100
        result[comm_col] *= 100
        result = result.rename(columns={inv_col: "inv_amt"})
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def _johnstone_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """process a manually-filled template"""
        customer = "customer"
        city = "city"
        sales = "sales"
        commission = "commission"

        data = self.check_headers_and_fix(
            cols=[customer, city, sales, commission], df=data
        )
        data = data.dropna(subset=data.columns[0])
        data = data.dropna(axis=1, how="all")
        data = data.apply(self.upper_all_str)
        data.loc[:, sales] *= 100
        data.loc[:, commission] *= 100
        data["id_string"] = data[[customer, city]].apply("_".join, axis=1)
        result = data[["id_string", sales, commission]]
        result = result.rename(columns={sales: "inv_amt", commission: "comm_amt"})
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def _re_michel_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """process a manually-filled template"""
        return self._johnstone_report_preprocessing(data, **kwargs)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "johnstone_pos": self._johnstone_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return
