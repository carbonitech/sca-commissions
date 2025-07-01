"""
Manufacturer report preprocessing definition
for Atlantic Chemical & Equipment (ACE)
"""

import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        data, cols = self.use_column_options(data, **kwargs)
        customer: str = cols.customer
        sales: str = cols.sales
        commissions: str = cols.commissions

        data.dropna(subset=customer, inplace=True)
        data.loc[:, sales] *= 100
        data.loc[:, commissions] *= 100
        data.loc[:, "id_string"] = data[customer] + "__"  # signal missing location data
        result = data[["id_string", sales, commissions]]
        result = result.apply(self.upper_all_str)
        result = result.astype(self.EXPECTED_TYPES)
        self.assert_commission_amounts_match(result, **kwargs)
        return PreProcessedData(result)

    def _johnstone_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        data, cols = self.use_column_options(data, **kwargs)
        customer: str = self.get_customer(**kwargs)
        city: str = cols.city
        state: str = cols.state
        sales: str = cols.sales
        commissions = "comm_amt"
        comm_rate: float = kwargs.get("standard_commission_rate")

        data.dropna(subset=city, inplace=True)
        data.loc[:, sales] *= 100
        data.loc[:, commissions] = data[sales] * comm_rate
        data.loc[:, "id_string"] = data[[customer, city, state]].apply("_".join, axis=1)
        result = data[["id_string", sales, commissions]]
        result = result.apply(self.upper_all_str)
        result = result.astype(self.EXPECTED_TYPES)
        self.assert_commission_amounts_match(result, **kwargs)
        return PreProcessedData(result)

    def _baker_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        data, cols = self.use_column_options(data, **kwargs)
        customer: str = self.get_customer(**kwargs)
        city: str = cols.city
        state: str = cols.state
        sales: str = cols.sales
        commissions = "comm_amt"
        comm_rate: float = kwargs.get("standard_commission_rate")

        data.dropna(subset=city, inplace=True)
        data.loc[:, sales] *= 100
        data.loc[:, commissions] = data[sales] * comm_rate
        data.loc[:, "id_string"] = data[[customer, city, state]].apply("_".join, axis=1)
        result = data[["id_string", sales, commissions]]
        result = result.apply(self.upper_all_str)
        result = result.astype(self.EXPECTED_TYPES)
        self.assert_commission_amounts_match(result, **kwargs)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "johnstone": self._johnstone_report_preprocessing,
            "baker": self._baker_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return
