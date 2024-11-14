"""
Manufacturer report preprocessing definition
for Glasfloss
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
        city: str = cols.city
        state: str = cols.state
        sale: str = cols.sales
        commission: str = cols.commissions
        commission: str = "comm_amt"
        comm_rate = kwargs.get("standard_commission_rate", 0)

        total_freight: float = kwargs.get("total_freight_amount", None)
        total_rebate_credits: float = kwargs.get("total_rebate_credits", None)

        data = data.dropna(subset=data.columns[1])
        data.loc[:, sale] *= 100

        if total_rebate_credits:
            total_sales = data[sale].sum() / 100  # converted to dollars
            discount_rate = total_rebate_credits / total_sales
            data.loc[:, sale] = data[sale] * (1 - discount_rate)

        if total_freight:
            total_sales = data[sale].sum() / 100  # converted to dollars
            discount_rate = total_freight / total_sales
            data.loc[:, sale] = data[sale] * (1 - discount_rate)
            data.loc[:, commission] = data[sale] * comm_rate
        else:
            data.loc[:, commission] = 0

        result = data.loc[:, [customer, city, state, sale, commission]]
        col_names = ["customer", "city", "state", "inv_amt", commission]
        result.columns = col_names
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        result = result[["id_string", "inv_amt", commission]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {"standard": self._standard_report_preprocessing}
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return
