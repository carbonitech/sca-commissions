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
        """processes the standard Glasfloss file"""

        customer_name_col: str = "shiptoname"
        city_name_col: str = "shiptocity"
        state_name_col: str = "shiptostate"
        inv_col: str = "sumofsales"
        comm_col: str = "comm_amt"
        total_freight: float = kwargs.get("total_freight_amount", None)
        total_rebate_credits: float = kwargs.get("total_rebate_credits", None)
        comm_rate = kwargs.get("standard_commission_rate", 0)

        data = self.check_headers_and_fix(
            [customer_name_col, city_name_col, state_name_col, inv_col], data
        )
        data = data.dropna(subset=data.columns.to_list()[1])
        data.loc[:, inv_col] *= 100

        if total_rebate_credits:
            total_sales = data[inv_col].sum() / 100  # converted to dollars
            discount_rate = total_rebate_credits / total_sales
            data.loc[:, inv_col] = data[inv_col] * (1 - discount_rate)

        if total_freight:
            total_sales = data[inv_col].sum() / 100  # converted to dollars
            discount_rate = total_freight / total_sales
            data.loc[:, inv_col] = data[inv_col] * (1 - discount_rate)
            data.loc[:, comm_col] = data[inv_col] * comm_rate
        else:
            data.loc[:, comm_col] = 0

        result = data.loc[
            :, [customer_name_col, city_name_col, state_name_col, inv_col, comm_col]
        ]
        col_names = ["customer", "city", "state", "inv_amt", comm_col]
        result.columns = col_names
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        result = result[["id_string", "inv_amt", comm_col]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {"standard": self._standard_report_preprocessing}
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return
