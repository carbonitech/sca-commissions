"""
Manufacturer report preprocessing definition
for JB Industries
"""

import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """processes the standard JB file"""

        customer: str = "name"
        city: str = "city"
        state: str = "code"
        sales: str = "grosssale"
        commissions: str = "cmsnamount"

        data = data.dropna(subset=data.columns[0])
        result = data.loc[:, [customer, city, state, sales, commissions]]
        result.loc[
            (result[customer].isna())
            & (result[sales].isna())
            & (result[commissions].lt(0)),
            [customer, city, state],
        ] = ["UNMAPPED"] * 3
        result = result.groupby(result.columns[:3].to_list()).sum().reset_index()
        result.loc[:, sales] *= 100
        result.loc[:, commissions] *= 100
        result["id_string"] = result[result.columns[:3]].apply("_".join, axis=1)
        result = (
            result[["id_string", sales, commissions]]
            .apply(self.upper_all_str)
            .rename(columns={sales: "inv_amt", commissions: "comm_amt"})
            .astype(self.EXPECTED_TYPES)
        )
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
