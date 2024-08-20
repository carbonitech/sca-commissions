"""
Manufacturer report preprocessing definition
for Atco Flex
"""

import re
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        data, cols = self.use_column_options(data, **kwargs)
        customer: str = cols["customer"]
        city: str = cols["city"]
        state: str = cols["state"]
        sales: str = cols["sales"]
        commissions: str = cols["commissions"]

        data = data.dropna(subset=data.columns[0])
        data[sales] *= 100
        data[commissions] *= 100
        data["id_string"] = data[[customer, city, state]].apply("_".join, axis=1)
        result = (
            data[["id_string", sales, commissions]]
            .rename(columns={sales: "inv_amt", commissions: "comm_amt"})
            .apply(self.upper_all_str)
        )
        result = result.astype(self.EXPECTED_TYPES)
        self.assert_commission_amounts_match(result, **kwargs)
        return PreProcessedData(result)

    def _re_michel_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """
        NOTE: RE MICHEL report comes with the first row of data on the first row, no headers.
        NOTE - 06/29/2023: going forward, headers might be included
        """
        default_customer_name: str = self.get_customer(**kwargs)
        commission_rate = kwargs.get("standard_commission_rate", 0)

        store_number_col: str = "branch#"
        city_name_col: str = "branchlocation"
        state_name_col: str = "state"
        inv_col: str = "amttransferred"
        customer_name_col: str = "customer"

        headers = [store_number_col, city_name_col, state_name_col, inv_col]
        data = self.check_headers_and_fix(headers, data)
        comm_col: str = data.columns[7]

        named_headers: bool = all(header in data.columns for header in headers)

        if not named_headers:
            store_number_col: int = 0
            city_name_col: int = 1
            state_name_col: int = 2
            customer_name_col: int = -1
            inv_col: int = -2

        result_columns = [
            "store_number",
            "customer",
            "city",
            "state",
            "inv_amt",
            "comm_amt",
        ]

        def isolate_city_name(row: pd.Series) -> str:
            city_state: str = row[city_name_col]
            city_state = city_state.upper().split(" ")
            ## since a city could have spaces, rejoin the list up-to but excluding the state str
            ## found in the next column
            return " ".join(
                city_state[: city_state.index(row[state_name_col].upper())]
            ).upper()

        data = data.dropna(subset=data.columns[0])
        data.loc[:, city_name_col] = data.apply(isolate_city_name, axis=1)
        if not named_headers:
            data.iloc[:, inv_col] *= 100
            data["comm_amt"] = data.iloc[:, inv_col] * commission_rate
            inv_col -= 2
            comm_col: int = -2
            result_strategy = data.iloc
        else:
            data.loc[:, inv_col] *= 100
            data.loc[:, comm_col] *= 100
            result_strategy = data.loc

        data["customer"] = default_customer_name
        result = result_strategy[
            :,
            [
                store_number_col,
                customer_name_col,
                city_name_col,
                state_name_col,
                inv_col,
                comm_col,
            ],
        ]
        result = result.apply(self.upper_all_str)
        result.columns = result_columns
        result["store_number"] = result["store_number"].astype(str)
        result["id_string"] = result[result.columns.tolist()[:4]].apply(
            "_".join, axis=1
        )
        result = result.loc[:, ["id_string", "inv_amt", "comm_amt"]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
