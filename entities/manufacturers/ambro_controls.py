"""
Manufacturer report preprocessing definition
for Ambro Controls
"""

import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """processes the Ambro Controls standard report"""

        customer_name_col: str = "customername"
        city_name_col: str = "shiptocity"
        state_name_col: str = "state"
        state_name_col_alt0: str = "shiptostate"
        state_name_col_alt1: str = "county"
        inv_col: str = "amount"
        comm_col: str = "commissionpayable"

        data = self.check_headers_and_fix(
            [customer_name_col, city_name_col, inv_col], data
        )
        data = data.dropna(subset=data.columns[0])
        possible_state_cols = {state_name_col, state_name_col_alt0, state_name_col_alt1}
        # using set intersection on the col names to assign a name to the state column
        # next -> iter will pick just one name if the intersection has more than one element
        active_state_col = next(iter(set(data.columns) & possible_state_cols))

        result = data.loc[
            :, [customer_name_col, city_name_col, active_state_col, inv_col, comm_col]
        ]
        result.loc[:, [city_name_col, active_state_col]] = result[
            [city_name_col, active_state_col]
        ].fillna("")
        result.loc[:, inv_col] *= 100
        result.loc[:, comm_col] *= 100
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[
            [customer_name_col, city_name_col, active_state_col]
        ].apply("_".join, axis=1)
        result.columns = [
            "customer",
            "city",
            "state",
            "inv_amt",
            "comm_amt",
            "id_string",
        ]
        result = result[["id_string", "inv_amt", "comm_amt"]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def _re_michel_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """process the RE Michel report"""
        customer_name = self.get_customer(**kwargs)
        city_name_col = "city"
        state_name_col = "state"
        sales_amt_col_1 = "remcogs"
        sales_amt_col_2 = "amount"
        comm_col: str = "commissionpayable"
        comm_rate = kwargs.get("standard_commission_rate", 0)

        data = self.check_headers_and_fix([city_name_col, state_name_col], data)
        poss_sales_cols = {sales_amt_col_1, sales_amt_col_2}
        sales_amt_col = next(iter(set(data.columns) & poss_sales_cols))

        data = data.apply(self.upper_all_str)
        data = data.dropna(subset=[city_name_col])
        data["customer"] = customer_name
        data[sales_amt_col] *= 100
        if comm_col in data.columns:
            data["comm_amt"] = data[comm_col] * 100
        else:
            data["comm_amt"] = data[sales_amt_col] * comm_rate
        data["id_string"] = data[["customer", city_name_col, state_name_col]].apply(
            "_".join, axis=1
        )
        result = data[["id_string", sales_amt_col, "comm_amt"]]
        result.columns = ["id_string", "inv_amt", "comm_amt"]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def _uri_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        customer: str = self.get_customer(**kwargs)
        city: str = "branchname"
        state: str = "state"
        inv_amt: str = "amount"
        comm_amt: str = "commissionpayable"

        data = self.check_headers_and_fix([city, state, inv_amt, comm_amt], data)
        data = data.dropna(subset=data.columns[0])
        data["customer"] = customer
        data[inv_amt] *= 100
        data[comm_amt] *= 100
        data["id_string"] = data[["customer", city, state]].apply("_".join, axis=1)
        result = data[["id_string", inv_amt, comm_amt]]
        result = result.rename(columns={inv_amt: "inv_amt", comm_amt: "comm_amt"})
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing,
            "uri_pos": self._uri_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return
