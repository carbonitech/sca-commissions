"""
Manufacturer report preprocessing definition for Vybond
"""

import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _calculate_commission_amounts(
        self,
        data: pd.DataFrame,
        inv_col: str,
        comm_col: str,
        total_commission: float | None,
    ):
        if total_commission:
            total_sales = data[inv_col].sum() / 100  # converted to dollars
            comm_rate = total_commission / total_sales
            data.loc[:, comm_col] = data[inv_col] * comm_rate
        else:
            data.loc[:, comm_col] = 0
        return data

    def pos_result(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        customer = kwargs.get("customer")
        city = kwargs.get("city")
        state = kwargs.get("state")
        sales = kwargs.get("sales")
        commission = kwargs.get("commission")
        sales_pos = kwargs.get("sales_pos")
        total_comm = kwargs.get("total_comm")

        header_alts: list[tuple] = kwargs.get("header_alts", [(None, None)])

        headers = [city, state]
        data = self.check_headers_and_fix(cols=headers, df=data)

        if not all(header in data.columns for header in headers):
            for alt in header_alts:
                data = self.check_headers_and_fix(cols=list(alt), df=data)
                if all(header in data.columns for header in alt):
                    city, state = alt
                    break
        data = data.dropna(subset=state)
        data = data.dropna(axis=1, how="all")
        if sales_pos:
            data.loc[:, sales] = data.iloc[:, sales_pos] * 100
        elif sales != "inv_amt":
            data.loc[:, sales] *= 100
        else:
            data.loc[:, sales] = data.iloc[:, -3:].sum(axis=1) * 100
        data = self._calculate_commission_amounts(data, sales, commission, total_comm)
        data.loc[:, "customer"] = customer
        result = data.loc[:, ["customer", city, state, sales, commission]]
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[["customer", city, state]].apply("_".join, axis=1)
        if sales != "inv_amt":
            result.rename(columns={sales: "inv_amt"}, inplace=True)
            sales = "inv_amt"
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result[["id_string", sales, commission]])

    def _standard_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        customer: str = "billtoname"
        city: str = "city"
        state: str = "state"
        sales: str = "commissionablesales"
        commission: str = "commission"

        data = data.dropna(subset=data.columns.to_list()[0])
        data = data[data["status"] == "CLSD"]
        result = data.loc[:, [customer, city, state, sales, commission]]
        result.loc[:, sales] *= 100
        result.loc[:, commission] *= 100
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[result.columns[:3]].apply("_".join, axis=1)
        result = result[["id_string", sales, commission]].rename(
            columns={sales: "inv_amt", commission: "comm_amt"}
        )
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def _baker_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        customer: str = self.get_customer(**kwargs)
        city: str = "storename"
        state: str = "storestate"
        sales: str = "inv_amt"
        commission: str = "comm_amt"
        total_comm: float = kwargs.get("total_commission_amount")

        header_alts = [
            ("city", "state")
        ]  # only city and state are used as headers, in this order
        last_col_not_the_sum_of_prior_three = data.loc[
            ~(data.iloc[:, -1] == data.iloc[:, -4:-1].sum(axis=1, numeric_only=True)),
            data.columns[0],
        ].any()
        all_but_last_row_empty_in_rightmost_col = data.iloc[:-1, -1].isna().all()

        if (
            all_but_last_row_empty_in_rightmost_col
            or last_col_not_the_sum_of_prior_three
        ):
            sales_pos = None
        else:
            sales_pos: int = -1

        return self.pos_result(
            data=data,
            customer=customer,
            city=city,
            state=state,
            sales=sales,
            sales_pos=sales_pos,
            commission=commission,
            total_comm=total_comm,
            header_alts=header_alts,
        )

    def _johnstone_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        customer: str = self.get_customer(**kwargs)
        city: str = "storename"
        state: str = "storestate"
        sales_pos = None
        sales: str = "lastmocogs"
        commission: str = "comm_amt"
        total_comm: float = kwargs.get("total_commission_amount")

        return self.pos_result(
            data=data,
            customer=customer,
            city=city,
            state=state,
            sales=sales,
            sales_pos=sales_pos,
            commission=commission,
            total_comm=total_comm,
        )

    def _re_michel_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        customer: str = self.get_customer(**kwargs)
        city: str = "storename"
        state: str = "storestate"
        sales_pos: int = -2
        sales: str = "inv_amt"
        commission: str = "comm_amt"
        total_comm: float = kwargs.get("total_commission_amount")

        header_alts = [
            ("city", "state")
        ]  # only city and state are used as headers, in this order

        return self.pos_result(
            data=data,
            customer=customer,
            city=city,
            state=state,
            sales=sales,
            sales_pos=sales_pos,
            commission=commission,
            total_comm=total_comm,
            header_alts=header_alts,
        )

    def _united_refrigeration_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        customer: str = self.get_customer(**kwargs)
        city: str = "branchname"
        state: str = "state"
        sales_pos = None
        sales: str = "cost"
        commission: str = "comm_amt"
        total_comm: float = kwargs.get("total_commission_amount")

        data.columns = data.columns[:-2].to_list() + ["cost", "perc"]

        return self.pos_result(
            data=data,
            customer=customer,
            city=city,
            state=state,
            sales=sales,
            sales_pos=sales_pos,
            commission=commission,
            total_comm=total_comm,
        )

    def _winsupply_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        customer: str = self.get_customer(**kwargs)
        city: str = "storename"
        state: str = "storestate"
        sales_pos = None
        sales: str = "monthtodatesalesdollars"
        commission: str = "comm_amt"
        total_comm: float = kwargs.get("total_commission_amount", None)

        header_alts = [("billtoaddress1", "billtostate")]

        return self.pos_result(
            data=data,
            customer=customer,
            city=city,
            state=state,
            sales=sales,
            sales_pos=sales_pos,
            commission=commission,
            total_comm=total_comm,
            header_alts=header_alts,
        )

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "baker_pos": self._baker_report_preprocessing,
            "johnstone_pos": self._johnstone_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing,
            "winsupply_pos": self._winsupply_report_preprocessing,
            "united_refrigeration_pos": self._united_refrigeration_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return
