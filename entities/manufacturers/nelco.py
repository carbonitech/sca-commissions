"""
Manufacturer report preprocessing definition
for Nelco
"""

import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.Series, **kwargs
    ) -> PreProcessedData:
        """processes the standard Nelco file

        this report comes in as a PDF and looks more like a sales report
        heavy amount of parsing required to get to the data, which comes in as one long series
        """

        for i, value in data.items():
            if value == "Sorted By :":
                sort_strategy = data.loc[i + 1]
                match sort_strategy:
                    case "Salesman":
                        return self._sorted_by_salesman_strategy(data, **kwargs)
                    case "Customer":
                        return self._sorted_by_customer_strategy(data, **kwargs)

    def _sorted_by_salesman_strategy(
        self, data: pd.Series, **kwargs
    ) -> PreProcessedData:
        invoice_date = r"\d{2}/\d{2}/\d{2}"
        comm_rate: float = kwargs.get("standard_commission_rate")

        # grab first text element of PDF page header
        data_printed_date = data.iloc[0]
        # remove all text elements matching the header elements in the first 42 text elements
        data_cleaned = data.loc[~data.isin(data[:42])].str.replace(
            data_printed_date, ""
        )
        # remove page number lines using regex
        data_cleaned = data_cleaned[
            ~data_cleaned.str.contains(r"PAGE:\s*\d")
        ].reset_index(drop=True)
        invoice_boundaries: list[int] = data_cleaned.loc[
            data_cleaned.str.match(invoice_date)
        ].index.tolist()
        compiled_data = {
            "customer": [],
            "city": [],
            "state": [],
            "inv_amt": [],
            # adding comm_amt later
        }
        for i, index in enumerate(invoice_boundaries):
            if i == len(invoice_boundaries) - 1:
                # last customer, go to the end of the dataset, except grand totals
                subseries = data_cleaned.iloc[index:-3]
            else:
                subseries = data_cleaned.iloc[index : invoice_boundaries[i + 1]]

            customer = subseries.iloc[3]
            city = ""
            state = subseries.iloc[4]
            inv_label_index = subseries[
                subseries.str.match("Invoice Totals")
            ].index.item()
            inv_amt = float(subseries.loc[inv_label_index + 2])

            compiled_data["customer"].append(customer)
            compiled_data["city"].append(city)
            compiled_data["state"].append(state)
            compiled_data["inv_amt"].append(inv_amt)

        result = pd.DataFrame(compiled_data)
        result.loc[:, "inv_amt"] *= 100
        result.loc[:, "comm_amt"] = result.loc[:, "inv_amt"] * comm_rate
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[["customer", "city", "state"]].apply(
            "_".join, axis=1
        )
        result = result[["id_string", "inv_amt", "comm_amt"]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def _sorted_by_customer_strategy(
        self, data: pd.Series, **kwargs
    ) -> PreProcessedData:
        """processes the standard Nelco file

        this report comes in as a PDF and looks more like a sales report
        heavy amount of parsing required to get to the data, which comes in as one long series
        """

        customer_name: int = 2
        city_state_regex: str = (
            r"^([^().\d/_]+?),\s*?(\w{2})"  # searches for format like Atlanta, GA at beginning of str
        )
        inv_col: int = -1
        customer_boundary_value: str = "Customer :"
        comm_rate: float = kwargs.get("standard_commission_rate")

        # grab first text element of PDF page header
        data_printed_date = data.iloc[0]
        # remove all text elements matching the header elements in the first 42 text elements
        data_cleaned = data.loc[~data.isin(data[:42])].str.replace(
            data_printed_date, ""
        )
        # remove page number lines using regex
        data_cleaned = data_cleaned[
            ~data_cleaned.str.contains(r"PAGE:\s*\d")
        ].reset_index(drop=True)
        # use boundary value `customer_boundary_value` to isolate customers
        customer_boundaries: list[int] = data_cleaned.loc[
            data_cleaned.str.contains(customer_boundary_value)
        ].index.tolist()
        compiled_data = {
            "customer": [],
            "city": [],
            "state": [],
            "inv_amt": [],
            # adding comm_amt later
        }
        for i, index in enumerate(customer_boundaries):
            if i == len(customer_boundaries) - 1:
                # last customer, go to the end of the dataset, except grand totals
                subseries = data_cleaned.iloc[index:-3]
            else:
                subseries = data_cleaned.iloc[index : customer_boundaries[i + 1]]

            address = subseries.str.extract(city_state_regex).dropna()
            if not address.empty:
                # some reports have city & state, some only state. this will error-out if address is null
                city, state = address.iloc[0, 0], address.iloc[0, 1]
            else:
                city = ""  # allows for a str join for the id_string without having to drop the column
                ## this returns a mix of the 2-letter state and UOM's, but the state is always the first in the series
                state = subseries[subseries.str.len() == 2].to_list()[0]
            inv_amt = float(subseries.iloc[inv_col])
            compiled_data["customer"].append(subseries.iloc[customer_name])
            compiled_data["city"].append(city)
            compiled_data["state"].append(state)
            compiled_data["inv_amt"].append(inv_amt)

        result = pd.DataFrame(compiled_data)
        result.loc[:, "inv_amt"] *= 100
        result.loc[:, "comm_amt"] = result.loc[:, "inv_amt"] * comm_rate
        result = result.apply(self.upper_all_str)

        col_names = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[:3]].apply(
            "_".join, axis=1
        )  # empty city col makes this 'customer__state'
        result = result[result.columns[-3:]].astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(pdf="text"), **kwargs)
        else:
            return
