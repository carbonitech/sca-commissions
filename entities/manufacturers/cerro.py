"""
Manufacturer report preprocessing definition
for Cerro Flow
"""

import pandas as pd
import re
import unicodedata
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.Series, **kwargs
    ) -> PreProcessedData:
        """processes the Cerro Flow commission report

        this data comes in an irregular pdf file with no obvious delimiter
        that will predictably separate and align values.
        So extraction is done by relative position to predictable row-wise
        cell contents proximity to the edge, or whether there are numbers
        in the contents
        """

        debugging = True if kwargs.get("debugging") else False
        comm_rate = kwargs.get("standard_commission_rate", 0)
        dollar_figure = r"^(?!^\d$)\$?(\d{1,3}(,\d{3})*|\d+)(\.\d+)?$"
        digits_or_sales_order = r"([0-9]|^(Sales)$|^(order)$)"

        df = (
            data[1:-2]
            .replace(
                r"[\x00-\x1F\x7F]", " ", regex=True
            )  # convert whitespace hex characters into literal white space
            .str.split(
                r"\s+", regex=True, expand=True
            )  # this makes the most predictable result
        )

        def is_hyphen(char: str) -> bool:
            return unicodedata.category(char) == "Pd"

        def create_id_str(row: pd.Series) -> str:
            """use rel position to last LIN and the first cell
            afterward that doesn't have numbers is the beginning
            the customer id cells"""
            try:
                subseries = row[int(row["lin_position"]) + 1 : -1].dropna()
            except:
                # rows with totals do not have lin_position. Just ignore these rows
                return
            id_start = subseries[
                ~subseries.str.contains(digits_or_sales_order, regex=True)
            ].index.min()  # does NOT contain
            id_end = (
                subseries[subseries.str.contains(dollar_figure, regex=True)].index.min()
                - 1
            )
            id_data = subseries.loc[id_start:id_end].replace(r"[0-9]", "", regex=True)
            return "_".join(id_data.values.tolist()).upper()

        def extract_sales_amount(row: pd.Series) -> float:
            """sales is second to last, but the cents
            might be cut off and placed in its own
            cell, pushing the figure further left.

            find the first $ going right-to-left, and recombine
            the cents figure from the next column if needed"""

            sign: int
            sales_fig: str = None
            fig_index = None
            compacted = row.dropna()
            compacted_rev = compacted[::-1]
            for i, val in enumerate(compacted_rev):
                if not isinstance(val, str):
                    continue
                else:
                    val = val.strip()
                    if len(val) == 0:
                        continue
                if val == "$":
                    sales_fig = compacted_rev.iloc[i + 1]
                    sign = 1
                if not sales_fig:
                    if val.startswith("$"):
                        sales_fig = val
                        fig_index = i
                        sign = 1
                    elif is_hyphen(val[0]) and val[1] == "$":
                        sales_fig = val
                        fig_index = i
                        sign = -1
                    elif i == 4:
                        sales_fig = compacted_rev.iloc[i - 1]
                        if sales_fig.startswith("-"):
                            sign = -1
                        else:
                            sign = 1
                if sales_fig:
                    break
            if fig_index:
                num_cent_figures = len(sales_fig.split(".")[-1])
                if num_cent_figures < 2:
                    sales_fig += str(compacted[fig_index + 1])
            if debugging:
                print("sales fig: ", sales_fig)
                print("sign: ", sign)
                print("extracted str: ", re.sub(r"[^.0-9]", "", sales_fig))
                print("result: ", float(re.sub(r"[^.0-9]", "", sales_fig)) * sign, "\n")
            return float(re.sub(r"[^.0-9]", "", sales_fig)) * sign

        def find_rightmost_LIN(row: pd.Series) -> int:
            """used to find and generate id_string contents"""
            no_nan_row = row.dropna()
            return no_nan_row[no_nan_row.str.fullmatch("LIN")].index.max()

        df["lin_position"] = df.apply(find_rightmost_LIN, axis=1)
        df["id_string"] = df.apply(create_id_str, axis=1)
        df.dropna(subset="lin_position", inplace=True)
        df["inv_amt"] = df.apply(extract_sales_amount, axis=1)

        result = df.loc[:, ["id_string", "inv_amt"]]

        result["inv_amt"] = result.loc[:, "inv_amt"].astype(float) * 100
        result["comm_amt"] = result.loc[:, "inv_amt"] * comm_rate
        result = result.astype(self.EXPECTED_TYPES)
        if debugging:
            print("df\n", df.to_string())
            print("result\n", result.to_string())
            raise Exception("catching print on Exception for debugging")
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
