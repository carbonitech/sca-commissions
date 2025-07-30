"""
Processing for Allied
"""

import pandas as pd
import numpy as np
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor
from numpy import nan


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.Series, **kwargs
    ) -> PreProcessedData:
        """processes the Allied standard report"""

        customer_name_col: str = "customer"
        inv_col: str = "inv_amt"
        comm_rate: float = kwargs.get("standard_commission_rate", 0)

        # reduce to a slice, using the series value that contains the "Sales Data" header,
        # but leave out the header and totals rows
        data: pd.Series = data[
            data.index[data.str.match(r"^\s*Sales Data")].tolist()[0] :
        ]
        match data.iloc[1].strip():
            case "-":
                slice_ = slice(2, -1)
            case val if len(val) > 1:
                slice_ = slice(1, -1)
        data = data.iloc[slice_]
        df = (
            pd.DataFrame(
                data.str.split(r"\s{2,}", regex=True).to_list()
            )  # best col delimiter for now is 2+ spaces
            .replace(r"RH\d{5}\s", "", regex=True)
            .replace("\s?\-\s?", 0, regex=True)
            .replace(
                r"\(([0-9,]*)\)", "-\\1", regex=True
            )  # numbers surrounded by parens are negative
            .replace(
                r"([0-9])\,([0-9])", "\\1\\2", regex=True
            )  # remove comma notation within numbers, using this regex
            # because customer and first col of values are still combined
        )
        df = df[df.iloc[:, 0] != 0]
        df = df.loc[df.iloc[:, 1:].ne(0).any(axis=1)]
        # drop columns with all zeros
        df = df.loc[:, (df != 0).any(axis=0)]
        # the first column of numbers is separated only by 1 space from the customer name
        # so we'll use a letter-space-number combo in the first col to separate and replace the first column
        df = pd.concat(
            [df.pop(0).str.extract(r"([^0-9]*)\s(-?[0-9]+|-)", expand=True), df],
            axis=1,
            ignore_index=True,
        )
        df = df.replace(r"^-$", 0, regex=True)
        df[df.columns[1:]] = df.iloc[:, 1:].astype(float)
        # if the last column has a 0, pull the right-most value over to the totals column
        # and remove it from it's prior position in the table
        replace_values: pd.Series = (
            df.iloc[:, 1:]
            .apply(
                lambda row: (
                    [row.index[row > 0].max(), row.loc[row.index[row > 0].max()]]
                    if row.iloc[-1] == 0
                    else 0
                ),
                axis=1,
            )
            .replace(0, np.nan)
            .dropna()
        )
        if not replace_values.empty:
            for index, val in replace_values.items():
                source_index = val[0]
                dollar_amount = val[1]
                df.loc[index, df.columns[source_index]] = 0
                df.loc[index, df.columns[-1]] = dollar_amount
        # keep only lastest month
        df = df.iloc[:, [0, -2]]
        df.columns = [customer_name_col, inv_col]
        df[inv_col] *= 100.00
        df["comm_amt"] = df[inv_col] * comm_rate
        df = df.apply(self.upper_all_str)
        # tag duplicated names with suffixes
        dup_names = df.loc[df.duplicated(subset="customer"), ["customer"]].astype(str)
        dup_names = dup_names.reset_index().reset_index().set_index("index")
        dup_names["customer"] += "-" + dup_names["level_0"].astype(
            str
        )  # level_0 is the column name assigned to the column created by the second reset_index call
        df.loc[dup_names.index, "customer"] = dup_names["customer"]
        df["id_string"] = df[customer_name_col]
        result = df.iloc[:, -3:].dropna()
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(
                self.file.to_df(pdf="text", combine_sheets=True), **kwargs
            )
        else:
            return
