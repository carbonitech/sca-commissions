"""
Manufacturer report preprocessing definition
for Agas
"""

import pandas as pd
from numpy import nan
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing_pdf(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """
        processes the standard Agas file
            Data arrives from the to_df method reindexed on both axes
        """

        customer_name_col: int = 0
        city_name_col: int = 1
        inv_col: int = -4
        comm_col: int = -1

        def right_justify_part_of_df(
            df: pd.DataFrame, split_index: int
        ) -> pd.DataFrame:
            last_col = df.iloc[:, -1]
            nan_rows_i = list(last_col[last_col.isna()].index)
            if nan_rows_i:
                shift_amount = (
                    df.columns[-1]
                    - df.loc[nan_rows_i].dropna(axis=1, how="all").columns[-1]
                )  # expects col names to be 0-based integer sequence
                df.iloc[nan_rows_i, split_index:] = df.iloc[
                    nan_rows_i, split_index:
                ].shift(shift_amount, fill_value=nan, axis=1)
            return df

        # make sure "Unnamed #" cells from reindexing are treated as NA
        data = (
            data.replace(r"Unnamed: \d+", nan, regex=True)
            .dropna(subset=data.columns[2])
            .reset_index(drop=True)
        )
        # customer id info is in first 2 cols, the rest we don't care about
        right_justify_part_of_df(data, 3)  # modifies dhe df that goes in
        data = data.iloc[:, [customer_name_col, city_name_col, inv_col, comm_col]]

        data.columns = ["customer", "city", "inv_amt", "comm_amt"]
        # from here, all the variable from above have been replaced by strings
        customer_name_col, city_name_col, inv_col, comm_col = data.columns.tolist()
        # convert string currency figure to float
        data.loc[:, inv_col] = data[inv_col].replace(r"^\(", "-", regex=True)
        data.loc[:, inv_col] = data[inv_col].replace(r"[^-.0-9]", "", regex=True)
        data = data.loc[~(data[inv_col] == "")]
        data[inv_col] = data[inv_col].astype(float)
        # convert string currency figure to float
        data.loc[:, comm_col] = data[comm_col].replace(r"^\(", "-", regex=True)
        data.loc[:, comm_col] = (
            data[comm_col].replace(r"[^-.0-9]", "", regex=True).astype(float)
        )

        data.loc[:, inv_col] *= 100
        data.loc[:, comm_col] *= 100
        data = data.apply(self.upper_all_str)

        data = data.dropna(axis=1, how="all")
        data["id_string"] = data[[customer_name_col, city_name_col]].apply(
            "_".join, axis=1
        )

        result = data.loc[:, ["id_string", inv_col, comm_col]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def _standard_report_preprocessing_xlsx(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:

        data, cols = self.use_column_options(data, **kwargs)

        customer: str = cols.customer
        city: str = cols.city
        sales: str = cols.sales
        commissions: str = cols.commissions

        data = self.check_headers_and_fix([customer, city, sales, commissions], data)
        data = data.dropna(subset=data.columns[1])
        data[sales] *= 100
        data[commissions] *= 100
        data["id_string"] = data[[customer, city]].apply("_".join, axis=1)
        result = data[["id_string", sales, commissions]]
        result = result.rename(columns={sales: "inv_amt", commissions: "comm_amt"})
        result = result.apply(self.upper_all_str)
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing_pdf,
            "standard_excel": self._standard_report_preprocessing_xlsx,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            if "excel" in self.report_name:
                return preprocess_method(self.file.to_df(), **kwargs)
            else:
                return preprocess_method(
                    self.file.to_df(pdf="table", combine_sheets=True), **kwargs
                )
        else:
            return
