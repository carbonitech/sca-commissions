"""
Manufacturer report preprocessing definition
for Milwaukee Tool
"""

import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor
from logging import getLogger

logger = getLogger("uvicorn.info")


class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(
        self, data: pd.DataFrame, **kwargs
    ) -> PreProcessedData:
        """processes the Milwaukee Full Detail List tab"""

        customer_name_col: str = "customername"
        city_name_col: str = "city"
        state_name_col: str = "state"
        inv_col: str = "proratedsalesamt"
        inv_col_alt: str = "salesamount"
        comm_col: str = "commission"

        if missed_transfers := kwargs.get("additional_file_1", None):
            missed_transfers_df: pd.DataFrame = pd.read_excel(missed_transfers)
            missed_transfers_df = missed_transfers_df.rename(
                columns=lambda col: col.lower().replace(" ", "")
            )
            data = pd.concat([data, missed_transfers_df])

        target_cols = [
            customer_name_col,
            city_name_col,
            state_name_col,
            inv_col,
            comm_col,
        ]
        data = data.dropna(subset=customer_name_col)
        try:
            result = data.loc[:, target_cols]
        except KeyError as e:
            if inv_col in str(e):
                try:
                    target_cols[target_cols.index(inv_col)] = inv_col_alt
                    inv_col = inv_col_alt
                    result = data.loc[:, target_cols]
                except Exception as e:
                    logger.critical("Failed to match the sales column by name")
                    logger.critical(e)
                    raise e

        result[inv_col] = pd.to_numeric(result[inv_col], errors="coerce").fillna(0)
        result[comm_col] = pd.to_numeric(result[comm_col], errors="coerce").fillna(0)

        result = result.groupby(result.columns.tolist()[:3]).sum().reset_index()
        result.columns = target_cols
        result.loc[:, inv_col] *= 100
        result.loc[:, comm_col] *= 100
        result = result.apply(self.upper_all_str)

        col_names = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        result = result[["id_string", "inv_amt", "comm_amt"]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "full_detail_list": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return
