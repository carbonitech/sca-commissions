"""
Manufacturer report preprocessing definition
for Cerro Flow
"""
import pandas as pd
import numpy as np
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.Series, **kwargs) -> PreProcessedData:
        """processes the Cerro Flow commission report"""

        comm_rate = kwargs.get("standard_commission_rate",0)

        # using the unicode space and literal space characters present in the data, split into a df of arbitrary size
        data: pd.DataFrame = data[1:].str.split(r"[(\xa0)+|\s]", regex=True, expand=True)
        # excluding the bottom two summary rows, replace empty '' spaces with NaN
        data = data.iloc[:-2,:].replace('',np.nan).dropna(axis=1,how="all")
        data.columns = list(range(len(data.columns.tolist())))
        # condense the table into it's tightest possible form
        data = data.apply(lambda row: row.dropna().reset_index(drop=True),axis=1)

        def fix_dollar_numbers(row: pd.Series, *args, **kwargs) -> pd.Series:
            row_short = row.dropna()
            row_short.iloc[-7:] = row_short.iloc[-7:].replace('[^-.0-9]','', regex=True)
            return row_short
        
        def extract_id(row: pd.Series, *args, **kwargs) -> str:
            row_short = row.dropna()
            # picking an arbitrary column to match with for which the distance
            # between it and the first customer info column is constant
            pre_index = list(row_short[row_short.str.fullmatch("LIN")].index)[-1]
            true_id_start = pre_index + 3
            # there are always 7 number columns after the end of the customer info columns
            id_data = row_short.iloc[true_id_start:-7]
            id_data = '_'.join(id_data.values.tolist()).upper()
            return id_data

        data = data.apply(fix_dollar_numbers, axis=1)
        data["id_string"] = data.apply(extract_id, axis=1)

        result = data.loc[:,["id_string", 24]]
        result.columns = ["id_string", "inv_amt"]

        result.loc[:,"inv_amt"] *= 100
        result.loc[:,"comm_amt"] = result.loc[:,"inv_amt"]*comm_rate

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