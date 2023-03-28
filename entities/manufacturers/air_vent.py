"""
Manufacturer report preprocessing definition
for Agas
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.Series, **kwargs) -> PreProcessedData:
        """processes the standard Nelco file
        
            this report comes in as a PDF and looks more like a sales report data.
            Data states as one long series of strings
        """

        customer_boundary_value: str = r"\d{6}\s+([a-zA-Z ]*?)\s{2,}([a-zA-Z ]*?)\s{2,}([A-Z]{2})" # format to find 6-digit number, then customer name, city, and state - grouped
        inv_line_item_re: str = r"INVOICE TOTAL\s+([0-9^,.-]+)\s+([0-9^,.-]+)\s+([0-9^,.-]+)"

        customer_col_name: str = "customer"
        city_col_name: str = "city"
        state_col_name: str = "state"
        inv_col_name: str = "inv"
        comm_col_name: str = "comm"

        customer_boundaries: list[int] = data.loc[data.str.contains(customer_boundary_value)].index.tolist()
        dfs = []
        for i, index in enumerate(customer_boundaries):
            if i == len(customer_boundaries)-1:
                # last customer, go to the end of the dataset, except grand totals
                subseries = data.iloc[index:-1]
            else:
                subseries = data.iloc[index:customer_boundaries[i+1]]
            customer_info = subseries.str.extract(customer_boundary_value).dropna().rename(
                columns={0:customer_col_name, 1: city_col_name, 2: state_col_name}
            )
            amounts = subseries.str.extract(inv_line_item_re).dropna().replace('[^-.0-9]','', regex=True) # extract numbers and replace chars not used in a float number
            amounts = amounts.replace('([0-9^.]+)(-)?$',r'\2\1', regex=True).astype(float) # put trailing minus sign in front if it's there
            amounts[inv_col_name] = amounts[0] - amounts[1]
            amounts = amounts.rename(columns={2:comm_col_name})
            sales_comm = amounts[[inv_col_name, comm_col_name]]
            combined = pd.concat([customer_info,sales_comm], axis=1).fillna(method="ffill").dropna().reset_index(drop=True)
            dfs.append(combined)

        result = pd.concat(dfs, ignore_index=True)

        result[inv_col_name] *= 100
        result[comm_col_name] *= 100
        for col in [customer_col_name, city_col_name, state_col_name]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        result.columns = [customer_col_name, city_col_name, state_col_name, inv_col_name, comm_col_name]
        result["id_string"] = result[[customer_col_name, city_col_name, state_col_name]].apply("_".join, axis=1)
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
