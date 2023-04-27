"""
Manufacturer report preprocessing definition
for superior hvacr
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the standard superior hvacr commission file with an additional file"""

        ## commission file
        customer_name_col_comm_file: str = "customer"
        customer_number_col_comm_file: str = "to"
        inv_col_comm_file: str = "mth_sales"
        comm_col_comm_file: str = "commission"

        ## sales file
        sales_file: pd.DataFrame
        if sales_file := kwargs.get("additional_file_1", None):
            sales_data: pd.DataFrame = pd.read_excel(sales_file, skiprows=1)
            sales_data = sales_data.rename(columns=lambda col: col.lower().replace(" ",""))

        customer_number_col_sales_file: str = "soldto"
        city_name_col_sales_file: str = "city"

        data = data.loc[
            data.loc[:,inv_col_comm_file] > 0,
            [customer_name_col_comm_file, customer_number_col_comm_file, inv_col_comm_file, comm_col_comm_file]]

        sales_data = sales_data.loc[
            ~sales_data.iloc[:,0].isnull(),
            [customer_number_col_sales_file, city_name_col_sales_file]]
        sales_data = sales_data.drop_duplicates()

        result = data.merge(sales_data, left_on=customer_number_col_comm_file, right_on=customer_number_col_sales_file)
        result = result.loc[:, [customer_name_col_comm_file, city_name_col_sales_file, inv_col_comm_file, comm_col_comm_file]]
        result = result.reset_index(drop=True)

        result.loc[:,inv_col_comm_file] *= 100
        result.loc[:,comm_col_comm_file] *= 100

        result = result.apply(self.upper_all_str)

        col_names = ["customer", "city", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[:2]].apply("_".join, axis=1)

        return PreProcessedData(result)


    def _united_refrigeration_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        DEFAULT_NAME = "UNITED REFRIGERATION" # important for trying to auto-match
        store_number: str = "branch"
        city: str = "branchname"
        state: str = "state"
        inv_amt: int = -2
        comm_amt: int = -1

        id_cols = [store_number, city, state]
        data = self.check_headers_and_fix(id_cols, data)
        data = data.apply(self.upper_all_str)
        # fill NaNs in sales and commission columns with 0's
        data.iloc[:,[inv_amt, comm_amt]] = data.iloc[:,[inv_amt, comm_amt]].fillna(0)
        data = data.dropna(subset=data.columns[0])
        data = pd.concat([data.loc[:,id_cols], data.iloc[:,[inv_amt, comm_amt]]], axis=1)
        data = data.groupby(id_cols).sum(numeric_only=True).reset_index()
        data["customer"] = DEFAULT_NAME
        id_cols = [store_number, "customer", city, state]
        data["id_string"] = data[id_cols].apply("_".join, axis=1)
        result = data.iloc[:,-3:]
        result.columns = ["inv_amt", "comm_amt", "id_string"]
        new_col_order = result.columns.to_list()
        new_col_order = [new_col_order.pop()] + new_col_order
        result = result.loc[:,new_col_order]
        
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "uri_report": self._united_refrigeration_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return