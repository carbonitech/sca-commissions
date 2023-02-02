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

        events = []

        ## commission file
        customer_name_col_comm_file: str = "customer"
        customer_number_col_comm_file: str = "to"
        inv_col_comm_file: str = "mth_sales"
        comm_col_comm_file: str = "commission"

        ## sales file
        sales_file: pd.DataFrame
        if sales_file := kwargs.get("additional_file_1", None):
            sales_data: pd.DataFrame = pd.read_excel(sales_file, skiprows=1)

        customer_number_col_sales_file: str = "soldto"
        city_name_col_sales_file: str = "city"

        data.columns = [col.lower().strip() for col in data.columns]
        sales_data.columns = [col.lower().strip() for col in sales_data.columns]

        data = data.loc[
            data.loc[:,inv_col_comm_file] > 0,
            [customer_name_col_comm_file, customer_number_col_comm_file, inv_col_comm_file, comm_col_comm_file]]

        sales_data = sales_data.loc[
            ~sales_data.iloc[:,0].isnull(),
            [customer_number_col_sales_file, city_name_col_sales_file]]
        sales_data = sales_data.drop_duplicates()

        result = data.merge(sales_data, left_on=customer_number_col_comm_file, right_on=customer_number_col_sales_file)
        result = result.loc[:, [customer_name_col_comm_file, city_name_col_sales_file, inv_col_comm_file, comm_col_comm_file]]

        for col in [customer_name_col_comm_file,city_name_col_sales_file]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()

        col_names = self.result_columns.copy()
        col_names.pop(2) # remove "state"
        result.columns = col_names
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return