"""
Manufacturer report preprocessing definition
for Genesis Cable / Resideo
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the 'Sales Detail' genesis file"""

        events = []
        customer_name_col: str = "Cust Name"
        city_name_col: str = "City"
        state_name_col: str = "State"
        inv_col: str = "Amount"
        comm_col: str = "Comm Due"

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        result.columns = self.result_columns # local result.cols are same length and position as self.result_columns
        return PreProcessedData(result,events)


    def _baker_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        processes the 'Baker' genesis file
        Remarks:
            - Entire amount is credited to one customer_branch
            - split is applied to both sales and commission
        """

        events = []
        customer_name_col: str = "Customer Name"
        city_name_col: str = "City"
        state_name_col: str = "St."
        inv_col: str = "Amount"
        comm_col: str = "Comm.  Due"

        split: float = kwargs.get("split", 1.0)
        default_branch: dict[str,str] = kwargs.get("default_branch")

        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100*split
        result.loc[:,comm_col] = result[comm_col]*100*split
        events.append(("Formatting",f"replaced {inv_col} column with {split*100:,.2f}% of the values",self.submission_id))
        events.append(("Formatting",f"replaced {comm_col} column with {split*100:,.2f}% of the values",self.submission_id))
        
        # a default is used to credit all sales data to agency. Locations in the data are not territory-specific,
        # although they're used to calculate the values
        total_inv = result[inv_col].sum()
        total_comm = result[comm_col].sum()
        result = pd.DataFrame(
                [
                    list(default_branch.values())
                    +[total_inv, total_comm]
                ],
            columns=self.result_columns)

        return PreProcessedData(result,events)


    def _lennox_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        processes the 'Lennox' genesis file
        Remarks:
        - Lennox report does not have commission amounts in the actual table, only sales.
        - Sales will be scaled down by the split proportion to determine sales
        - Commission amount will be 1% of sales post-split. Column starts as all zeros
        - Entire amount is credited to one customer_branch
        """

        events = []
        customer_name_col: str = "Customer Name"
        city_name_col: str = "City"
        state_name_col: str = "St."
        inv_col: str = "Amount"
        comm_col: str = "Comm.  Due"

        split: float = kwargs.get("split", 1.0)
        default_branch: dict[str,str] = kwargs.get("default_branch")

        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100*split
        result.loc[:,comm_col] = result[inv_col]*0.01 # all reps in this report get 1%, it's the split that varies
        events.append(("Formatting",f"replaced {inv_col} column with {split*100:,.2f}% of the values",self.submission_id))
        events.append(("Formatting",f"replaced {comm_col} column with {0.01*100:,.2f}% of sales",self.submission_id))

        # a default is used to credit all sales data to agency. Locations in the data are not territory-specific,
        # although they're used to calculate the values
        total_inv = result[inv_col].sum()
        total_comm = result[comm_col].sum()
        result = pd.DataFrame(
                [
                    list(default_branch.values())
                    +[total_inv, total_comm]
                ],
            columns=self.result_columns)

        return PreProcessedData(result,events)


    def _winsupply_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        
        events = []
        customer_name_col: str = "BILL TO ADDRESS 1"
        city_name_col: str = "BILL TO ADDRESS 4"
        state_name_col: str = "BILL TO STATE"
        inv_col: str = "MTD SALES $"
        comm_col: str = "comm_amt" 
        comm_col_index: int = -1 # commission is last column but isn't named properly
        

        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        data.loc[:,comm_col] = data.iloc[:,comm_col_index]
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        result.columns = self.result_columns # local result.cols are same length and position as self.result_columns
        return PreProcessedData(result,events)

    def _rebate_detail_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        ...

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "sales_detail": self._standard_report_preprocessing,
            "baker_pos": self._baker_report_preprocessing,
            "lennox_pos": self._lennox_report_preprocessing,
            "winsupply_pos": self._winsupply_report_preprocessing,
            "rebate_detail": self._rebate_detail_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            if self.report_name == "winsupply_pos":
                return preprocess_method(self.file.to_df(combine_sheets=True), **kwargs)
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return