"""
Manufacturer report preprocessing definition
for Glasfloss
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the standard Glasfloss file"""

        events = []
        customer_name_col: str = "Ship To Name"
        city_name_col: str = "Ship To City"
        state_name_col: str = "Ship To State"
        inv_col: str = "Sum of Sales"
        comm_col: str = "comm_amt"
        total_freight: float = kwargs.get("total_freight_amount", None)
        comm_rate = 0.03    # TODO: HAVE THIS SUPPLIED AS AN ARGUMENT

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))

        data.loc[:,inv_col] = data[inv_col]*100
        if total_freight:
            total_sales = data[inv_col].sum()/100 # converted to dollars
            discount_rate = total_freight/total_sales
            data.loc[:,inv_col] = data[inv_col]*(1-discount_rate)
            data.loc[:,comm_col] = data[inv_col]*comm_rate
            events.append(
                ("Formatting",
                f"reduced {inv_col} amounts proportional to ${total_freight:,.2f}/${total_sales:,.2f}, {discount_rate*100:,.2f}%",
                self.submission_id)
            )
            events.append(
                ("Formatting",
                f"Calculated {comm_col} using {comm_rate*100:,.2f}% on {inv_col}",
                self.submission_id)
            )
        else:
            data.loc[:,comm_col] = 0
            events.append(
                ("Formatting",
                f"added a commission amount column filled with zeros (0) due to no total freight amount being supplied",
                self.submission_id)
            )

        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.columns = self.result_columns # local result.cols are same length and position as self.result_columns
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