"""
Manufacturer report preprocessing definition
for Nelco
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.Series, **kwargs) -> PreProcessedData:
        """processes the standard Nelco file
        
            this report comes in as a PDF and looks more like a sales report
            heavy amount of parsing required to get to the data, which comes in as one long series
        """

        events = []
        customer_name: int = 2
        city_state_regex: str = r"^([^().\d/_]+?),\s*?(\w{2})" # searches for format like Atlanta, GA at beginning of str
        inv_col: int = -1
        customer_boundary_value: str = "Customer :"
        comm_rate: float = kwargs.get("standard_commission_rate")

        # grab first text element of PDF page header
        data_printed_date = data.iloc[0]
        # remove all text elements matching the header elements in the first 42 text elements
        data_cleaned = data.loc[~data.isin(data[:42])].str.replace(data_printed_date,"")
        # remove page number lines using regex
        data_cleaned = data_cleaned[~data_cleaned.str.contains(r"PAGE:\s*\d")].reset_index(drop=True)
        # use boundary value `customer_boundary_value` to isolate customers
        customer_boundaries: list[int] = data_cleaned.loc[data_cleaned.str.contains(customer_boundary_value)].index.tolist()
        compiled_data = {
            "customer": [],
            "city": [],
            "state": [],
            "inv_amt": []
            # adding comm_amt later
        }
        for i, index in enumerate(customer_boundaries):
            if i == len(customer_boundaries)-1:
                # last customer, go to the end of the dataset, except grand totals
                subseries = data_cleaned.iloc[index:-3]
            else:
                subseries = data_cleaned.iloc[index:customer_boundaries[i+1]]
            
            address = subseries.str.extract(city_state_regex).dropna()
            if not address.empty:
                # some reports have city & state, some only state. this will error-out if address is null
                city, state = address.iloc[0,0], address.iloc[0,1]
            else:
                # this returns a mix of the 2-letter state and UOM's, but the state is always first in the series
                city = None
                state = subseries[subseries.str.len() == 2].to_list()[0]
            inv_amt = float(subseries.iloc[inv_col])
            compiled_data["customer"].append(subseries.iloc[customer_name])
            compiled_data["city"].append(city) 
            compiled_data["state"].append(state) 
            compiled_data["inv_amt"].append(inv_amt)

        result = pd.DataFrame(compiled_data)
        events.append(("Formatting",f"extracted customer, city, state, and sales amount and formatted into a table",self.submission_id))

        result["inv_amt"] = result["inv_amt"]*100
        result.loc[:,"comm_amt"] = result["inv_amt"]*comm_rate
        events.append(("Formatting",f"calculated comm_amt column as {comm_rate*100:,.2f}% of sales",self.submission_id))
        for col in ["customer","city","state"]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        result.columns = self.result_columns
        result = result.dropna(axis=1, how='all') # drops city column, if empty
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(pdf="text"), **kwargs)
        else:
            return
