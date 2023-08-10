"""
Manufacturer report preprocessing definition
for General Filters
"""
import re
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.Series, **kwargs) -> PreProcessedData:
        """processes the standard General Filters file
        
            this report comes in as a PDF and looks more like a sales report
            heavy amount of parsing required to get to the data, which comes in as one long series
        """

        customer_boundary_re: str = r"\d{4} CUST: SHIP-TO"
        page_num_re: str = r"^(Page \d of \d)"
        offset: int = -1
        customer_name_sales_comm_re: str = r"([0-9^.,-]+)\s+[0-9^.,-]?\s?TOTAL SHIP-TO:\s(?:DEFAULT|[0-9]+)?\s?(.+?)\s+([0-9^.,-]+)"
        # group 1 (comm_amt), 2(customer),    ^^^^^^^^^^                                                        ^^^     ^^^^^^^^^^
        #       3(inv_amt)
        city_state_re: str = r"^\d{4}\sCUST:\sSHIP-TO:\s(.+)"
        # city state combined without a space            ^^

        data = data[1:]
        data = data.loc[
            ~data.isin(data[:4])
            & ~data.str.contains(page_num_re)
        ].reset_index(drop=True)
        customer_boundaries: list[int] = [
            index+offset 
            for index
            in data.loc[data.str.contains(customer_boundary_re)].index.tolist()
        ]
        compiled_data = {
            "customer": [],
            "city": [],
            "state": [],
            "inv_amt": [],
            "comm_amt": []
        }
        for i, index in enumerate(customer_boundaries):
            if i == len(customer_boundaries)-1:
                # last customer, go to the end of the dataset, except grand totals
                subseries = data.iloc[index:-1]
            else:
                subseries = data.iloc[index:customer_boundaries[i+1]]

            city_state = re.match(city_state_re, subseries.iloc[1]).group(1)
            city, state = city_state[:-2], city_state[-2:] # split city from 2-letter state

            customer_inv_comm: list[str] = subseries.str.extract(customer_name_sales_comm_re).dropna().values[0]
            customer = customer_inv_comm[1]
            inv_amt = float(customer_inv_comm[2].replace(",",""))
            comm_amt = float(customer_inv_comm[0].replace(",",""))

            compiled_data["customer"].append(customer)
            compiled_data["city"].append(city)
            compiled_data["state"].append(state) 
            compiled_data["inv_amt"].append(inv_amt)
            compiled_data["comm_amt"].append(comm_amt)

        result = pd.DataFrame(compiled_data)

        result["inv_amt"] *= 100
        result["comm_amt"] *= 100
        result = result.apply(self.upper_all_str)
        col_names = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        result = result[["id_string", "inv_amt", "comm_amt"]]

        return PreProcessedData(result)


    def _unifilter_report_preprocessing(self, data: pd.Series, **kwargs) -> PreProcessedData:
        """report is same exact format as standard"""
        return self._standard_report_preprocessing(data,**kwargs)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "unifilter": self._unifilter_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(pdf="text"), **kwargs)
