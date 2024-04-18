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

            (?:DEFAULT|[0-9]+)?
            \s?
        """

        customer_boundary_re: str = r"\d{4} CUST: SHIP-TO"
        page_num_re: str = r"^(Page \d of \d)"
        offset: int = -1
        customer_name_sales_comm_re: str = r"""
            (?P<comm_amt>-?[0-9,]+(?:\.[0-9]*)?)
            \s+
            (?:-?[0-9,]+(?:\.[0-9]*)?)?
            \s*
            TOTAL\ SHIP-TO:\s+(?:[A-Z]*[0-9]*\s)        # using re.VERBOSE means literal spaces used for capture in regex MUST be escaped, or they'll be removed
            (?P<customer>[A-Z\s\t-.,'/]+)
            \s*
            (?P<inv_amt>-?[0-9,]+(?:\.[0-9]*)?)
            .*"""
        city_state_re: str = r"^\d{4}\sCUST:\sSHIP-TO:\s?([A-Z]+)"

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
            "location": [],
            "inv_amt": [],
            "comm_amt": []
        }
        for i, index in enumerate(customer_boundaries):
            if i == len(customer_boundaries)-1:
                # last customer, go to the end of the dataset, except grand totals
                subseries = data.iloc[index:-1]
            else:
                subseries = data.iloc[index:customer_boundaries[i+1]]
            
            city_state = re.match(city_state_re, subseries.iloc[1:3].str.cat(sep='')).group(1)
            # city, state = city_state[:-2], city_state[-2:] # split city from 2-letter state
            # print(subseries[4])
            # print(subseries.str.extract(customer_name_sales_comm_re, re.VERBOSE).dropna())
            customer_inv_comm = subseries.str.extract(customer_name_sales_comm_re, re.VERBOSE).dropna()
            customer: str = customer_inv_comm['customer'].item()
            customer = customer.strip()
            inv_amt = float(customer_inv_comm['inv_amt'].item().replace(",",""))
            comm_amt = float(customer_inv_comm['comm_amt'].item().replace(",",""))

            compiled_data["customer"].append(customer)
            compiled_data["location"].append(city_state)
            compiled_data["inv_amt"].append(inv_amt)
            compiled_data["comm_amt"].append(comm_amt)

        result = pd.DataFrame(compiled_data)
        result["inv_amt"] *= 100
        result["comm_amt"] *= 100
        result = result.apply(self.upper_all_str)
        col_names = ["customer", "location", "inv_amt", "comm_amt"]
        result["id_string"] = result[col_names[:2]].apply("_".join, axis=1)
        result = result[["id_string", "inv_amt", "comm_amt"]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)


    def _unifilter_report_preprocessing(self, data: pd.Series, **kwargs) -> PreProcessedData:
        """report is same exact format as standard"""
        return self._standard_report_preprocessing(data,**kwargs)

    def _splits_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """This scheme is based off of manually converting received files into a basic template"""
        customer = 'customer'
        city = 'city'
        state = 'state'
        inv_amt = 'inv_amt'
        comm_amt = 'comm_amt'
        headers = [customer, city, state, inv_amt, comm_amt]
        data = self.check_headers_and_fix(cols=headers, df=data)
        data = data.dropna(subset=data.columns[0])
        data = data.dropna(how='all', axis=1)
        data[inv_amt] *= 100
        data[comm_amt] *= 100
        data = data.apply(self.upper_all_str)
        data['id_string'] = data[[customer, city, state]].apply("_".join, axis=1)
        result = data[['id_string', inv_amt, comm_amt]]
        result = result.astype(self.EXPECTED_TYPES)
        return PreProcessedData(result)




    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "unifilter": self._unifilter_report_preprocessing,
            "splits": self._splits_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if self.report_name == 'splits':
            return preprocess_method(self.file.to_df(), **kwargs)
        elif preprocess_method:
            return preprocess_method(self.file.to_df(pdf="text"), **kwargs)
