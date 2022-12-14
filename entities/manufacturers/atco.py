"""
Manufacturer report preprocessing definition
for Atco Flex
"""
import re
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):
    """
    Remarks:
        - Atco's report comes in one file with 2 tabs: "Cash Receipt" and "Invoices"
        - Column headings are different between them, so a "split" switch is used to get a dict of DataFrames

    Returns: PreProcessedData object with data and attributes set to enable further processing
    """

    def _standard_report_preprocessing(self, data_dict: dict[str,pd.DataFrame], **kwargs) -> PreProcessedData:
        
        events = []

        ## for Cash Receipt Tab
        cr_tab_name_re: str = r"cash\s?rec[ei]{2}pt" # use -i flag for case insensitivity
        cr_customer_name_col: str = "Customer Name"
        cr_city_name_col: str = "Ship To City"
        cr_state_name_col: str = "Ship To State"
        cr_inv_col: str = "Net Cash"
        cr_comm_col: str = "Commission"

        ## for Invoices Tab
        inv_tab_name_re: str = r"inv[oi]{2}ces?" # use -i flag for case insensitivity
        inv_customer_name_col: str = "Sort Name"
        inv_city_name_col: str = "Ship To City"
        inv_state_name_col: str = "Ship to State"
        inv_inv_col: str = "Ttl Sales Less Frt and EPD"
        inv_comm_col: str = "Commission Earned"

        # std cols
        inv_col = self.result_columns[-2]
        comm_col = self.result_columns[-1]

        df_list = []
        for sheet, df in data_dict.items():
            if re.match(cr_tab_name_re, sheet, 2):
                extracted_data = df.loc[:,
                    [cr_customer_name_col,
                    cr_city_name_col,
                    cr_state_name_col,
                    cr_inv_col,
                    cr_comm_col]
                ]
                extracted_data.columns = self.result_columns
                df_list.append(extracted_data)
            elif re.match(inv_tab_name_re, sheet, 2):
                # move the special row value that sums POS to customer name so it isn't dropped later
                df = df.dropna(subset=["Invoice"])
                df["Invoice"] = df["Invoice"].astype(str)
                df.loc[df["Invoice"].str.contains(r"[^0-9]"),inv_customer_name_col] = df.loc[df["Invoice"].str.contains(r"[^0-9]"),"Invoice"].values
                extracted_data = df.loc[:,
                    [inv_customer_name_col,
                    inv_city_name_col,
                    inv_state_name_col,
                    inv_inv_col,
                    inv_comm_col]
                ]
                extracted_data.columns = self.result_columns
                df_list.append(extracted_data)
        
        if not df_list:
            raise Exception("no data loaded")
        
        data = pd.concat(df_list)

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        data.loc[:,inv_col] = data[inv_col].fillna(0)
        data.loc[:,inv_col] = data[inv_col]*100
        data.loc[:,comm_col] = data[comm_col]*100
        for col in self.result_columns[:3]:
            data.loc[:, col] = data[col].str.upper()
            data.loc[:, col] = data[col].str.strip()
        return PreProcessedData(data,events)

    def _re_michel_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        default_customer_name: str = "RE MICHEL"
        commission_rate = kwargs.get("standard_commission_rate", 0)

        events = []
        store_number_col: int = 0
        city_name_col: int = 1
        state_name_col: int = 2
        inv_col: int = 7
        customer_name_col: int = -1
        comm_col: int = customer_name_col-1

        def isolate_city_name(row: pd.Series) -> str:
            city_state = row[city_name_col].split(" ")
            return " ".join(city_state[:city_state.index(row[state_name_col])]).upper()

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        data.iloc[:,city_name_col] = data.apply(isolate_city_name, axis=1)
        events.append(("Formatting",f"isolated city name in column {str(city_name_col+1)} by keeping everything up to the state name",self.submission_id))
        data.iloc[:,inv_col] = data.iloc[:,inv_col]*100
        data["comm_amt"] = data.iloc[:,inv_col]*commission_rate
        events.append(("Formatting",f"added commissions column by calculating {commission_rate*100:,.2f}% of the inv_amt",
            self.submission_id))
        data["customer"] = default_customer_name

        result = data.iloc[:,[store_number_col, customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            if self.report_name == "standard":
                return preprocess_method(self.file.to_df(split_sheets=True), **kwargs)
            return preprocess_method(self.file.to_df(), **kwargs)