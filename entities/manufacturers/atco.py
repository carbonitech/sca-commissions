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
        
        ## for Cash Receipt Tab
        cr_tab_name_re: str = r"cash\s?rec[ei]{2}pt" # use -i flag for case insensitivity
        cr_customer_name_col: str = "customername"
        cr_city_name_col: str = "shiptocity"
        cr_state_name_col: str = "shiptostate"
        cr_inv_col: str = "netcash"
        cr_comm_col: int = -2
        cr_comm_col_named: str = "commissionsunique"

        ## for Invoices Tab
        inv_tab_name_re: str = r"inv[oi]{2}ces?" # use -i flag for case insensitivity
        inv_customer_name_col: str = "sortname"
        inv_city_name_col: str = "shiptocity"
        inv_state_name_col: str = "shiptostate"
        inv_inv_col: str = "ttlsaleslessfrtandepd"
        inv_comm_col: str = "commissionearned"

        # std cols
        inv_col = "inv_amt"
        comm_col = "comm_amt"
        result_columns = ["customer", "city", "state", inv_col, comm_col]

        df_list = []
        for sheet, df in data_dict.items():
            if re.match(cr_tab_name_re, sheet, 2):
                df.columns.values[cr_comm_col] = cr_comm_col_named
                extracted_data = df.loc[:,
                    [cr_customer_name_col,
                    cr_city_name_col,
                    cr_state_name_col,
                    cr_inv_col,
                    cr_comm_col_named]
                ]
                extracted_data.columns = result_columns
                df_list.append(extracted_data)
            elif re.match(inv_tab_name_re, sheet, 2):
                # move the special row value that sums POS to customer name so it isn't dropped later
                # removing because REM POS is back
                # df = df.dropna(subset=["Invoice"])
                # df["Invoice"] = df["Invoice"].astype(str)

                # df.loc[df["Invoice"].str.contains(r"[^0-9]"),inv_customer_name_col] = df.loc[df["Invoice"].str.contains(r"[^0-9]"),"Invoice"].values
                extracted_data = df.loc[:,
                    [inv_customer_name_col,
                    inv_city_name_col,
                    inv_state_name_col,
                    inv_inv_col,
                    inv_comm_col]
                ]
                extracted_data.columns = result_columns
                df_list.append(extracted_data)
        
        if not df_list:
            raise Exception("no data loaded")
        
        data = pd.concat(df_list, ignore_index=True)
        data = data.dropna(subset=data.columns.to_list()[0])
        data[inv_col] = data[inv_col].fillna(0)
        data[inv_col] *= 100
        data[comm_col] *= 100
        data["id_string"] = data[result_columns[:3]].apply("_".join, axis=1)
        result = data.loc[:,["id_string"] + result_columns[-2:]].apply(self.upper_all_str)
        return PreProcessedData(result)


    def _re_michel_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
            NOTE: RE MICHEL report comes with the first row of data on the first row, no headers.
        """
        default_customer_name: str = "RE MICHEL"
        commission_rate = kwargs.get("standard_commission_rate", 0)

        store_number_col: int = 0
        city_name_col: int = 1
        state_name_col: int = 2
        inv_col: int = 7
        customer_name_col: int = -1
        comm_col: int = customer_name_col-1
        result_columns = ["customer", "city", "state", "inv_amt", "comm_amt"]

        def isolate_city_name(row: pd.Series) -> str:
            city_state: str = row[city_name_col]
            city_state = city_state.upper().split(" ")
            ## since a city could have spaces, rejoin the list up-to but excluding the state str
            ## found in the next column
            return " ".join(city_state[:city_state.index(row[state_name_col].upper())]).upper()

        data = data.dropna(subset=data.columns.to_list()[0])
        data.iloc[:,city_name_col] = data.apply(isolate_city_name, axis=1)
        data.iloc[:,inv_col] *= 100
        data["comm_amt"] = data.iloc[:,inv_col]*commission_rate
        data["customer"] = default_customer_name

        result = data.iloc[:,[store_number_col, customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result = result.apply(self.upper_all_str)
        result.columns = ["store_number"] + result_columns
        result["store_number"] = result["store_number"].astype(str)
        result["id_string"] = result[result.columns.tolist()[:4]].apply("_".join, axis=1)
        result = result.loc[:,["id_string","inv_amt","comm_amt"]]
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            if self.report_name == "standard":
                return preprocess_method(self.file.to_df(split_sheets=True, treat_headers=True), **kwargs)
            return preprocess_method(self.file.to_df(make_header_a_row=True), **kwargs)