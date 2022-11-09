"""
Manufacturer report preprocessing definition
for Berry Global
"""
import re
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):
    """
    Remarks:
        - Berry sends a standard with one tab and a POS report file with seperate tabs for each account
        - standard reports are monthly and POS reports are quarterly

        
    Returns: PreProcessedData object with data and attributes set to enable further processing
    """

    def _calculate_commission_amounts(self, data: pd.DataFrame, inv_col: str, comm_col: str, total_commission: float|None, events: list):
        if total_commission:
            total_sales = data[inv_col].sum()/100 # converted to dollars
            comm_rate = total_commission/total_sales
            data.loc[:,comm_col] = data[inv_col]*comm_rate
            events.append(
                ("Formatting",
                f"filled comm_amt column by applying the commission rate {comm_rate*100:.2f}% to inv_amt, "\
                    f"derived from total commissions divided by total sales: "\
                    f"${total_commission:,.2f} / ${total_sales:,.2f} = {comm_rate*100:.2f}%",
                self.submission_id)
            )
        else:
            data.loc[:,comm_col] = 0
            events.append(
                ("Formatting",
                f"added a commission amount column filled with zeros (0) due to no commission amount being supplied",
                self.submission_id)
            )
        return data, events

    def _standard_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:
        
        events = []
        customer_name_col: str = "BILL TO NAME"
        city_name_col: str = "CITY"
        state_name_col: str = "STATE"
        inv_col: str = "COMMISSIONABLE SALES"
        comm_col: str = "COMMISSION"

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        data = data[data["STATUS"] == "CLSD"]
        events.append(("Formatting","kept only rows showing STATUS as CLSD",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        result.columns = self.result_columns # local result.cols are same length and position as self.result_columns
        return PreProcessedData(result,events)

    def _baker_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        events = []
        default_customer_name: str = "BAKER DISTRIBUTING"
        store_number_col: str = "Store #"
        city_name_col: str = "Store Name"
        state_name_col: str = "Store State"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=store_number_col)
        events.append(("Formatting",f"removed all rows with no values in the {store_number_col} column",self.submission_id))
        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()

        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data, events = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm,events)
        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, "comm_amt"]
        ]
        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)

    def _johnstone_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        events = []
        default_customer_name: str = "JOHNSTONE SUPPLY"
        store_number_col: str = "Store #"
        city_name_col: str = "Store Name"
        state_name_col: str = "Store State"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=store_number_col)
        events.append(("Formatting",f"removed all rows with no values in the {store_number_col} column",self.submission_id))
        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda val: re.match(r"JS\s?(\w*\s?\S*)\s?-\s?\d{2,3}",val).group(1).strip())
        events.append(("Formatting",f"extracted city names contained between 'JS' and '-' in the column {city_name_col}",
            self.submission_id)) 
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data, events = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm,events)
        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, "comm_amt"]
        ]
        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)


    def _re_michel_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        This report reveals that two branches can be in the same city-state with different store numbers,
        and marked on the city itself with a compass direction qualifier
        The user could make a canonical city name to keep the distinction.
        As-is, this would appear to be a "duplicated" branch without the context of a store_number,
        causing the merge in a branch_id method to generate as many duplicates
        TODO - make this condition an error condition that sequesters the data 
        """

        events = []
        default_customer_name: str = "RE MICHEL"
        store_number_col: str = "Store #"
        city_name_col: str = "Store Name"
        state_name_col: str = "Store State"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=store_number_col)
        events.append(("Formatting",f"removed all rows with no values in the {store_number_col} column",self.submission_id))
        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda value: value.split("-")[0].strip())
        events.append(("Formatting",f"extracted city names before hyphens ('-'), if any, in the column {city_name_col}",
            self.submission_id)) 
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data, events = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm,events)
        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, "comm_amt"]
        ]
        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)

    def _united_refrigeration_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        the only differences between this method and the RE Michel method 
            - the city name splits on comma instead of hyphen
            - column headers use different names, but relative positioning wrt the table boundaries is the same
            - column drop is by city instead of store number column due to where the summary titles have been placed
        """
        events = []
        default_customer_name: str = "UNITED REFRIGERATION"
        store_number_col: str = "Branch"
        city_name_col: str = "BRANCH NAME"
        state_name_col: str = "STATE"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=city_name_col)
        events.append(("Formatting",f"removed all rows with no values in the {city_name_col} column",self.submission_id))
        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda value: value.split(",")[0].strip())
        events.append(("Formatting",f"extracted city names before commas (','), if any, in the column {city_name_col}",
            self.submission_id)) 
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data, events = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm,events)
        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, "comm_amt"]
        ]
        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)

    def _winsupply_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        events = []
        default_customer_name: str = "WINSUPPLY"
        store_number_col: str = "Store #"
        city_name_col: str = "Store Name"
        state_name_col: str = "Store State"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=store_number_col)
        events.append(("Formatting",f"removed all rows with no values in the {store_number_col} column",self.submission_id))
        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda value: value.split("-")[0].strip())
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda value: value.split(",")[0].strip())
        events.append(("Formatting",f"extracted city names before hyphens(-) or commas(,), if any, in the column {city_name_col}",
            self.submission_id)) 
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data, events = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm,events)
        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, "comm_amt"]
        ]
        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "baker_pos": self._baker_report_preprocessing,
            "johnstone_pos": self._johnstone_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing,
            "united_refrigeration_pos": self._united_refrigeration_report_preprocessing,
            "winsupply_pos": self._winsupply_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return