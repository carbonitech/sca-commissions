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

    def _calculate_commission_amounts(self, 
            data: pd.DataFrame,
            inv_col: str,
            comm_col: str,
            total_commission: float|None):
        if total_commission:
            total_sales = data[inv_col].sum()/100 # converted to dollars
            comm_rate = total_commission/total_sales
            data.loc[:,comm_col] = data[inv_col]*comm_rate
        else:
            data.loc[:,comm_col] = 0
        return data

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        
        customer_name_col: str = "billtoname"
        city_name_col: str = "city"
        state_name_col: str = "state"
        inv_col: str = "commissionablesales"
        comm_col: str = "commission"

        data = data.dropna(subset=data.columns.to_list()[0])
        data = data[data["STATUS"] == "CLSD"]
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        result.columns = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result["id_string"] = result[result.columns.tolist()[:3]].apply("_".join, axis=1)
        return PreProcessedData(result)

    def _baker_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        default_customer_name: str = "BAKER DISTRIBUTING"
        store_number_col: str = "store#"
        city_name_col: str = "storename"
        state_name_col: str = "storestate"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=store_number_col)
        data = data.dropna(axis=1, how='all')
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()

        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm)
        data.loc[:,"customer"] = default_customer_name
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, comm_col]
        ]
        result.columns = ["store_number", "customer", "city", "state", inv_col_name, comm_col]
        result["id_string"] = result[result.columns.tolist()[:4]].apply("_".join, axis=1)
        return PreProcessedData(result)

    def _johnstone_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        default_customer_name: str = "JOHNSTONE SUPPLY"
        store_number_col: str = "store#"
        city_name_col: str = "storename"
        state_name_col: str = "storestate"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=store_number_col)
        data = data.dropna(axis=1, how='all')
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda val: re.match(r"JS\s?(\w*\s?\S*)\s?-\s?\d{2,3}",val).group(1).strip())
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm)
        data.loc[:,"customer"] = default_customer_name
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, comm_col]
        ]
        result.columns = ["store_number", "customer", "city", "state", inv_col_name, comm_col]
        result["id_string"] = result[result.columns.tolist()[:4]].apply("_".join, axis=1)

        return PreProcessedData(result)


    def _re_michel_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """"""

        default_customer_name: str = "RE MICHEL"
        store_number_col: str = "store#"
        city_name_col: str = "storename"
        state_name_col: str = "storestate"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=store_number_col)
        data = data.dropna(axis=1, how='all')
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda value: value.split("-")[0].strip())
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm)
        data.loc[:,"customer"] = default_customer_name
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, comm_col]
        ]
        result.columns = ["store_number", "customer", "city", "state", inv_col_name, comm_col]
        result["id_string"] = result[result.columns.tolist()[:4]].apply("_".join, axis=1)
        return PreProcessedData(result)

    def _united_refrigeration_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        the only differences between this method and the RE Michel method 
            - the city name splits on comma instead of hyphen
            - column headers use different names, but relative positioning wrt the table boundaries is the same
            - column drop is by city instead of store number column due to where the summary titles have been placed
        """
        default_customer_name: str = "UNITED REFRIGERATION"
        store_number_col: str = "branch"
        city_name_col: str = "branchname"
        state_name_col: str = "state"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=city_name_col)
        data = data.dropna(axis=1, how='all')
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda value: value.split(",")[0].strip())
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm)
        data.loc[:,"customer"] = default_customer_name
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, comm_col]
        ]
        result.columns = ["store_number", "customer", "city", "state", inv_col_name, comm_col]
        result["id_string"] = result[result.columns.tolist()[:4]].apply("_".join, axis=1)
        return PreProcessedData(result)

    def _winsupply_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        default_customer_name: str = "WINSUPPLY"
        store_number_col: str = "store#"
        city_name_col: str = "storename"
        state_name_col: str = "storestate"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated
        total_comm: float = kwargs.get("total_commission_amount", None)

        data = data.dropna(subset=store_number_col)
        data = data.dropna(axis=1, how='all')
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda value: value.split("-")[0].strip())
        data.loc[:,city_name_col] = data[city_name_col].apply(lambda value: value.split(",")[0].strip())
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data = self._calculate_commission_amounts(data,inv_col_name,comm_col,total_comm)
        data.loc[:,"customer"] = default_customer_name
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, comm_col]
        ]
        result.columns = ["store_number", "customer", "city", "state", inv_col_name, comm_col]
        result["id_string"] = result[result.columns.tolist()[:4]].apply("_".join, axis=1)
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": (self._standard_report_preprocessing,0),
            "baker_pos": (self._baker_report_preprocessing,1),
            "johnstone_pos": (self._johnstone_report_preprocessing,1),
            "re_michel_pos": (self._re_michel_report_preprocessing,1),
            "winsupply_pos": (self._winsupply_report_preprocessing,1),
            "united_refrigeration_pos": (self._united_refrigeration_report_preprocessing,0),
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param), **kwargs)
        else:
            return