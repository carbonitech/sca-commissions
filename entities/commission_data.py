from dataclasses import dataclass
from pandas import DataFrame
from numpy import sign
from typing import Literal

SumSign = int
SumAmount = float
SumWithSign = tuple[SumSign, SumAmount]
DollarFigCol = Literal["inv_amt", "comm_amt"]


@dataclass
class PreProcessedData:
    data: DataFrame

    def get_abs_sum(self, col: DollarFigCol) -> SumWithSign:
        amount = self.data[col].sum()
        return sign(amount), abs(amount)

    def __post_init__(self) -> None:
        sales_sign, sales_amount = self.get_abs_sum("inv_amt")
        commission_sign, commissions_amount = self.get_abs_sum("comm_amt")
        sales_negative = sales_sign == -1
        commissions_negative = commission_sign == -1
        if commissions_amount > sales_amount:
            raise Exception(
                "Commissions are greater than sales.\n"
                f"\tSales = {'-' if sales_negative else ''}${sales_amount:,.2f}\n"
                f"\tCommissions = {'-' if commissions_negative else ''}"
                f"${commissions_amount:,.2f}"
            )
