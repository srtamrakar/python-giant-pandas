import os
import sys
import xlrd
import pandas as pd
from FreqObjectOps import DirOps
from GiantPandas import PandasOps
from typing import List, Tuple, NoReturn, Union

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


class ExcelConnector(object):
    """
    Python module to read / write dataframe from / into xlsx file.
    """

    def __init__(self) -> NoReturn:
        pass

    @classmethod
    def get_sheet_names(cls, file_path: str) -> list:
        excel_file = xlrd.open_workbook(file_path, on_demand=True)
        return excel_file.sheet_names()

    @classmethod
    def get_dataframe_from_excel(
        cls,
        file_path: str,
        sheet_name: Union[int, str] = None,
        skip_rows_list: list = None,
    ) -> pd.DataFrame:
        if sheet_name is None:
            sheet_name = 0

        df = pd.read_excel(
            file_path, sheet_name=sheet_name, encoding="utf-8", skiprows=skip_rows_list
        )
        return df

    @classmethod
    def send_dataframe_to_excel(
        cls,
        file_path: str,
        df_to_sheet_name_tuple_list: List[Tuple[pd.DataFrame, str]],
        write_index: bool = False,
    ) -> NoReturn:
        dir_ = DirOps.get_dir_from_file_path(file_path=file_path)
        if dir_ in [None, ""]:
            dir_ = os.getcwd()

        abs_file_path = os.path.join(dir_, file_path)

        if not DirOps.exists_dir(dir_):
            os.makedirs(dir_)

        writer = pd.ExcelWriter(
            abs_file_path, engine="xlsxwriter", options={"strings_to_urls": False}
        )
        for df, sheet in df_to_sheet_name_tuple_list:
            if PandasOps.get_row_count(df) == 0:
                continue
            df.to_excel(writer, sheet_name=sheet, index=write_index)
        writer.save()
        return
