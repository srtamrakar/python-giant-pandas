import os
import sys
import xlrd
import pandas as pd
from FreqObjectOps import DirOps
from GiantPandas import PandasOps
from typing import List, Tuple, NoReturn

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


class ExcelConnector(object):
    """
    Python module to read / write dataframe from / into xlsx file.
    """

    def __init__(self) -> NoReturn:
        pass

    @classmethod
    def get_sheet_names(cls, file: str = None) -> list:
        """
        :param file: str
        :return:
            list of sheetnames in excel file
        """
        excel_file = xlrd.open_workbook(file, on_demand=True)
        return excel_file.sheet_names()

    @classmethod
    def get_dataframe_from_excel(
        cls, file: str = None, sheet_name: str = None, skip_rows_list: list = None
    ) -> pd.DataFrame:
        """
        :param file: str
        :param sheet_name: str
        :param skip_rows_list: list
        :return:
            a pandas dataframe
        """
        if sheet_name is None:
            sheet_name = 0  # if not specified, get first sheet
        if skip_rows_list is None:
            skip_rows_list = []

        df = pd.read_excel(
            file, sheet_name=sheet_name, encoding="utf-8", skiprows=skip_rows_list
        )
        return df

    @classmethod
    def send_dataframe_to_excel(
        self,
        file: str = None,
        dataframe_to_sheet_name_tuple_list: List[Tuple[pd.DataFrame, str]] = None,
        write_index: bool = None,
    ) -> NoReturn:
        """
        :param file: str
        :param dataframe_to_sheet_name_tuple_list: list of tuples
            list of tuples of string e.g. [(df, 'sheet_1')], defining sheetnames to save the dataframes into
        :param write_index: bool
            whether dataframe's index should be written to excel file
        :return:
        """
        if write_index is None:
            write_index = False
        if file is None:
            return

        folder = DirOps.get_directory_from_file_path(file_path=file)
        if folder in [None, ""]:
            folder = os.getcwd()
            file = os.path.join(folder, file)

        if not DirOps.exists_folder(folder):
            os.makedirs(folder)

        writer = pd.ExcelWriter(
            file, engine="xlsxwriter", options={"strings_to_urls": False}
        )
        for df, sheet in dataframe_to_sheet_name_tuple_list:
            if PandasOps.get_row_count(df) == 0:
                continue
            df.to_excel(writer, sheet_name=sheet, index=write_index)
        writer.save()
        return
