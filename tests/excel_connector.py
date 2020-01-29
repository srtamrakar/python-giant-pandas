import os.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from GiantPandas.ExcelConnector import ExcelConnector

test_excel_folder = "tests"
test_excel_filename = "test.xlsx"
test_excel_file = os.path.join(test_excel_folder, test_excel_filename)


def test_001_get_sheet_names():
    assert ExcelConnector.get_sheet_names(test_excel_file) == ["sheet_1", "sheet_2"]
