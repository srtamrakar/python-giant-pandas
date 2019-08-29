import os.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd

from GiantPandas.PandasOps import PandasOps

test_excel_folder = 'tests'
test_excel_filename = 'test.xlsx'
test_excel_file = os.path.join(test_excel_folder, test_excel_filename)

test_df = pd.read_excel(test_excel_file)


def test_001_get_row_count():
	assert PandasOps.get_row_count(test_df) == 10


def test_002_get_dictionary_from_two_columns_without_duplicate_keys():
	assert PandasOps.get_dict_from_two_columns(
		test_df, key_column='key_for_dict', value_column='value_for_dict', keep_duplicate_keys=False
	) == {
			   0: 0
		   }


def test_003_get_dictionary_from_two_columns_with_first_values():
	assert PandasOps.get_dict_from_two_columns(
		test_df, key_column='key_for_dict', value_column='value_for_dict', keep_duplicate_keys='first'
	) == {
			   0: 0,
			   1: 1,
			   2: 1,
			   3: 1
		   }


def test_004_get_dictionary_from_two_columns_with_last_values():
	assert PandasOps.get_dict_from_two_columns(
		test_df, key_column='key_for_dict', value_column='value_for_dict', keep_duplicate_keys='LAST'
	) == {
			   0: 0,
			   1: 3,
			   2: 3,
			   3: 3
		   }


def test_005_set_column_as_index():
	df = test_df.copy()
	PandasOps.set_column_as_index(df, column_name='id', drop_original_column=False)
	assert df.index.name == 'id'


def test_006_get_dict_of_column_name_to_type():
	assert PandasOps.get_dict_of_column_name_to_type(test_df) == {
		'id': 'int64',
		'all strings': 'object',
		'all_int': 'int64',
		'float with nans': 'float64',
		' # int with nan # ': 'float64',
		'Unnamed: 6': 'float64',
		'camelCase': 'int64',
		'alphanumeric123': 'int64',
		'updated date': 'datetime64[ns]',
		'key_for_dict': 'int64',
		'value_for_dict': 'int64',
	}


def test_007_get_column_names_by_type():
	assert (
			PandasOps.get_column_names_by_type(test_df, 'object') == ['all strings'] and
			PandasOps.get_column_names_by_type(test_df, 'datetime64[ns]') == ['updated date']
	)


def test_008_contains_all_integer_in_float_column():
	assert (
			PandasOps.contains_all_integer_in_float_column(test_df, 'id') == True and
			PandasOps.contains_all_integer_in_float_column(test_df, 'float with nans') == False and
			PandasOps.contains_all_integer_in_float_column(test_df, ' # int with nan # ') == True

	)


def test_009_set_column_names_to_alpha_numeric():
	df = test_df.copy()
	PandasOps.set_column_names_to_alpha_numeric(df)
	assert list(df.columns) == ['id', 'all_strings', 'all_int', 'float_with_nans', '_int_with_nan_',
								'camelCase', 'Unnamed_6', 'alphanumeric123', 'updated_date',
								'key_for_dict', 'value_for_dict']


def test_010_set_column_names_to_snake_case():
	df = test_df.copy()
	PandasOps.set_column_names_to_snake_case(df)
	assert list(df.columns) == ['id', 'all_strings', 'all_int', 'float_with_nans', 'int_with_nan',
								'camel_case', 'unnamed_6', 'alphanumeric_123', 'updated_date',
								'key_for_dict', 'value_for_dict']


def test_011_exists_unnamed_headers():
	assert PandasOps.exists_unnamed_headers(test_df) == True


def test_012_exists_column():
	assert (
			PandasOps.exists_column(test_df, ['id']) == True and
			PandasOps.exists_column(test_df, ['id', 'float_with_nans']) == False
	)


def test_013_get_maximum_length_of_dtype_object_values():
	assert PandasOps.get_maximum_length_of_dtype_object_values(test_df, 'all strings') == 4
