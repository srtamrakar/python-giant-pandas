import os.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pandas as pd
import itertools

from FreqObjectOps import StrOps


class FreqPandasOps(object):

	def __init__(self):
		pass

	@classmethod
	def get_row_count(cls, dataframe=None):
		"""
		Fastest way to count dataframe rows.
		:param dataframe: pandas.DataFrame
		:return:
			row count as int
		"""
		return len(dataframe.index)

	@classmethod
	def get_dictionary_from_two_columns(cls, dataframe=None,
										key_column=None, value_column=None,
										keep_duplicate_keys=None):
		"""
		:param dataframe: pandas.DataFrame
		:param key_column: str
		:param value_column: str
		:param keep_duplicate_keys: 'first' | 'last' | False
			Whether to drop duplicate keys or to retain only first/last ones
		:return:
			dictionary of {items from key_column : items from value_column}
		"""
		keep_duplicate_keys = str(keep_duplicate_keys).lower()
		if keep_duplicate_keys.lower() not in ['first', 'last']:
			keep_duplicate_keys = False

		dataframe_ = dataframe[[key_column, value_column]].copy()
		dataframe_.drop_duplicates(subset=[key_column], keep=keep_duplicate_keys, inplace=True)
		return pd.Series(dataframe_[value_column].values, index=dataframe_[key_column]).to_dict()

	@classmethod
	def get_dataframe_with_all_permutations_from_dict(cls, dict_with_list_values=None):
		"""
		:param dict_with_list_values: dict
			dictionary with keys of type 'str' and values of type 'list'
		:return:
			pandas dataframe with all possible permutations of the lists
		"""
		df = pd.DataFrame(list(
			itertools.product(*dict_with_list_values.values())
		), columns=dict_with_list_values.keys())
		return df

	@classmethod
	def set_column_as_index(cls, dataframe=None, column_name=None, drop_original_column=None):
		"""
		:param dataframe: pandas.DataFrame
		:param column_name: str
		:param drop_original_column: bool
			whether or not to drop the column after setting it as index
		:return:
			pandas dataframe with column set as index
		"""
		if drop_original_column is None: drop_original_column = False
		dataframe.reset_index(drop=True, inplace=True)
		dataframe.set_index(column_name, inplace=True, drop=drop_original_column)

	@classmethod
	def get_dict_of_column_name_to_type(cls, dataframe=None):
		"""
		:param dataframe: pandas.DataFrame
		:return:
			dictionary of {column name : column dtype}
		"""
		column_name_type_dict = dataframe.dtypes.apply(lambda x: x.name).to_dict()
		return column_name_type_dict

	@classmethod
	def get_column_names_by_type(cls, dataframe=None, column_dtype=None):
		"""
		:param dataframe: pandas.DataFrame
		:param column_dtype: dtype | str
		:return:
			list of column names, which are of dtype column_dtype
		"""
		column_name_type_dict = cls.get_dict_of_column_name_to_type(dataframe=dataframe)
		matched_columns = [k for k, v in column_name_type_dict.items()
						   if str(v).lower() == str(column_dtype).lower()]
		return matched_columns

	@classmethod
	def contains_all_integer_in_float_column(cls, dataframe=None, column_name=None):
		"""
		:param dataframe: pandas.DataFrame
		:param column_name: str
		:return:
			whether all non-nan items in float column are integer
		"""
		column_values = dataframe[column_name].values.copy()
		column_values = column_values[~np.isnan(column_values)]
		return np.array_equal(column_values, column_values.astype(int))

	@classmethod
	def set_column_names_to_alpha_numeric(cls, dataframe=None):
		"""
		:param dataframe: pandas.DataFrame
		:return:
			dataframe with its columns names changed to alpha-numeric
		"""
		columns_as_alpha_numeric = list(map(StrOps.text_to_alpha_numeric, dataframe.columns))
		dataframe.columns = columns_as_alpha_numeric

	@classmethod
	def set_column_names_to_snake_case(cls, dataframe=None):
		"""
		:param dataframe: pandas.DataFrame
		:return:
			dataframe with its columns names changed to snake_case
		"""
		columns_as_snake_case = list(map(StrOps.text_to_snake_case, dataframe.columns))
		dataframe.columns = columns_as_snake_case

	@classmethod
	def exists_unnamed_headers(cls, dataframe=None):
		"""
		:param dataframe: pandas.DataFrame
		:return:
			whether dataframe contains unnamed columns
		"""
		return any('unnamed' in col.lower() for col in dataframe.columns)

	@classmethod
	def exists_column(cls, dataframe=None, column_name_list=None):
		"""
		:param dataframe: pandas.DataFrame
		:param column_name_list: list of str
		:return:
			whether dataframe contains all the columns in column_name_list
		"""
		return set(column_name_list).issubset(set(dataframe.columns))

	@classmethod
	def get_maximum_length_of_dtype_object_values(cls, dataframe=None, column_name=None):
		"""
		:param dataframe: pandas.DataFrame
		:param column_name: str
		:return:
			max length of objects in column
		"""

		def __get_length_of_dtype_object(object_value=None):
			try:
				return len(object_value)
			except:
				return 1

		return dataframe[column_name].map(lambda x: __get_length_of_dtype_object(x)).max()
