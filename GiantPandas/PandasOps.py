import itertools
import numpy as np
import pandas as pd
from FreqObjectOps import StrOps
from typing import List, Dict, NoReturn, Union

from .exceptions import InvalidOptionError


class PandasOps(object):
    def __init__(self):
        pass

    @classmethod
    def get_row_count(cls, df: pd.DataFrame) -> int:
        return len(df.index)

    @classmethod
    def get_dict_from_two_columns(
        cls,
        df: pd.DataFrame,
        key_column: str,
        value_column: str,
        keep_duplicate_keys: Union[str, bool] = False,
    ) -> dict:
        keep_duplicate_keys_allowed_value_list = ["first", "last", False]

        if keep_duplicate_keys not in keep_duplicate_keys_allowed_value_list:
            raise InvalidOptionError(
                keep_duplicate_keys, keep_duplicate_keys_allowed_value_list
            )

        df_ = df[[key_column, value_column]].copy()
        df_.drop_duplicates(subset=[key_column], keep=keep_duplicate_keys, inplace=True)
        return pd.Series(df_[value_column].values, index=df_[key_column]).to_dict()

    @classmethod
    def get_dataframe_with_all_permutations_from_dict(
        cls, dict_with_list_values: Dict[str, list]
    ):
        df = pd.DataFrame(
            list(itertools.product(*dict_with_list_values.values())),
            columns=dict_with_list_values.keys(),
        )
        return df

    @classmethod
    def set_column_as_index(
        cls, df: pd.DataFrame, column_name: str, drop_original_column: bool = False
    ) -> NoReturn:
        df.reset_index(drop=True, inplace=True)
        df.set_index(column_name, inplace=True, drop=drop_original_column)

    @classmethod
    def get_dict_of_column_name_to_type(cls, df: pd.DataFrame) -> Dict[str, np.dtype]:
        column_name_type_dict = df.dtypes.apply(lambda x: x.name).to_dict()
        return column_name_type_dict

    @classmethod
    def get_column_names_by_type(
        cls, df: pd.DataFrame, column_dtype: Union[np.dtype, str]
    ) -> list:
        column_name_type_dict = cls.get_dict_of_column_name_to_type(df=df)
        matched_column_list = [
            k
            for k, v in column_name_type_dict.items()
            if str(v).lower() == str(column_dtype).lower()
        ]
        return matched_column_list

    @classmethod
    def contains_all_integer_in_float_column(
        cls, df: pd.DataFrame, column_name: str
    ) -> bool:
        column_values = df[column_name].values.copy()
        column_values = column_values[~np.isnan(column_values)]
        return np.array_equal(column_values, column_values.astype(int))

    @classmethod
    def set_column_names_to_alpha_numeric(cls, df: pd.DataFrame) -> NoReturn:
        columns_as_alpha_numeric = list(map(StrOps.text_to_alpha_numeric, df.columns))
        df.columns = columns_as_alpha_numeric

    @classmethod
    def set_column_names_to_snake_case(cls, df: pd.DataFrame, case: str) -> NoReturn:
        columns_as_snake_case = list(
            map(
                StrOps.text_to_snake_case,
                df.columns,
                itertools.repeat(case, len(df.columns)),
            )
        )
        df.columns = columns_as_snake_case

    @classmethod
    def exists_unnamed_headers(cls, df: pd.DataFrame) -> bool:
        return any("unnamed" in col.lower() for col in df.columns)

    @classmethod
    def exists_column(cls, df: pd.DataFrame, column_name_list: List[str]) -> bool:
        return set(column_name_list).issubset(set(df.columns))

    @classmethod
    def get_maximum_length_of_dtype_object_values(
        cls, df: pd.DataFrame, column_name: str
    ) -> int:
        def __get_length_of_dtype_object(object_value=None):
            try:
                return len(object_value.encode("utf-8"))
            except:
                return 1

        return df[column_name].map(__get_length_of_dtype_object).max()
