# GiantPandas
Some special functions and connectors for Pandas.

## Requirements

* Python 3+ (Tested in 3.7)
* pandas>=0.25.0
* pytest>=5.0.1
* Unidecode>=1.0.22


## Install with pip
```bash
$ pip install GiantPandas
```

## Usage
1. Import the library.
    ```python
    from GiantPandas import PandasOps, ExcelConnector, PsqlConnector
    ```
1. Each of the imported submodules has several functions. Please refer to respective help for more information.

#### ```PandasOps```

1. ```get_row_count(dataframe)```
1. ```get_dictionary_from_two_columns(dataframe, key_column, value_column, keep_duplicate_keys)```
1. ```get_dataframe_with_all_permutations_from_dict(dict_with_list_values)```
1. ```set_column_as_index(dataframe, column_name, drop_original_column)```
1. ```get_dict_of_column_name_to_type(dataframe)```
1. ```get_column_names_by_type(dataframe, column_dtype)```
1. ```contains_all_integer_in_float_column(dataframe, column_name)```
1. ```set_column_names_to_alpha_numeric(dataframe)```
1. ```set_column_names_to_snake_case(dataframe)```
1. ```exists_unnamed_headers(dataframe)```
1. ```exists_column(dataframe, column_name_list)```
1. ```get_maximum_length_of_dtype_object_values(dataframe, column_name)```



* **&copy; Samyak Ratna Tamrakar** - [Github](https://github.com/srtamrakar), [LinkedIn](https://www.linkedin.com/in/srtamrakar/).