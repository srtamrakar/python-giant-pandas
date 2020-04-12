# GiantPandas
Convenient functions and connectors for Pandas.


## Install with pip
```bash
$ pip install GiantPandas
```

## Usage
1. Import the library.
    ```python
    from GiantPandas import PandasOps, ExcelConnector, PsqlConnector, S3Connector
    ```
1. Each of the imported submodules has several functions. Please refer to respective help for more information.

#### ```PandasOps```

1. ```PandasOps.get_row_count(dataframe)```: get row count of a dataframe
1. ```PandasOps.get_dict_from_two_columns(dataframe, key_column, value_column, keep_duplicate_keys)```: get dictionary from two dataframe columns
1. ```PandasOps.get_dataframe_with_all_permutations_from_dict(dict_with_list_values)```: create dataframe with all possible permutations from dict with values of type list
1. ```PandasOps.set_column_as_index(dataframe, column_name, drop_original_column)```: set column as an index
1. ```PandasOps.get_dict_of_column_name_to_type(dataframe)```: get dict of column name to their dtype
1. ```PandasOps.get_column_names_by_type(dataframe, column_dtype)```: get all columns of desired dtype
1. ```PandasOps.contains_all_integer_in_float_column(dataframe, column_name)```: check if all non-nan values in float columns are int
1. ```PandasOps.set_column_names_to_alpha_numeric(dataframe)```: convert column name to alpha numeric
1. ```PandasOps.set_column_names_to_snake_case(dataframe)```: convert column name to snake case
1. ```PandasOps.exists_unnamed_headers(dataframe)```: check if a dataframe contains any unnamed headers
1. ```PandasOps.exists_column(dataframe, column_name_list)```: check if a dataframe contains desired column
1. ```PandasOps.get_maximum_length_of_dtype_object_values(dataframe, column_name)```: get maximum length of object in a column

#### ```ExcelConnector```
1. 	```ExcelConnector.get_sheet_names(file)```: get all sheet names
1. 	```ExcelConnector.get_dataframe_from_excel(file, sheet_name, skip_rows_list)```: read excel sheet into a dataframe
1. 	```ExcelConnector.send_dataframe_to_excel(file, dataframe_to_sheet_name_tuple_list, write_index)```: write dataframe to an excel sheet

#### ```PsqlConnector```
First, an instance must be created for establishing connection.
```python
psql_connector = PsqlConnector(
    host="localhost",
    dbname="postgres",
    username="postgres",
    password="",
    port="5432",
)
```
Then,
1. ```psql_connector.get_psql_query_results_as_dataframe(query)```: get results of a psql query as a dataframe
1. ```psql_connector.upload_dataframe_to_psql(dataframe, schema_name, table_name, if_exists)```: upload dataframe to psql


#### ```S3Connector```
First, an instance must be created for establishing connection.
```python
s3_connector = S3Connector(
    aws_access_key_id="############",
    aws_secret_access_key="############",
    aws_region="############",
)
```
Then,
1. ```s3_connector.upload_dataframe_as_csv(dataframe, bucket_name, object_name, csv_sep, csv_null_identifier)```: upload pandas dataframe as a csv file into S3 bucket


#### Demo

Demo script for saving results of PostgreSQL query into an excel file.
```bash
$ python3 demo/Psql2Excel.py -f table_from_psql.xlsx -H localhost -d postgres -u postgres -t test_table -sn Sheet1
```

Demo script for uploading a table from excel sheet into a PSQL database.
```bash
$ python3 demo/Excel2Psql.py -f tests/table_to_psql.xlsx -H localhost -d postgres -u postgres -t test_table -ie replace
```

**&copy; [Samyak Ratna Tamrakar](https://www.linkedin.com/in/srtamrakar/)**, 2020.