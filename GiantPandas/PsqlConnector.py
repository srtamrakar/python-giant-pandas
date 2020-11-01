import re
import io
import math
import psycopg2
import numpy as np
import pandas as pd
from typing import NoReturn, Dict, Tuple, Union

from . import PandasOps
from .exceptions import InvalidOptionError


class PsqlConnector(object):
    """
    Python module to upload dataframe into PSQL / get PSQL query results as dataframe.
    """

    # general csv features
    _csv_sep = ","
    _csv_null_identifier = {
        "int32": -9223372036854775808,
        "int64": -9223372036854775808,
        "general": "#N/A",
    }

    def __init__(
        self, host: str, dbname: str, username: str, password: str, port: str = "5432"
    ) -> NoReturn:
        self.__host = host
        self.__dbname = dbname
        self.__user = username
        self.__password = password
        self.__port = port

    def _get_database_connectors(self) -> Tuple:
        conn = psycopg2.connect(
            host=self.__host,
            user=self.__user,
            password=self.__password,
            dbname=self.__dbname,
            port=self.__port,
        )
        cur = conn.cursor()
        return conn, cur

    def _close_database_connectors(self, conn, cur) -> NoReturn:
        conn.commit()
        cur.close()
        conn.close()
        return None

    def get_query_results(self, query: str) -> pd.DataFrame:
        conn, cur = self._get_database_connectors()
        df = pd.read_sql_query(query, con=conn)
        self._close_database_connectors(conn, cur)
        return df

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        schema_name: str,
        table_name: str,
        if_exists: str = "replace",
    ) -> NoReturn:
        if_exists = str(if_exists).lower()
        if_exists_allowed_value_list = ["replace", "append"]

        if if_exists not in if_exists_allowed_value_list:
            raise InvalidOptionError(if_exists, if_exists_allowed_value_list)

        df_for_upload = df.copy()

        self._correct_float_columns(df_for_upload)
        PandasOps.set_column_names_to_alpha_numeric(df_for_upload)
        PandasOps.set_column_names_to_snake_case(df_for_upload, "lower")
        self._clean_delimiter_in_object_columns_from_dataframe(df_for_upload)

        if if_exists == "replace":
            self._drop_table(schema_name=schema_name, table_name=table_name)

        self._create_table(
            schema_name=schema_name,
            table_name=table_name,
            column_name_type_dict=self._get_dict_of_column_name_to_psql_type(
                df_for_upload
            ),
        )

        self._insert_df_to_psql(
            df=df_for_upload, schema_name=schema_name, table_name=table_name
        )

        for column_type in ["int64", "int32"]:
            self._update_null_in_columns(
                schema_name=schema_name,
                table_name=table_name,
                df=df_for_upload,
                column_dtype=column_type,
            )

    def _insert_df_to_psql(
        self,
        df: pd.DataFrame,
        schema_name: str,
        table_name: str,
    ) -> NoReturn:
        conn, cur = self._get_database_connectors()

        # save dataframe as temp csv
        csv_io = io.StringIO()
        df.to_csv(
            csv_io,
            sep=self._csv_sep,
            encoding="utf-8-sig",
            header=False,
            index=False,
            na_rep=self._csv_null_identifier["general"],
        )
        csv_contents = csv_io.getvalue()
        csv_contents = re.sub(
            r"NaT", self._csv_null_identifier["general"], csv_contents
        )
        csv_io.seek(0)
        csv_io.write(csv_contents)

        # copy from temp csv to psql table
        csv_io.seek(0)
        cur.copy_from(
            csv_io,
            f"{schema_name}.{table_name}",
            columns=df.columns.tolist(),
            sep=self._csv_sep,
            null=self._csv_null_identifier["general"],
        )
        csv_io.close()

        self._close_database_connectors(conn, cur)

    def _get_psql_array_format_of_python_list(self, python_list: list) -> str:
        psql_array = f"({str(python_list)[1:-1]})"
        return psql_array

    def _execute_query(self, query: str) -> NoReturn:
        conn, cur = self._get_database_connectors()
        cur.execute(query)
        self._close_database_connectors(conn, cur)

    def _exists_table(self, schema_name: str, table_name: str) -> bool:
        check_command = f"""
        SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema = '{schema_name}'
            AND    table_name = '{table_name}'
        );"""
        check_df = self.get_query_results(query=check_command)
        return check_df.at[0, "exists"]

    def _drop_table(self, schema_name: str, table_name: str) -> NoReturn:
        del_command = f"""DROP TABLE IF EXISTS {schema_name}."{table_name}";"""
        self._execute_query(del_command)

    def _create_table_as(
        self, query: str, table_name: str, schema_name: str
    ) -> NoReturn:
        create_command = f"""CREATE TABLE {schema_name}."{table_name}" AS {query}"""
        self._execute_query(query=create_command)

    def _create_table(
        self,
        schema_name: str,
        table_name: str,
        column_name_type_dict: Dict[str, str],
    ) -> NoReturn:
        column_types = ", ".join(
            [f"{col_n} {col_t}" for col_n, col_t in column_name_type_dict.items()]
        )
        create_command = f"""CREATE TABLE IF NOT EXISTS {schema_name}."{table_name}" ({column_types});"""
        self._execute_query(query=create_command)

    def _correct_float_columns(self, df: pd.DataFrame) -> NoReturn:
        float_32_column_list = PandasOps.get_column_names_by_type(
            df=df, column_dtype="float32"
        )
        float_64_column_list = PandasOps.get_column_names_by_type(
            df=df, column_dtype="float64"
        )
        float_column_list = float_32_column_list + float_64_column_list

        if len(float_column_list) == 0:
            return None

        for float_col in float_column_list:
            if PandasOps.contains_all_integer_in_float_column(
                df=df, column_name=float_col
            ):
                df[float_col] = (
                    df[float_col].fillna(self._csv_null_identifier["int64"]).astype(int)
                )

    def _get_dict_of_column_name_to_psql_type(self, df: pd.DataFrame) -> Dict[str, str]:
        pandas_dtype_to_psql_column_type_dict = {
            "int64": "bigint",
            "int32": "bigint",
            "float32": "double precision",
            "float64": "double precision",
            "datetime64[ns]": "timestamp",
            "bool": "boolean",
            "array[object]": "character varying(256)[]",
        }

        pandas_column_name_type_dict = PandasOps.get_dict_of_column_name_to_type(df)
        psql_column_name_type_dict = dict()

        for k, v in pandas_column_name_type_dict.items():
            if v != "object":
                psql_column_name_type_dict[k] = pandas_dtype_to_psql_column_type_dict[v]
            else:
                max_character_length = (
                    PandasOps.get_maximum_length_of_dtype_object_values(
                        df=df, column_name=k
                    )
                )
                max_character_length = math.ceil(1.25 * max_character_length)

                if max_character_length <= 2056:
                    psql_column_name_type_dict[
                        k
                    ] = f"character varying({max_character_length})"
                else:
                    psql_column_name_type_dict[k] = "text"

        return psql_column_name_type_dict

    def _clean_delimiter_in_object_columns_from_dataframe(
        self, df: pd.DataFrame
    ) -> NoReturn:
        object_column_list = PandasOps.get_column_names_by_type(
            df=df, column_dtype="object"
        )
        for obj_col in object_column_list:
            df[obj_col] = (
                df[obj_col]
                .str.replace("\t", " ", regex=True)
                .replace("\r\n", "", regex=True)
                .replace("\n", "", regex=True)
                .replace('"', "'", regex=True)
                .replace(",", "|", regex=True)
            )

    def _update_null_in_columns(
        self,
        df: pd.DataFrame,
        column_dtype: Union[np.dtype, str],
        schema_name: str,
        table_name: str,
    ) -> NoReturn:
        int_columns = PandasOps.get_column_names_by_type(
            df=df, column_dtype=column_dtype
        )
        if len(int_columns) < 1:
            return None

        update_command = ""
        for int_col in int_columns:
            update_command += f"""
                UPDATE {schema_name}.{table_name}
                SET {int_col} = NULL
                WHERE {int_col} = {self._csv_null_identifier[column_dtype]};
                """
        self._execute_query(query=update_command)
