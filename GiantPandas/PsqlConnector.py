import os
import sys
import re
import io
import math
import psycopg2
import numpy as np
import pandas as pd
from typing import NoReturn, Dict
from GiantPandas import PandasOps

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


class PsqlConnector(object):
    """
    Python module to upload dataframe into PSQL / get PSQL query results as dataframe.
    """

    # general csv features
    _csv_sep = "\t"
    _csv_null_identifier = {
        "int32": -9223372036854775808,
        "int64": -9223372036854775808,
        "general": "#N/A",
    }

    def __init__(
        self,
        host: str = None,
        dbname: str = None,
        username: str = None,
        password: str = None,
        port: str = None,
    ) -> NoReturn:
        """
        :param host: str, mandatory
            host of psql db
        :param dbname: str, mandatory
            dbname of psql db
        :param username: str, mandatory
            username for psql db
        :param password: str
            password for psql db
        :param port: str
            port for psql db
        """

        if None in [host, dbname, username]:
            return None
        if port is None:
            port = "5432"

        self.__host = host
        self.__dbname = dbname
        self.__user = username
        self.__password = password
        self.__port = port

    def _get_database_connectors(self):
        conn = psycopg2.connect(
            host=self.__host,
            user=self.__user,
            password=self.__password,
            dbname=self.__dbname,
            port=self.__port,
        )
        cur = conn.cursor()
        return conn, cur

    def _close_database_connectors(self, conn=None, cur=None) -> NoReturn:
        conn.commit()
        cur.close()
        conn.close()
        return None

    def get_psql_query_results_as_dataframe(self, query: str = None) -> pd.DataFrame:
        """
        :param query: str
            a psql query
        :return:
            pandas dataframe with results from psql query
        """
        conn, cur = self._get_database_connectors()
        df = pd.read_sql_query(query, con=conn)
        self._close_database_connectors(conn, cur)
        return df

    def send_dataframe_to_psql(
        self,
        dataframe: pd.DataFrame = None,
        schema_name: str = None,
        table_name: str = None,
        if_exists: str = None,
    ) -> NoReturn:
        """
        :param dataframe: pandas.DataFrame
        :param schema_name: str
        :param table_name: str
        :param if_exists: 'replace' | 'append'
        :return:
            uploads a dataframe to a psql table
        """
        if dataframe is None:
            return None
        if table_name is None:
            return None
        if schema_name is None:
            schema_name = "public"
        if str(if_exists).lower() not in ["replace", "append"]:
            if_exists = "replace"

        dataframe_for_upload = dataframe.copy()

        self._correct_float_columns(dataframe_for_upload)
        PandasOps.set_column_names_to_alpha_numeric(dataframe_for_upload)
        PandasOps.set_column_names_to_snake_case(dataframe_for_upload)
        self._clean_delimiter_in_object_columns_from_dataframe(dataframe_for_upload)

        if if_exists == "replace":
            self._drop_table(schema_name=schema_name, table_name=table_name)

        self._create_table(
            schema_name=schema_name,
            table_name=table_name,
            column_name_type_dict=self._get_dict_of_column_name_to_type_from_dataframe_for_psql(
                dataframe_for_upload
            ),
        )

        self._upload_dataframe_to_psql(
            dataframe=dataframe_for_upload,
            schema_name=schema_name,
            table_name=table_name,
        )

        self._update_null_in_columns(
            schema_name=schema_name,
            table_name=table_name,
            dataframe=dataframe_for_upload,
            column_dtype="int64",
        )
        self._update_null_in_columns(
            schema_name=schema_name,
            table_name=table_name,
            dataframe=dataframe_for_upload,
            column_dtype="int32",
        )

    def _upload_dataframe_to_psql(
        self,
        dataframe: pd.DataFrame = None,
        schema_name: str = None,
        table_name: str = None,
    ) -> NoReturn:
        conn, cur = self._get_database_connectors()

        # save dataframe as temp csv
        csv_io = io.StringIO()
        dataframe.to_csv(
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
            "{0}.{1}".format(schema_name, table_name),
            columns=dataframe.columns.tolist(),
            sep=self._csv_sep,
            null=self._csv_null_identifier["general"],
        )
        csv_io.close()

        self._close_database_connectors(conn, cur)

    def _get_psql_array_format_of_python_list(self, python_list: list = None) -> str:
        psql_array = "({0})".format(str(python_list)[1:-1])
        return psql_array

    def _execute_query(self, query: str = None) -> NoReturn:
        conn, cur = self._get_database_connectors()
        cur.execute(query)
        self._close_database_connectors(conn, cur)

    def _exists_table(self, schema_name: str = None, table_name: str = None) -> bool:
        check_command = """
        SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema = '{0}'
            AND    table_name = '{1}'
        );""".format(
            schema_name, table_name
        )
        check_df = self.get_psql_query_results_as_dataframe(query=check_command)
        return check_df.at[0, "exists"]

    def _drop_table(self, schema_name: str = None, table_name: str = None) -> NoReturn:
        del_command = 'DROP TABLE IF EXISTS {0}."{1}";'.format(schema_name, table_name)
        self._execute_query(del_command)

    def _create_table_as(
        self, query: str = None, schema_name: str = None, table_name: str = None
    ) -> NoReturn:
        create_command = 'CREATE TABLE {0}."{1}" AS {2}'.format(
            schema_name, table_name, query
        )
        self._execute_query(query=create_command)
        return None

    def _create_table(
        self,
        schema_name: str = None,
        table_name: str = None,
        column_name_type_dict: Dict[str, str] = None,
    ) -> NoReturn:
        column_types = ", ".join(
            [
                "{0} {1}".format(col_n, col_t)
                for col_n, col_t in column_name_type_dict.items()
            ]
        )
        create_command = 'CREATE TABLE IF NOT EXISTS {0}."{1}" ({2});'.format(
            schema_name, table_name, column_types
        )
        self._execute_query(query=create_command)

    def _correct_float_columns(self, dataframe: pd.DataFrame = None) -> NoReturn:
        float_32_column_list = PandasOps.get_column_names_by_type(
            dataframe=dataframe, column_dtype="float32"
        )
        float_64_column_list = PandasOps.get_column_names_by_type(
            dataframe=dataframe, column_dtype="float64"
        )
        float_column_list = float_32_column_list + float_64_column_list

        if len(float_column_list) == 0:
            return None

        for float_col in float_column_list:
            if PandasOps.contains_all_integer_in_float_column(
                dataframe=dataframe, column_name=float_col
            ):
                dataframe[float_col] = (
                    dataframe[float_col]
                    .fillna(self._csv_null_identifier["int64"])
                    .astype(int)
                )

    def _get_dict_of_column_name_to_type_from_dataframe_for_psql(
        self, dataframe: pd.DataFrame = None
    ) -> Dict[str, str]:
        pandas_dtype_to_psql_column_type_dict = {
            "int64": "bigint",
            "int32": "bigint",
            "float32": "double precision",
            "float64": "double precision",
            "datetime64[ns]": "timestamp",
            "bool": "boolean",
            "array[object]": "character varying(256)[]",
        }

        pandas_column_name_type_dict = PandasOps.get_dict_of_column_name_to_type(
            dataframe
        )
        psql_column_name_type_dict = dict()

        for k, v in pandas_column_name_type_dict.items():
            if v != "object":
                psql_column_name_type_dict[k] = pandas_dtype_to_psql_column_type_dict[v]
            else:
                max_number_of_characters = PandasOps.get_maximum_length_of_dtype_object_values(
                    dataframe=dataframe, column_name=k
                )
                max_number_of_characters = math.ceil(1.25 * max_number_of_characters)

                if max_number_of_characters <= 2056:
                    psql_column_name_type_dict[k] = "character varying({})".format(
                        max_number_of_characters
                    )
                else:
                    psql_column_name_type_dict[k] = "text"

        return psql_column_name_type_dict

    def _clean_delimiter_in_object_columns_from_dataframe(
        self, dataframe: pd.DataFrame = None
    ) -> NoReturn:
        object_column_list = PandasOps.get_column_names_by_type(
            dataframe=dataframe, column_dtype="object"
        )
        for obj_col in object_column_list:
            dataframe[obj_col] = (
                dataframe[obj_col]
                .str.replace("\t", " ", regex=True)
                .replace("\r\n", "", regex=True)
                .replace("\n", "", regex=True)
                .replace('"', "'", regex=True)
                .replace(",", "\|", regex=True)
            )

    def _update_null_in_columns(
        self,
        dataframe: pd.DataFrame = None,
        column_dtype: np.dtype = None,
        schema_name: str = None,
        table_name: str = None,
    ) -> NoReturn:
        int_columns = PandasOps.get_column_names_by_type(
            dataframe=dataframe, column_dtype=column_dtype
        )
        if len(int_columns) < 1:
            return None

        update_command = ""
        for int_col in int_columns:
            update_command += """
                UPDATE {0}.{1}
                SET {2} = NULL
                WHERE {2} = {3};
                """.format(
                schema_name,
                table_name,
                int_col,
                self._csv_null_identifier[column_dtype],
            )
        self._execute_query(query=update_command)
