import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import logging
import argparse
from GiantPandas import ExcelConnector, PsqlConnector

logger = logging.getLogger(__name__)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument(
    "-f",
    "--file",
    required=True,
    help="Name of the excel file to be uploaded",
    type=str,
)
arg_parser.add_argument(
    "-sn", "--sheetname", required=False, help="Sheetname", type=str
)
arg_parser.add_argument(
    "-ie",
    "--if_exists",
    required=False,
    help="Replace/append table if exists",
    choices=["replace", "append"],
    type=str,
)
arg_parser.add_argument(
    "-H", "--host", required=True, help="Host for psql db", type=str
)
arg_parser.add_argument("-d", "--dbname", required=True, help="Database name", type=str)
arg_parser.add_argument("-u", "--username", required=True, help="Username", type=str)
arg_parser.add_argument("-p", "--password", required=False, help="Password", type=str)
arg_parser.add_argument(
    "-s", "--schema", required=False, help="Schema name", type=str, default="public"
)
arg_parser.add_argument(
    "-t", "--table", required=True, help="Table to upload the excel file", type=str
)
args = vars(arg_parser.parse_args())


def main():
    logger.info("START: Uploading excel to psql")

    logger.info(
        "Reading excel file to dataframe: {0}; sheet name: {1} ...".format(
            args["file"], args["sheetname"]
        )
    )
    df = ExcelConnector.get_dataframe_from_excel(
        file_path=args["file"], sheet_name=args["sheetname"]
    )

    logger.info("Initializing PSQL connector ... ")
    psql_connector = PsqlConnector(
        host=args["host"],
        dbname=args["dbname"],
        username=args["username"],
        password=args["password"],
    )

    logger.info(
        'Uploading dataframe to psql table {0}."{1}" ...'.format(
            args["schema"], args["table"]
        )
    )
    psql_connector.upload_dataframe_to_psql(
        df=df,
        schema_name=args["schema"],
        table_name=args["table"],
        if_exists=args["if_exists"],
    )

    logger.info("END: Uploading excel to psql")
    return


if __name__ == "__main__":
    main()
