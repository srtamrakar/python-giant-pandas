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
arg_parser.add_argument("-sn", "--sheetname", required=True, help="Sheetname", type=str)
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
    logger.info("START: Getting psql query results in excel")

    logger.info("Initializing PSQL connector ... ")
    psql_connector = PsqlConnector(
        host=args["host"],
        dbname=args["dbname"],
        username=args["username"],
        password=args["password"],
    )

    logger.info("Reading psql query results to dataframe ...")
    df = psql_connector.get_query_results(
        query='SELECT * FROM {0}."{1}";'.format(args["schema"], args["table"])
    )

    logger.info("Writing dataframe to excel sheet ...")
    ExcelConnector.send_dataframe_to_excel(
        file_path=args["file"],
        df_to_sheet_name_tuple_list=[(df, args["sheetname"])],
        write_index=False,
    )

    logger.info("END: Getting psql query results in excel")
    return


if __name__ == "__main__":
    main()
