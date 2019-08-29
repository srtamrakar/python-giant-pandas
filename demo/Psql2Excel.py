import os.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from GiantPandas import ExcelConnector, PsqlConnector

import argparse
from DailyLogger import DailyLogger

arg_parser = argparse.ArgumentParser()

arg_parser.add_argument("-f", "--file",
						required=True,
						help="Name of the excel file to be uploaded",
						type=str)

arg_parser.add_argument("-sn", "--sheetname",
						required=True,
						help="Sheetname",
						type=str)

arg_parser.add_argument("-H", "--host",
						required=True,
						help="Host for psql db",
						type=str)

arg_parser.add_argument("-d", "--database",
						required=True,
						help="Database name",
						type=str)

arg_parser.add_argument("-u", "--username",
						required=True,
						help="Username",
						type=str)

arg_parser.add_argument("-p", "--password",
						required=False,
						help="Password",
						type=str)

arg_parser.add_argument("-s", "--schema",
						required=False,
						help="Schema name",
						type=str,
						default='public')

arg_parser.add_argument("-t", "--table",
						required=True,
						help="Table to upload the excel file",
						type=str)

args = vars(arg_parser.parse_args())

py_logger = DailyLogger(
	log_folder='demo/logs',
	project_name='demo',
	log_level='info',
	should_also_log_to_stdout=True
)
logger = py_logger.get_logger()


def main():
	logger.info(py_logger.as_header_style('START: Getting psql query results in excel'))

	logger.info('Initializing PSQL connector ... ')
	psql_connector = PsqlConnector(host=args['host'],
								   dbname=args['database'],
								   username=args['username'],
								   password=args['password'])

	logger.info('Reading psql query results to dataframe ...')
	df = psql_connector.get_psql_query_results_as_dataframe(
		query='SELECT * FROM {0}."{1}";'.format(args['schema'], args['table'])
	)

	logger.info('Writing dataframe to excel sheet ...')
	ExcelConnector.send_dataframe_to_excel(
		file=args['file'],
		dataframe_to_sheet_name_tuple_list=[(df, args['sheetname'])],
		write_index=False
	)

	logger.info(py_logger.as_header_style('END: Getting psql query results in excel'))
	return


if __name__ == '__main__':
	main()
