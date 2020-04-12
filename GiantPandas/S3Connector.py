import re
import io
import boto3
import logging
import pandas as pd
from typing import NoReturn

logger = logging.getLogger(__name__)


class S3Connector(object):
    """
    Python module to upload dataframe as csv to S3.
    """

    # general csv features
    _csv_sep = ","
    _csv_null_identifier = "#N/A"

    def __init__(
        self, aws_access_key_id: str, aws_secret_access_key: str, aws_region: str
    ) -> NoReturn:
        self.resource = boto3.resource(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )

    def upload_dataframe_as_csv(
        self,
        df: pd.DataFrame,
        bucket_name: str,
        object_name: str,
        csv_sep: str,
        csv_null_identifier: str,
    ) -> NoReturn:

        if csv_sep is None:
            csv_sep = self._csv_sep
        if csv_null_identifier is None:
            csv_null_identifier = self._csv_null_identifier

        # save dataframe as temp csv
        csv_io = io.StringIO()
        df.to_csv(
            csv_io,
            sep=csv_sep,
            encoding="utf-8-sig",
            header=True,
            index=False,
            na_rep=csv_null_identifier,
        )
        csv_contents = csv_io.getvalue()
        csv_contents = re.sub(r"NaT", csv_null_identifier, csv_contents)
        csv_io.seek(0)
        csv_io.write(csv_contents)

        # copy temp csv file to S3 object
        self.resource.Object(bucket_name, object_name).put(Body=csv_io.getvalue())
        csv_io.close()
        logger.info(f"File uploaded: {bucket_name}/{object_name}")
