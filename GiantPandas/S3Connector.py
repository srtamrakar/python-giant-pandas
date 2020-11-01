import re
import io
import boto3
import logging
import pandas as pd
from typing import NoReturn
from botocore.config import Config


logger = logging.getLogger(__name__)


class S3Connector(object):
    """
    Python module to upload dataframe as csv to S3.
    """

    default_config = Config(
        connect_timeout=60,
        read_timeout=60,
        max_pool_connections=50,
        retries={"max_attempts": 3},
    )
    aws_default_region = "eu-central-1"

    # general csv features
    default_csv_sep = ","
    default_csv_null_identifier = "#N/A"

    def __init__(
        self,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_region: str = None,
        config: Config = None,
    ) -> NoReturn:
        self.config = config if config is not None else S3Connector.default_config
        self.aws_region = (
            aws_region if aws_region is not None else S3Connector.aws_default_region
        )
        self.__resource = None
        self._create_client(aws_access_key_id, aws_secret_access_key)

    def _create_client(
        self,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ) -> NoReturn:

        if any(cred is None for cred in [aws_access_key_id, aws_secret_access_key]):
            self.__resource = boto3.resource(
                "s3", region_name=self.aws_region, config=self.config
            )
        else:
            self.__resource = boto3.resource(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=self.aws_region,
                config=self.config,
            )
        logger.info("S3 client created")

    def upload_dataframe_as_csv(
        self,
        df: pd.DataFrame,
        bucket: str,
        object_key: str,
        csv_sep: str = None,
        csv_null_identifier: str = None,
        include_header: bool = True,
        include_index: bool = False,
        encoding: str = "utf-8-sig",
    ) -> NoReturn:
        csv_sep = csv_sep if csv_sep is not None else S3Connector.default_csv_sep
        csv_null_identifier = (
            csv_null_identifier
            if csv_null_identifier is not None
            else S3Connector.default_csv_null_identifier
        )

        # save dataframe as temp csv
        csv_io = io.StringIO()
        df.to_csv(
            csv_io,
            sep=csv_sep,
            encoding=encoding,
            header=include_header,
            index=include_index,
            na_rep=csv_null_identifier,
        )
        csv_contents = csv_io.getvalue()
        csv_contents = re.sub(r"NaT", csv_null_identifier, csv_contents)
        csv_io.seek(0)
        csv_io.write(csv_contents)

        # copy temp csv file to S3 object
        self.__resource.Object(bucket, object_key).put(Body=csv_io.getvalue())
        csv_io.close()
        logger.info(f"File uploaded: {bucket}/{object_key}")
