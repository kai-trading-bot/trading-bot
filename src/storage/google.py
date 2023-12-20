import boto3
import pandas as pd
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq

from botocore.exceptions import ClientError
from deprecated import deprecated
from pathlib import Path
from typing import *

from src.config import GOOGLE_ACCESS_KEY_ID, GOOGLE_ACCESS_KEY_SECRET, BUCKET
from src.storage.base import Storage
from src.utils.logger import logger


class GCS(Storage):
    """ Interact with Google Cloud Storage. """

    def __init__(self):
        self.client = boto3.client(
            "s3",
            region_name="auto",
            endpoint_url="https://storage.googleapis.com",
            aws_access_key_id=GOOGLE_ACCESS_KEY_ID,
            aws_secret_access_key=GOOGLE_ACCESS_KEY_SECRET,
        )
        self.bucket = BUCKET

    def upload(self, filename: str, key: Union[str, Path]) -> None:
        logger.debug(f"Uploading file to {key}")
        self.client.upload_file(Bucket=self.bucket, Key=str(key), Filename=filename)

    def download(self, filename: str, key: Union[str, Path]) -> Any:
        logger.debug(f"Downloading {key}")
        return self.client.download_file(Bucket=self.bucket, Key=str(key), Filename=filename)

    def delete(self, key: Union[str, Path]) -> Any:
        logger.warning(f"Deleting {key}")
        return self.client.delete_object(Bucket=self.bucket, Key=key)

    def peek(self, prefix: str = "") -> List[str]:
        """ Returns some or all (up to 1,000) of the objects in the bucket.

        Args:
            prefix (str): prefix of the key.

        Example:
            >>> self.peek()
            >>> ['kraken/CryptoTrend-ADA/execution.csv',
                 'kraken/CryptoTrend-ETH/execution.csv',
                 'kraken/CryptoTrend-ETH/pos.csv']
            >>> self.peek('kraken/CryptoTrend-ADA')
            >>> ['kraken/CryptoTrend-ADA/execution.csv']
        """
        results = []
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=str(prefix))
        if response.get('IsTruncated'):
            logger.warning('The result is truncated.')
        contents = response.get('Contents', [])
        results += [content['Key'] for content in contents if content['Size'] > 0]
        return results

    def write_csv(self, df: pd.DataFrame, filename: str, **kwargs: Any) -> None:
        """ Write a CSV file to GCS.

        Args:
            df (pd.DataFrame): pandas data frame.
            filename (str): file key in GCS.
            kwargs: Any additional keyword arguments for pd.write_csv().
        """
        with tempfile.NamedTemporaryFile(suffix='csv') as temp:
            df.to_csv(temp.name, **kwargs)
            self.upload(temp.name, filename)

    def read_csv(self, filename: str, **kwargs: Any) -> Optional[pd.DataFrame]:
        """ Read a CSV file from GCS.

        Args:
            filename (str): File key, e.g. a/b/file.csv
            kwargs (Any): Any additional keyword arguments for pd.read_csv(), e.g. index_col=0

        Returns:
            A pandas dataframe or None if file not found.
        """
        with tempfile.NamedTemporaryFile(suffix='csv') as temp:
            try:
                self.download(temp.name, filename)
                return pd.read_csv(temp.name, **kwargs)
            except ClientError as e:
                logger.warning(f'Failed to read CSV: {e}')
                return None

    @deprecated
    def write_parquet(self, df: pd.DataFrame, filename: str, use_pyarrow: bool = False, **kwargs: Any) -> None:
        with tempfile.NamedTemporaryFile(suffix='parquet.gz') as temp:
            if use_pyarrow:
                self._pyarrow_to_parquet(df, temp.name)
            else:
                df.to_parquet(temp.name, allow_truncated_timestamps=True, **kwargs)
            self.upload(temp.name, filename)

    @deprecated
    def read_parquet(self, filename: str, **kwargs: Any) -> pd.DataFrame:
        with tempfile.NamedTemporaryFile() as temp:
            self.download(temp.name, filename)
            return pd.read_parquet(temp.name, **kwargs)

    @deprecated
    def _pyarrow_to_parquet(self, df: pd.DataFrame, filename: str):
        # Pandas df.to_parquet cannot handle multi-index columns.
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
