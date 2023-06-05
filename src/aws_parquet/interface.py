"""Interface for ParquetDataset class."""
from abc import ABC
from typing import Any, Iterator, List, Literal, Optional, overload, Union

import pandas as pd

from aws_parquet._types import PartitionLike


class ParquetDatasetInterface(ABC):
    """Interface for ParquetDataset class."""

    def create(
        self,
        if_exists: Literal["ignore", "warn", "raise"] = "ignore",
        sync: bool = True,
    ) -> None:
        """Create the dataset."""
        ...

    @overload
    def read(
        self,
        partition: Optional[PartitionLike] = None,
        chunked: Literal[False] = False,
        columns: Optional[List[str]] = None,
        use_threads: bool = True,
    ) -> pd.DataFrame:
        ...

    @overload
    def read(
        self,
        partition: Optional[PartitionLike] = None,
        chunked: Literal[True] = True,
        columns: Optional[List[str]] = None,
        use_threads: bool = True,
    ) -> Iterator[pd.DataFrame]:
        ...

    def read(
        self,
        partition: Optional[PartitionLike] = None,
        chunked: bool = False,
        columns: Optional[List[str]] = None,
        use_threads: bool = True,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Read the dataset optionally from a given partition."""
        ...

    @overload
    def query(
        self,
        sql: str,
        *,
        workgroup: Optional[str] = None,
        chunksize: None = None,
        ctas_approach: bool = True,
        use_threads: bool = True,
        **kwargs: Any,
    ) -> pd.DataFrame:
        ...

    @overload
    def query(
        self,
        sql: str,
        *,
        workgroup: Optional[str] = None,
        chunksize: Union[int, bool],
        ctas_approach: bool = True,
        use_threads: bool = True,
        **kwargs: Any,
    ) -> Iterator[pd.DataFrame]:
        ...

    def query(
        self,
        sql: str,
        *,
        workgroup: Optional[str] = None,
        chunksize: Optional[Union[int, bool]] = None,
        ctas_approach: bool = True,
        use_threads: bool = True,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Execute a SQL query against the dataset."""
        ...

    def update(self, data: pd.DataFrame, overwrite: bool = False) -> None:
        """Update the dataset."""
        ...

    def delete(self, partition: Optional[PartitionLike] = None) -> None:
        """Delete the dataset."""
        ...
