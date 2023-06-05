"""Type definitions for the aws_parquet package."""
from typing import Any, Dict, Generator, TypeVar, Union

import pandas as pd

PartitionLike = Dict[str, Any]
DataFrameOrIteratorT = TypeVar(
    "DataFrameOrIteratorT",
    bound=Union[pd.DataFrame, Generator[pd.DataFrame, None, None]],
)
