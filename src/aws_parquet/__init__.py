import pandas as pd
import pyarrow

from .dataset import ParquetDataset

# Note this is a hack to address reading parquet data that has a Period dtype.
# See https://github.com/awslabs/aws-data-wrangler/issues/675 for more details
pyarrow.table(pd.DataFrame({"a": pd.period_range("2012", freq="Y", periods=3)}))


__all__ = ["ParquetDataset"]
