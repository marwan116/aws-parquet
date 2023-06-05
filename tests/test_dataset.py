"""Test the dataset module."""
import os
from typing import Iterator
from uuid import uuid4

import awswrangler as wr
import pandas as pd
import pandera as pa
import pytest

from aws_parquet.dataset import ParquetDataset


@pytest.fixture
def schema() -> pa.DataFrameSchema:
    """Create a schema."""

    class MyDatasetSchemaModel(pa.SchemaModel):
        """Schema for the dataset."""

        col1: pa.typing.Series[int] = pa.Field(ge=0)
        col2: pa.typing.Series[pa.DateTime]
        col3: pa.typing.Series[float] = pa.Field(ge=0)
        col4: pa.typing.Series[pa.Category]
        col5: pa.typing.Series[pd.CategoricalDtype] = pa.Field(
            dtype_kwargs={"categories": ["a", "b", "c"]}
        )

    return MyDatasetSchemaModel.to_schema()


@pytest.fixture
def dataset(schema: pa.DataFrameSchema) -> Iterator[ParquetDataset]:
    """Create a dataset."""
    table_name = str(uuid4()).replace("-", "")
    bucket_name = os.environ["AWS_S3_BUCKET"]

    dataset = ParquetDataset(
        database="default",
        table=table_name,
        partition_cols=["col1", "col2"],
        path=f"s3://{bucket_name}/{table_name}/",
        pandera_schema=schema,
    )

    yield dataset
    dataset.delete()


@pytest.fixture
def df() -> pd.DataFrame:
    """Create a dataframe."""
    return pd.DataFrame(
        {
            "col1": [1, 2, 3],
            "col2": ["2021-01-01", "2021-01-02", "2021-01-03"],
            "col3": [1.0, 2.0, 3.0],
            "col4": ["a", "b", "z"],
            "col5": ["a", "b", "c"],
        }
    )


def test_dataset_create(dataset: ParquetDataset) -> None:
    """Test the create method."""
    dataset.create()
    assert wr.catalog.does_table_exist(database=dataset.database, table=dataset.table)

    with pytest.raises(Exception):
        dataset.create(if_exists="raise")

    with pytest.warns(Warning):
        dataset.create(if_exists="warn")


def test_dataset_update(
    dataset: ParquetDataset, schema: pa.DataFrameSchema, df: pd.DataFrame
) -> None:
    """Test the update method."""
    dataset.create()
    dataset.update(df)
    out = dataset.read()
    assert out.equals(schema(df))


def test_dataset_read_partition(
    dataset: ParquetDataset, schema: pa.DataFrameSchema, df: pd.DataFrame
) -> None:
    """Test the read method."""
    dataset.create()
    dataset.update(df)
    out = dataset.read(partition={"col1": "1", "col2": "2021-01-01"})
    assert out.equals(schema(df[(df["col1"] == 1) & (df["col2"] == "2021-01-01")]))


def test_dataset_delete_partition(
    dataset: ParquetDataset, schema: pa.DataFrameSchema, df: pd.DataFrame
) -> None:
    """Test the delete method."""
    dataset.create()
    dataset.update(df)
    dataset.delete(partition={"col1": "1", "col2": "2021-01-01"})
    out = dataset.read()
    assert out.equals(
        schema(df[~((df["col1"] == 1) & (df["col2"] == "2021-01-01"))]).reset_index(
            drop=True
        )
    )

    dataset.update(df, overwrite=True)
    dataset.delete(partition={"col2": "2021-01-01"})
    out = dataset.read()
    assert out.equals(schema(df[~(df["col2"] == "2021-01-01")]).reset_index(drop=True))
