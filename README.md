# A toolkit to seemlessly create parquet datasets on AWS S3, sync them with AWS Glue and query them with AWS Athena

## Motivation
An object-oriented interface for defining parquet datasets for AWS built on top of awswrangler and pandera

## How to setup

Using pip:

```bash
pip install aws_parquet
```

## How to use

Creating a parquet dataset that will get registered in AWS Glue

```python
from aws_parquet import ParquetDataset
import pandera as pa
from pandera.typing import Series

class MyDatasetSchema(pa.SchemaModel):
    col1: Series[int] = pa.Field(nullable=False, ge=0, lt=10)
    col2: Series[pa.DateTime]
    col3: Series[float]

class MyDataset(ParquetDataset):
    
    @property
    def schema(self) -> ParquetDatasetSchema:
        return MyDatasetSchema.to_schema()
    
    @property
    def partition_columns(self) -> List[str]:
        return ["col1", "col2"]

    @property
    def path(self) -> str:
        return "s3://my-bucket/my-dataset"
    
    @property
    def database(self) -> str:
        return "my_database"
    
    @property
    def table(self) -> str:
        return "my_table"

dataset = MyDataset()
dataset.create()
```

instead with awswrangler one would have to do the following:

```python
import awswrangler as wr

wr.catalog.create_parquet_table(
    database="my_database",
    path="s3://my-bucket/my-dataset",
    table="my_table",
    partitions_types={"col1": "bigint", "col2": "timestamp"},
    columns_types={"col3": "double"},
)
```

Appending new data to the dataset

```python
df = pd.DataFrame({
    "col1": [1, 2, 3],
    "col2": ["2021-01-01", "2021-01-02", "2021-01-03"],
    "col3": [1.0, 2.0, 3.0]
})

dataset.update(df)
```

instead with awswrangler one would have to do the following:

```python
df = pd.DataFrame({
    "col1": [1, 2, 3],
    "col2": ["2021-01-01", "2021-01-02", "2021-01-03"],
    "col3": [1.0, 2.0, 3.0]
})

# perform schema validation and type casting 
df_validated = validate_schema(df)

wr.s3.to_parquet(
    df=df,
    path="s3://my-bucket/my-dataset",
    dataset=True,
    database="my_database",
    table="my_table",
    partition_cols=["col1", "col2"],
    mode="append"
)
```

Overwriting data in the dataset

```python
df = pd.DataFrame({
    "col1": [4, 5, 6],
    "col2": ["2021-01-01", "2021-01-02", "2021-01-03"],
    "col3": [1.0, 2.0, 3.0]
})
dataset.update(df, overwrite=True)
```

instead with awswrangler one would have to do the following:

```python
df_overwrite = pd.DataFrame({
    "col1": ["4", "5", "6"],
    "col2": ["2021-01-04", "2021-01-05", "2021-01-06"],
    "col3": [7.0, 8.0, 9.0]
})

df_overwrite_validated = MyDatasetSchema.validate(df_overwrite)

wr.s3.to_parquet(
    df=df_overwrite_validated,
    path="s3://my-bucket/my-dataset",
    dataset=True,
    database="my_database",
    table="my_table",
    partition_cols=["col1", "col2"],
    mode="overwrite_partitions"
)
```

Reading a partition of the dataset

```python
df = dataset.read({"col2": "2021-01-01"})
```

instead with awswrangler one would have to do the following:

```python
df = wr.s3.read_parquet(
    path="s3://my-bucket/my-dataset",
    dataset=True,
    database="my_database",
    table="my_table",
    partition_filter=lambda x: pd.Timestamp(x["col2"]) == pd.Timestamp("2021-01-01")
)
```

Deleting a partition of the dataset

```python
dataset.delete_partition({"col1": 4, "col2": "2021-01-04"})
```

instead with awswrangler one would have to do the following:

```python
wr.s3.delete_objects(path="s3://infima-package-testing/foo/bar/col1=4/col2=2021-01-04")
wr.catalog.delete_partitions(
    database="default",
    table="foo_bar",
    partitions_values=[["4", "2021-01-04 00:00:00"]],
)
```



