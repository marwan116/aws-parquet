"""Dataset for Parquet files in S3."""
import os
import warnings
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    overload,
    Tuple,
    Union,
)

import awswrangler as wr
import pandas as pd
import pandera as pa
from pydantic import BaseModel
from typeguard import typechecked

from aws_parquet._types import DataFrameOrIteratorT, PartitionLike
from aws_parquet.interface import ParquetDatasetInterface


@typechecked
def extract_partitions_metadata_from_paths(
    path: str, paths: List[str]
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Extract partitions metadata from Amazon S3 paths."""
    path = path if path.endswith("/") else f"{path}/"
    partitions_types: Dict[str, str] = {}
    partitions_values: Dict[str, List[str]] = {}
    for p in paths:
        if path not in p:
            raise ValueError(f"Object {p} is not under the root path ({path}).")
        path_wo_filename: str = p.rpartition("/")[0] + "/"
        if path_wo_filename not in partitions_values:
            path_wo_prefix: str = path_wo_filename.replace(f"{path}/", "")
            dirs: Tuple[str, ...] = tuple(
                x
                for x in path_wo_prefix.split("/")
                if (x != "") and (x.count("=") == 1)
            )
            if dirs:
                values_tups = cast(
                    Tuple[Tuple[str, str]], tuple(tuple(x.split("=")[:2]) for x in dirs)
                )
                values_dics: Dict[str, str] = dict(values_tups)
                p_values: List[str] = list(values_dics.values())
                p_types: Dict[str, str] = {x: "string" for x in values_dics.keys()}
                if not partitions_types:
                    partitions_types = p_types
                if p_values:
                    partitions_types = p_types
                    partitions_values[path_wo_filename] = p_values
                elif p_types != partitions_types:
                    raise ValueError(
                        f"At least two different partitions pandera_schema detected: "
                        f"{partitions_types} and {p_types}"
                    )
    if not partitions_types:
        return None, None
    return partitions_types, partitions_values


class FrozenBaseModel(BaseModel):
    """Base model with frozen fields."""

    class Config:
        """Model config."""

        frozen = True
        extras = "forbid"


@typechecked
class ParquetDataset(ParquetDatasetInterface, FrozenBaseModel):
    """Dataset for Parquet files in S3."""

    database: str
    table: str
    path: str
    pandera_schema: pa.DataFrameSchema
    partition_cols: List[str] = []
    compression: Literal["snappy", "gzip", "zstd", None] = "snappy"

    # TODO - check that the pandera schema is compatible for parquet serde

    @property
    def categorical_columns(self) -> List[str]:
        """Return the categorical columns of the dataset."""
        return [
            col_name
            for col_name, col_pandera_schema in self.pandera_schema.columns.items()
            if str(col_pandera_schema.dtype) == "category"
        ]

    @property
    def open_ended_categorical_columns(self) -> List[str]:
        """Return the categorical columns of the dataset with no categories defined."""
        return [
            col_name
            for col_name, col_pandera_schema in self.pandera_schema.columns.items()
            if (
                str(col_pandera_schema.dtype) == "category"
                and (col_pandera_schema.dtype.type.categories is None)
            )
        ]

    @property
    def columns(self) -> List[str]:
        """Return the columns of the dataset."""
        return list(self.pandera_schema.columns.keys())

    def _build_coercible_pandera_schema_given_columns(
        self, columns: Optional[List[str]] = None
    ) -> pa.DataFrameSchema:
        """Build the pandera_schema given a list of columns."""
        if columns is None:
            pandera_schema = self.pandera_schema
            pandera_schema.coerce = True
            return pandera_schema

        pandera_schema = pa.DataFrameSchema(
            columns={
                col_name: col_pandera_schema
                for col_name, col_pandera_schema in self.pandera_schema.columns.items()
                if col_name in columns
            }
        )
        pandera_schema.coerce = True
        return pandera_schema

    def _build_coercible_pandera_schema_given_partition(
        self, partition: PartitionLike
    ) -> pa.DataFrameSchema:
        """Build the pandera_schema of the partition."""
        return self._build_coercible_pandera_schema_given_columns(
            columns=list(partition.keys())
        )

    def _get_sample_df(self) -> pd.DataFrame:
        """Return a sample dataframe with the dataset pandera_schema."""
        cols = []
        for col, pa_col in self.pandera_schema.columns.items():
            if str(pa_col.dtype.type) == "category":
                s = self._get_sample_categorical_series(col, pa_col)

            else:
                s = pd.Series([], dtype=pa_col.dtype.type, name=col)

            cols.append(s)

        return pd.concat(cols, axis=1)

    def _get_sample_categorical_series(self, col: str, pa_col: pa.Column) -> pd.Series:
        if pa_col.dtype.type.categories is None:
            # if no categories are provided then assume the values are strings
            sample_string = "sample"
            s = pd.Series(
                [],
                dtype=pd.CategoricalDtype(categories=[sample_string], ordered=False),
                name=col,
            )
        else:
            s = pd.Series([], dtype=pa_col.dtype.type, name=col)
        return s

    def _get_athena_columns_types(self) -> Dict[str, str]:
        """Return the columns types for Athena."""
        df = self._get_sample_df()
        col_types, _ = wr._data_types.athena_types_from_pandas_partitioned(
            df=df,
            index=False,
            partition_cols=self.partition_cols,
        )

        return col_types

    def _get_athena_partitions_types(self) -> Dict[str, str]:
        """Return the partitions types for Athena."""
        df = self._get_sample_df()
        _, part_types = wr._data_types.athena_types_from_pandas_partitioned(
            df=df,
            index=False,
            partition_cols=self.partition_cols,
        )

        return part_types

    def _discover_partitions_from_s3(self) -> Optional[Dict[str, List[str]]]:
        paths = wr.s3.list_objects(path=self.path)

        _, partition_values = extract_partitions_metadata_from_paths(
            path=self.path, paths=paths
        )
        return partition_values

    def create(
        self,
        if_exists: Literal["ignore", "warn", "raise"] = "ignore",
        sync: bool = True,
    ) -> None:
        """Create the dataset."""
        wr.catalog.create_database(name=self.database, exist_ok=True)

        table_exists_in_catalog = wr.catalog.does_table_exist(
            database=self.database, table=self.table
        )
        if table_exists_in_catalog:
            if if_exists == "warn":
                warnings.warn(
                    message=(
                        f"Table {self.table} already exists"
                        f" in database {self.database}."
                    ),
                    category=RuntimeWarning,
                )

            elif if_exists == "raise":
                raise RuntimeError(
                    f"Table {self.table} already exists in database {self.database}."
                )

            elif if_exists == "ignore":
                return

        wr.catalog.create_parquet_table(
            database=self.database,
            path=self.path,
            table=self.table,
            columns_types=self._get_athena_columns_types(),
            partitions_types=self._get_athena_partitions_types(),
        )

        if sync:
            self.sync()

    def sync(self) -> None:
        """Sync the dataset between s3 and glue.

        Note this is mainly required if the dataset has been modified outside
        of this class.
        """
        partition_values = self._discover_partitions_from_s3()

        wr.catalog.delete_all_partitions(database=self.database, table=self.table)

        if partition_values is not None:
            wr.catalog.add_parquet_partitions(
                database=self.database,
                table=self.table,
                partitions_values=partition_values,
                compression=self.compression,
                columns_types=self._get_athena_columns_types(),
            )

    def _build_partition_filter(
        self, partition_values: PartitionLike
    ) -> Callable[[Dict[str, str]], bool]:
        """Build a partition filter function."""

        def compare_partition(partition: PartitionLike) -> bool:
            """Compare the partition."""
            partition_df = pd.DataFrame(
                {k: [v] for k, v in partition.items() if k in partition_values.keys()}
            )
            partition_pandera_schema = (
                self._build_coercible_pandera_schema_given_partition(partition_values)
            )
            partition_df_coerced = partition_pandera_schema(partition_df)

            partition_values_df = pd.DataFrame(
                {k: [v] for k, v in partition_values.items()}
            )
            partition_values_coerced = partition_pandera_schema(partition_values_df)
            return cast(bool, partition_df_coerced.equals(partition_values_coerced))

        return compare_partition

    def _apply_pandera_schema(
        self, df: pd.DataFrame, pandera_schema: pa.DataFrameSchema
    ) -> pd.DataFrame:
        out = pandera_schema(df)

        for col in self.categorical_columns:
            out[col] = out[col].cat.rename_categories(lambda x: str(x))

        for col in self.open_ended_categorical_columns:
            out[col] = out[col].cat.remove_unused_categories()

        return out[self.columns]

    def _coerce_and_check_pandera_schema(
        self,
        df: DataFrameOrIteratorT,
        columns: Optional[List[str]],
    ) -> DataFrameOrIteratorT:
        """Coerce the pandera_schema and check the pandera_schema."""
        pandera_schema = self._build_coercible_pandera_schema_given_columns(columns)
        if isinstance(df, pd.DataFrame):
            return cast(
                DataFrameOrIteratorT,
                self._apply_pandera_schema(df=df, pandera_schema=pandera_schema),
            )

        elif isinstance(df, Iterator):
            return cast(
                DataFrameOrIteratorT,
                (
                    self._apply_pandera_schema(df=sub_df, pandera_schema=pandera_schema)
                    for sub_df in df
                ),
            )

        else:
            raise TypeError("df must be a DataFrame or an Iterator of DataFrames.")

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
        partition_filter = (
            self._build_partition_filter(partition) if partition is not None else None
        )

        out = wr.s3.read_parquet(
            path=self.path,
            dataset=True,
            chunked=chunked,
            partition_filter=partition_filter,
            columns=columns,
            use_threads=use_threads,
        )

        return self._coerce_and_check_pandera_schema(out, columns=columns)

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
        """Perform an athena query on the parquet dataset."""
        return wr.athena.read_sql_query(
            sql=sql,
            categories=self.categorical_columns,
            ctas_approach=ctas_approach,
            database=self.database,
            chunksize=chunksize,
            use_threads=use_threads,
            workgroup=workgroup,
            **kwargs,
        )

    def update(self, data: pd.DataFrame, overwrite: bool = False) -> None:
        """Update the dataset."""
        pandera_schema = self._build_coercible_pandera_schema_given_columns()
        input_df = pandera_schema(data)
        wr.s3.to_parquet(
            df=input_df,
            path=self.path,
            dataset=True,
            mode="overwrite_partitions" if overwrite else "append",
            partition_cols=self.partition_cols,
            database=self.database,
            table=self.table,
        )

    def _get_partition_record(self, partition: PartitionLike) -> Dict[str, str]:
        """Get the partition record."""
        partition_df = pd.DataFrame({k: [v] for k, v in partition.items()})
        partition_pandera_schema = self._build_coercible_pandera_schema_given_partition(
            partition
        )
        partition_df_coerced = partition_pandera_schema(partition_df)
        partition_record = partition_df_coerced.to_dict(orient="records")[0]
        return {k: str(v) for k, v in partition_record.items()}

    def _build_path(self, partition_record: PartitionLike) -> str:
        """Build the path pattern for a given partition."""
        path = self.path
        for col in self.partition_cols:
            if col in partition_record:
                val = partition_record[col]
                path = os.path.join(path, f"{col}={val}/")
            else:
                path = os.path.join(path, "*/")

        path = os.path.join(path, "*")
        return path

    def delete(self, partition: Optional[PartitionLike] = None) -> None:
        """Delete data optionally only a given partition."""
        if partition:
            partition_record = self._get_partition_record(partition)
            path = self._build_path(partition_record)
            _, partition_values = extract_partitions_metadata_from_paths(
                path=self.path, paths=wr.s3.list_objects(path)
            )

            if partition_values:
                for path, part_vals in partition_values.items():
                    wr.s3.delete_objects(path)
                    wr.catalog.delete_partitions(
                        database=self.database,
                        table=self.table,
                        partitions_values=[part_vals],
                    )

        else:
            wr.s3.delete_objects(path=self.path)
            wr.catalog.delete_table_if_exists(database=self.database, table=self.table)
