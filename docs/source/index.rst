aws-parquet
===========

|PyPI version shields.io| |PyPI license| |PyPI pyversions| |Downloads|
|image1|

``aws-parquet`` is a toolkit than enables working with parquet datasets
on AWS. It handles AWS S3 reads/writes, AWS Glue catalog updates and AWS
Athena queries by providing a simple and intuitive interface.

Motivation
----------

The goal is to provide a simple and intuitive interface to create and
manage parquet datasets on AWS.

``aws-parquet`` makes use of the following tools: -
`awswrangler <https://aws-sdk-pandas.readthedocs.io/en/stable/>`__ as an
AWS SDK for pandas -
`pandera <https://pandera.readthedocs.io/en/stable/>`__ for pandas-based
data validation -
`typeguard <https://typeguard.readthedocs.io/en/stable/userguide.html>`__
and `pydantic <https://docs.pydantic.dev/latest/>`__ for runtime type
checking

Features
--------

``aws-parquet`` provides a ``ParquetDataset`` class that enables the
following operations:

-  create a parquet dataset that will get registered in AWS Glue
-  append new data to the dataset and update the AWS Glue catalog
-  read a partition of the dataset and perform proper schema validation
   and type casting
-  overwrite data in the dataset after performing proper schema
   validation and type casting
-  delete a partition of the dataset and update the AWS Glue catalog
-  query the dataset using AWS Athena

How to setup
------------

Using pip:

.. code:: bash

   pip install aws_parquet

How to use
----------

Create a parquet dataset that will get registered in AWS Glue

.. code:: python

   import os

   from aws_parquet import ParquetDataset
   import pandas as pd
   import pandera as pa
   from pandera.typing import Series

   # define your pandera schema model
   class MyDatasetSchemaModel(pa.SchemaModel):
       col1: Series[int] = pa.Field(nullable=False, ge=0, lt=10)
       col2: Series[pa.DateTime]
       col3: Series[float]

   # configuration
   database = "default"
   bucket_name = os.environ["AWS_S3_BUCKET"]
   table_name = "foo_bar"
   path = f"s3://{bucket_name}/{table_name}/"
   partition_cols = ["col1", "col2"]
   schema = MyDatasetSchemaModel.to_schema()

   # create the dataset
   dataset = ParquetDataset(
       database=database,
       table=table_name,
       partition_cols=partition_cols,
       path=path,
       pandera_schema=schema,
   )

   dataset.create()

Append new data to the dataset

.. code:: python

   df = pd.DataFrame({
       "col1": [1, 2, 3],
       "col2": ["2021-01-01", "2021-01-02", "2021-01-03"],
       "col3": [1.0, 2.0, 3.0]
   })

   dataset.update(df)

Read a partition of the dataset

.. code:: python

   df = dataset.read({"col2": "2021-01-01"})

Overwrite data in the dataset

.. code:: python

   df_overwrite = pd.DataFrame({
       "col1": [1, 2, 3],
       "col2": ["2021-01-01", "2021-01-02", "2021-01-03"],
       "col3": [4.0, 5.0, 6.0]
   })
   dataset.update(df_overwrite, overwrite=True)

Query the dataset using AWS Athena

.. code:: python

   df = dataset.query("SELECT col1 FROM foo_bar")

Delete a partition of the dataset

.. code:: python

   dataset.delete({"col1": 1, "col2": "2021-01-01"})

Delete the dataset in its entirety

.. code:: python

   dataset.delete()

.. |PyPI version shields.io| image:: https://img.shields.io/pypi/v/aws-parquet.svg
   :target: https://pypi.org/project/aws-parquet/
.. |PyPI license| image:: https://img.shields.io/pypi/l/aws-parquet.svg
   :target: https://pypi.python.org/pypi/
.. |PyPI pyversions| image:: https://img.shields.io/pypi/pyversions/aws-parquet.svg
   :target: https://pypi.python.org/pypi/aws-parquet/
.. |Downloads| image:: https://pepy.tech/badge/aws-parquet/month
   :target: https://pepy.tech/project/aws-parquet
.. |image1| image:: https://pepy.tech/badge/aws-parquet
   :target: https://pepy.tech/project/aws-parquet


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
