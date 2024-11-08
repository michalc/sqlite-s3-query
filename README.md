# sqlite-s3-query

[![PyPI package](https://img.shields.io/pypi/v/sqlite-s3-query?label=PyPI%20package&color=%234c1)](https://pypi.org/project/sqlite-s3-query/) [![Test suite](https://img.shields.io/github/actions/workflow/status/michalc/sqlite-s3-query/test.yml?label=Test%20suite)](https://github.com/michalc/sqlite-s3-query/actions/workflows/test.yml) [![Code coverage](https://img.shields.io/codecov/c/github/michalc/sqlite-s3-query?label=Code%20coverage)](https://app.codecov.io/gh/michalc/sqlite-s3-query)

Python context managers to query a SQLite file stored on S3. It uses multiple HTTP range requests per query to avoid downloading the entire file, and so is suitable for large databases.

All queries using the same instance of the context will query the same version of the database object in S3. This means that a context is roughly equivalent to a REPEATABLE READ transaction, and queries should complete succesfully even if the database is replaced concurrently by another S3 client. [Versioning _must_ be enabled on the S3 bucket](#versioning).

SQL statements that write to the database are not supported. If you're looking for a way to write to a SQLite database in S3, try [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs).

Inspired by [phiresky's sql.js-httpvfs](https://github.com/phiresky/sql.js-httpvfs), and [dacort's Stack Overflow answer](https://stackoverflow.com/a/59434097/1319998).


## Installation

You can install sqlite-s3-query from [PyPI](https://pypi.org/project/sqlite-s3-query/) using pip.

```bash
pip install sqlite_s3_query
```

This will automatically install [HTTPX](https://www.python-httpx.org/), which is used to communicate with S3. A package often used to communciate with S3 from Python is [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html), but sqlite-s3-query does not use boto3.

The libsqlite3 binary library is also required, but this is typically already installed on most systems. The earliest version of libsqlite3 known to work is 3.7.15 (2012-12-12).


## Usage

For single-statement queries, the `sqlite_s3_query` function can be used.

```python
from sqlite_s3_query import sqlite_s3_query

with sqlite_s3_query(url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite') as query:

    with query('SELECT * FROM my_table WHERE my_column = ?', params=('my-value',)) as (columns, rows):
        for row in rows:
            print(row)

    # Exactly the same results, even if the object in S3 was replaced
    with query('SELECT * FROM my_table WHERE my_column = ?', params=('my-value',)) as (columns, rows):
        for row in rows:
            print(row)

    # Or can use named parameters
    with query('SELECT * FROM my_table WHERE my_column = :my_param', named_params=((':my_param', 'my-value'),)) as (columns, rows):
        for row in rows:
            print(row)
```

For multi-statement queries, the `sqlite_s3_query_multi` function can be used.

```python
from sqlite_s3_query import sqlite_s3_query_multi

with sqlite_s3_query_multi(url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite') as query_multi:
    for (columns, rows) in query_multi('''
            SELECT * FROM my_table_a WHERE my_column_a = ?;
            SELECT * FROM my_table_b WHERE my_column_b = ?;
    ''', params=(('my-value-a',), ('my-value-b',)):
        for row in rows:
            print(row)
```

If in your project you query the same object from multiple places, `functools.partial` can be used to make an interface with less duplication.

```python
from functools import partial
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
)

with \
        query_my_db() as query, \
        query('SELECT * FROM my_table WHERE my_col = ?', params=('my-value',)) as (columns, rows):

    for row in rows:
        print(row)

with \
        query_my_db() as query, \
        query('SELECT * FROM my_table_2 WHERE my_col = ?', params=('my-value',)) as (columns, rows):

    for row in rows:
        print(row)
```

### Pandas DataFrame

You can create a Pandas DataFrame from query results by passing the `rows` iterable and `columns` tuple to the `DataFrame` constructor as below.

```python
import pandas as pd
from sqlite_s3_query import sqlite_s3_query

with \
        sqlite_s3_query(url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite') as query, \
        query('SELECT * FROM my_table WHERE my_column = ?', params=('my-value',)) as (columns, rows):

    df = pd.DataFrame(rows, columns=columns)

print(df)
```

### Permissions

The AWS credentials must have both the `s3:GetObject` and `s3:GetObjectVersion` permissions on the database object. For example if the database is at the key `my-db.sqlite` in bucket `my-bucket`, then the minimal set of permissions are shown below.

```json
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:GetObjectVersion"],
        "Resource": "arn:aws:s3:::my-bucket/my-db.sqlite"
    }]
}
```

### Credentials

The AWS region and the credentials are taken from environment variables, but this can be changed using the `get_credentials` parameter. Below shows the default implementation of this that can be overriden.

```python
from sqlite_s3_query import sqlite_s3_query
import os

def get_credentials(_):
    return (
        os.environ['AWS_REGION'],
        os.environ['AWS_ACCESS_KEY_ID'],
        os.environ['AWS_SECRET_ACCESS_KEY'],
        os.environ.get('AWS_SESSION_TOKEN'),  # Only needed for temporary credentials
    )

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_credentials=get_credentials,
)

with \
        query_my_db() as query, \
        query('SELECT * FROM my_table_2 WHERE my_col = ?', params=('my-value',)) as (columns, rows):

    for row in rows:
        print(row)
```

sqlite-s3-query does not install or use boto3, but if you install it separately, you can use it to fetch credentials as in the below example. This can be useful when you want to use temporary credentials associated with an ECS or EC2 role, which boto3 fetches automatically.

```python
import boto3
from sqlite_s3_query import sqlite_s3_query

def GetBoto3Credentials():
    session = boto3.Session()
    credentials = session.get_credentials()
    def get_credentials(_):
        return (session.region_name,) + credentials.get_frozen_credentials()

    return get_credentials

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_credentials=GetBoto3Credentials(),
)

with \
        query_my_db() as query, \
        query('SELECT * FROM my_table_2 WHERE my_col = ?', params=('my-value',)) as (columns, rows):

    for row in rows:
        print(row)
```

### HTTP Client

The HTTP client can be changed by overriding the the default `get_http_client` parameter, which is shown below.

```python
from functools import partial
import httpx
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_http_client=lambda: httpx.Client(transport=httpx.HTTPTransport(retries=3)),
)

with \
        query_my_db() as query, \
        query('SELECT * FROM my_table WHERE my_col = ?', params=('my-value',)) as (columns, rows):

    for row in rows:
        print(row)
```

### Location of libsqlite3

The location of the libsqlite3 library can be changed by overriding the `get_libsqlite3` parameter.

```python
from ctypes import cdll
from ctypes.util import find_library
from functools import partial
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_libsqlite3=lambda: cdll.LoadLibrary(find_library('sqlite3'))
)

with \
        query_my_db() as query, \
        query('SELECT * FROM my_table WHERE my_col = ?', params=('my-value',)) as (columns, rows):

    for row in rows:
        print(row)
```


## Multithreading

It is safe for multiple threads to call the same `query` function. Under the hood, each use of `query` uses a separate SQLite "connection" to the database combined with the `SQLITE_OPEN_NOMUTEX` flag, which makes this safe while not locking unnecessarily.


## Versioning

sqlite-s3-query is only for versioned buckets, to the point that it's a feature that it will error if run on an unversioned bucket. This is to keep the scope of this project small while giving the highest chance possible that a bucket is configured to allow queries running successfully during the replacement of the underlying database object.

This means that sqlite-s3-query is not for all use cases of querying SQLite databases on S3: specifically it won't work when versioning cannot be enabled. In these cases you will have to do something else. For example:

- Use https://github.com/litements/s3sqlite - at the time of writing it does not require versioning
- Use a fork of sqlite-s3-query that allows unversioned buckets, for example as in https://github.com/michalc/sqlite-s3-query/pull/84

This is not necessarily a permanent decision - it is possible that in future sqlite-s3-query will support unversioned buckets.


## Exceptions

Under the hood [HTTPX](https://www.python-httpx.org/) is used to communicate with S3, but any [exceptions raised by HTTPX](https://www.python-httpx.org/exceptions/) are passed through to client code unchanged. This includes `httpx.HTTPStatusError` when S3 returns a non-200 response. Most commonly this will be when S3 returns a 403 in the case of insufficient permissions on the database object being queried.

All other exceptions raised inherit from `sqlite_s3_query.SQLiteS3QueryError` as described in the following hierarchy.

### Exception hierarchy

- `SQLiteS3QueryError`

   The base class for explicitly raised exceptions.

   - `VersioningNotEnabledError`

      Versioning is not enabled on the bucket.

   - `QueryContextClosedError`

      A results iterable has been attempted to be used after the close of its surrounding query context.

   - `SQLiteError`

      SQLite has detected an error. The first element of the `args` member of the raised exception is the description of the error as provided by SQLite.


## Compatibility

- Linux (tested on Ubuntu 20.04), Windows (tested on Windows Server 2019), or macOS (tested on macOS 12)
- SQLite >= 3.7.15, (tested on 3.7.15, 3.36.0, 3.42.0, and the default version available on each OS tested)
- Python >= 3.6.7 (tested on 3.6.7, 3.7.1, 3.8.0, 3.9.0, 3.10.0, and 3.11.0)
- HTTPX >= 0.18.2 (tested on 0.18.2 with Python >= 3.6.7, and 0.24.1 with Python >= 3.7.1)
