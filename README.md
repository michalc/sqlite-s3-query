# sqlite-s3-query [![CircleCI](https://circleci.com/gh/michalc/sqlite-s3-query.svg?style=shield)](https://circleci.com/gh/michalc/sqlite-s3-query) [![Test Coverage](https://api.codeclimate.com/v1/badges/8e6c25c35521d6b338fa/test_coverage)](https://codeclimate.com/github/michalc/sqlite-s3-query/test_coverage)


Python context managers to query a SQLite file stored on S3. It uses multiple HTTP range requests per query to avoid downloading the entire file, and so is suitable for large databases.

All queries using the same instance of the context will query the same version of the database object in S3. This means that a context is roughly equivalent to a REPEATABLE READ transaction, and queries should complete succesfully even if the database is replaced concurrently by another S3 client. Versioning _must_ be enabled on the S3 bucket.

SQL statements that write to the database are not supported. If you're looking for a way to write to a SQLite database in S3, try [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs).

Inspired by [phiresky's sql.js-httpvfs](https://github.com/phiresky/sql.js-httpvfs), and [dacort's Stack Overflow answer](https://stackoverflow.com/a/59434097/1319998).


## Installation

```bash
pip install sqlite_s3_query
```

The libsqlite3 binary library is also required, but this is typically already installed on most systems. The earliest version of libsqlite3 known to work is 2012-12-12 (3.7.15).


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

### Multithreading

It is safe for multiple threads to call the same `query` function. Under the hood, each use of `query` uses a separate SQLite "connection" to the database combined with the`SQLITE_OPEN_NOMUTEX` flag, which makes this safe while not locking unnecessarily.
