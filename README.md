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
```

For multi-statement queries, the `sqlite_s3_query_multi` function can be used.

```python
from sqlite_s3_query import sqlite_s3_query_multi

with sqlite_s3_query_multi(url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite') as query_multi:
    for (columns, rows) in query_multi('''
            SELECT * FROM my_table_a WHERE my_column_a = ?;
            SELECT * FROM my_table_b WHERE my_column_b = ?;
    ''', params=('my-value-a','my-value-b')):
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

### Credentials

The AWS region and the credentials are taken from environment variables, but this can be changed using the `get_credentials` parameter. Below shows the default implementation of this that can be overriden.

```python
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

How to use this to fetch credentials for the IAM role associated with an ECS container is shown in the example below.

```python
import contextlib
import os
import threading
import httpx

def GetECSCredentials():
    aws_access_key_id, aws_secret_access_key, aws_session_token = None, None, None
    expiration = datetime.datetime.fromtimestamp(0)
    lock = threading.Lock()
    aws_region = os.environ['AWS_REGION']
    creds_path = os.environ['AWS_CONTAINER_CREDENTIALS_RELATIVE_URI']

    @contextlib.contextmanager
    def lock_with_timeout():
        lock.acquire(timeout=10)
        try:
            yield
        finally:
            lock.release()

    def get_credentials(now):
        nonlocal aws_access_key_id, aws_secret_access_key, aws_session_token
        nonlocal expiration

        # If this cannot be called from multiple threads at the same time, the lock can be ommitted
        with lock_with_timeout():
            if now > expiration:
                creds = httpx.get(f'http://169.254.170.2{creds_path}').json()
                aws_access_key_id = creds['AccessKeyId']
                aws_secret_access_key = creds['SecretAccessKey']
                aws_session_token = creds['Token']
                expiration = datetime.datetime.strptime(creds['Expiration'], '%Y-%m-%dT%H:%M:%SZ')

        return aws_region, aws_access_key_id, aws_secret_access_key, aws_session_token

    return get_credentials

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_credentials=GetECSCredentials(),
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
    get_http_client=lambda: httpx.Client(),
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
