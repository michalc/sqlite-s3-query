# sqlite-s3-query [![CircleCI](https://circleci.com/gh/michalc/sqlite-s3-query.svg?style=shield)](https://circleci.com/gh/michalc/sqlite-s3-query) [![Test Coverage](https://api.codeclimate.com/v1/badges/8e6c25c35521d6b338fa/test_coverage)](https://codeclimate.com/github/michalc/sqlite-s3-query/test_coverage)


Python context manager to query a SQLite file stored on S3. It uses multiple HTTP range requests per query to avoid downloading the entire file, and so is suitable for large databases.

All queries using the same instance of the context will query the same version of the database object in S3. This means that a context is roughly equivalent to a REPEATABLE READ transaction, and queries should complete succesfully even if the database is replaced concurrently by another S3 client. Versioning _must_ be enabled on the S3 bucket.

SQL statements that write to the database are not supported.

Inspired by [phiresky's sql.js-httpvfs](https://github.com/phiresky/sql.js-httpvfs), and [dacort's Stack Overflow answer](https://stackoverflow.com/a/59434097/1319998).


## Installation

```bash
pip install sqlite_s3_query
```

The libsqlite3 binary library is also required, but this is typically already installed on most systems.


## Usage

```python
from sqlite_s3_query import sqlite_s3_query

with sqlite_s3_query(url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite') as query:
    for row in query('SELECT * FROM my_table WHERE my_column = ?', params=('my-value',)):
        print(row)
```

If in your project you query the same object from multiple places, `functools.partial` can be used to make an interface with less duplication.

```python
from functools import partial
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
)

with query_my_db() as query:
    for row in query('SELECT * FROM my_table WHERE my_col = ?', params=('my-value',)):
        print(row)

with query_my_db() as query:
    for row in query('SELECT * FROM my_table_2 WHERE my_col = ?', params=('my-value',)):
        print(row)
```

The AWS region and the credentials are taken from environment variables, but this can be changed using the `get_credentials` parameter. Below shows the default implementation of this that can be overriden.

```python
import os
from functools import partial
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_credentials=lambda: (
        os.environ['AWS_DEFAULT_REGION'],
        os.environ['AWS_ACCESS_KEY_ID'],
        os.environ['AWS_SECRET_ACCESS_KEY'],
        os.environ.get('AWS_SESSION_TOKEN'),  # Only needed for temporary credentials
    ),
)

with query_my_db() as query:
    for row in query_my_db('SELECT * FROM my_table_2 WHERE my_col = ?', params=('my-value',)):
        print(row)
```

The HTTP client can be changed by overriding the the default `get_http_client` parameter, which is shown below.

```python
from functools import partial
import httpx
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_http_client=lambda: httpx.Client(),
)

with query_my_db() as query:
    for row in query_my_db('SELECT * FROM my_table WHERE my_col = ?', params=('my-value',)):
        print(row)
```

The location of the libsqlite3 library can be changed by overriding the `get_libsqlite3` parameter.

```python
from functools import partial
from sys import playform
import httpx
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_libsqlite3=lambda: cdll.LoadLibrary({'linux': 'libsqlite3.so', 'darwin': 'libsqlite3.dylib'}[platform])
)

with query_my_db() as query:
    for row in query_my_db('SELECT * FROM my_table WHERE my_col = ?', params=('my-value',)):
        print(row)
```
