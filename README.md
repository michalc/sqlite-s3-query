# sqlite-s3-query [![CircleCI](https://circleci.com/gh/michalc/sqlite-s3-query.svg?style=shield)](https://circleci.com/gh/michalc/sqlite-s3-query) [![Test Coverage](https://api.codeclimate.com/v1/badges/8e6c25c35521d6b338fa/test_coverage)](https://codeclimate.com/github/michalc/sqlite-s3-query/test_coverage)


Python function to query a SQLite file stored on S3. It uses HTTP range requests to avoid downloading the entire file, and so is suitable for large databases.

Operations that write to the database are not supported. However, S3 object-versioning is used, and required, so each query should complete succesfully even if the database is replaced concurrently by another S3 client.


## Installation

sqlite-s3-query depends on [APSW](https://github.com/rogerbinns/apsw), which is not available on PyPI, and can be installed directly from GitHub.

```bash
pip install sqlite_s3_query
pip install https://github.com/rogerbinns/apsw/releases/download/3.36.0-r1/apsw-3.36.0-r1.zip --global-option=fetch --global-option=--version --global-option=3.36.0 --global-option=--all --global-option=build --global-option=--enable-all-extensions
```


## Usage

```python
from sqlite_s3_query import sqlite_s3_query

results_iter = sqlite_s3_query(
    'SELECT * FROM my_table WHERE my_column = ?', params=('my-value',),
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
)

for row in results_iter:
    print(row)
```

If in your project you use multiple queries to the same file, `functools.partial` can be used to make an interface with less duplication.

```python
from functools import partial
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
)

for row in query_my_db('SELECT * FROM my_table WHERE my_col = ?', params=('my-value',)):
    print(row)

for row in query_my_db('SELECT * FROM my_table_2 WHERE my_col = ?', params=('my-value',)):
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

for row in query_my_db('SELECT * FROM my_table_2 WHERE my_col = ?', params=('my-value',)):
    print(row)
```
