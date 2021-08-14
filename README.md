# sqlite-s3-query

Python function to query a SQLite file stored on S3. It uses HTTP range requests to avoid downloading the entire file, and so is suitable for large databases.

Operations that write to the database are not supported. However, S3 object-versioning is used, and required, so each query should complete succesfully even if the database is overwritten during the query.

> Work-in-progress. This README serves as a rough design spec.


## Usage

```python
from sqlite_s3_query import sqlite_s3_query

results_iter = sqlite_s3_query(
    'SELECT * FROM my_table ORDER BY my_column',
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

for row in query_my_db('SELECT * FROM my_table_1 ORDER BY my_column'):
    print(row)

for row in query_my_db('SELECT * FROM my_table_2 ORDER BY my_column'):
    print(row)
```

The AWS region and the credentials are taken from environment variables, but this can be changed using the `get_credentials` parameter. Below shows default implementation of this that can be overriden.

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

for row in query_my_db('SELECT * FROM my_table ORDER BY my_column'):
    print(row)
```
