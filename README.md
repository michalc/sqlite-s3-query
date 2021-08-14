# sqlite-s3-query

Python function to query a SQLite file stored on S3. It uses HTTP range requests to avoid downloading the entire database.

> Work-in-progress. This README serves as a rough design spec.


## Usage


```python
import os
from sqlite_s3_query import sqlite_s3_query

results_iter = sqlite_s3_query(
    'SELECT * FROM my_table ORDER BY my_column',
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_credentials=lambda: (
        os.environ['AWS_DEFAULT_REGION'],
        os.environ['AWS_ACCESS_KEY_ID'],
        os.environ['AWS_SECRET_ACCESS_KEY'],
        os.environ.get('AWS_SESSION_TOKEN'),  # Only needed for temporary credentials
    ),
)

for row in results_iter:
    print(row)
```

If in your project you use multiple queries to the same file, `functools.partial` can be used to make an interface with less duplication.

```python
import os
from functools import partial
from sqlite_s3_query import sqlite_s3_query

query_my_db = partial(sqlite_s3_query,
    url='https://my-bucket.s3.eu-west-2.amazonaws.com/my-db.sqlite',
    get_credentials=lambda: (
        os.environ['AWS_DEFAULT_REGION'],
        os.environ['AWS_ACCESS_KEY_ID'],
        os.environ['AWS_SECRET_ACCESS_KEY'],
        os.environ.get('AWS_SESSION_TOKEN'),  # Only needed for temporary credentials
    ),
)

for row in query_my_db('SELECT * FROM my_table_1 ORDER BY my_column'):
    print(row)

for row in query_my_db('SELECT * FROM my_table_2 ORDER BY my_column'):
    print(row)
```

