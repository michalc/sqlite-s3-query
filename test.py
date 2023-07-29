from contextlib import contextmanager
from ctypes import cdll
from ctypes.util import find_library
import datetime
import functools
import hashlib
import hmac
import os
import socket
import sqlite3
import tempfile
import threading
import unittest
import urllib.parse
import uuid

import httpx

from sqlite_s3_query import sqlite_s3_query, sqlite_s3_query_multi


class TestSqliteS3Query(unittest.TestCase):

    def test_sqlite3_installed_on_ci(self):
        ci = os.environ.get('CI', '')
        sqlite3_version = os.environ.get('SQLITE3_VERSION', 'default')
        if ci and sqlite3_version != 'default':
            libsqlite3 = get_libsqlite3()
            self.assertEqual(libsqlite3.sqlite3_libversion_number(), int(sqlite3_version))

    def test_without_versioning(self):
        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);",()),
        ] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),()),
        ]) as db:
            put_object_without_versioning('bucket-without-versioning', 'my.db', db)

        with self.assertRaisesRegex(Exception, 'The bucket must have versioning enabled'):
            sqlite_s3_query('http://localhost:9000/bucket-without-versioning/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_libsqlite3=get_libsqlite3).__enter__()

    def test_select(self):
        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);",()),
        ] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),())
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with query('SELECT my_col_a FROM my_table') as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [('some-text-a',)] * 500)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with \
                    query('SELECT my_col_a FROM my_table') as (columns_a, rows_a), \
                    query('SELECT my_col_b FROM my_table') as (columns_b, rows_b):

                rows = [
                    (next(rows_a)[0], next(rows_b)[0])
                    for i in range(0, 500)
                ]

        self.assertEqual(rows, [('some-text-a','some-text-b')] * 500)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with get_db([
                ("CREATE TABLE my_table (my_col_a text, my_col_b text);", ()),
            ] + [
                ("INSERT INTO my_table VALUES " + ','.join(["('some-new-a', 'some-new-b')"] * 500), ()),
            ]) as db:
                put_object_with_versioning('my-bucket', 'my.db', db)

            with query('SELECT my_col_a FROM my_table') as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [('some-text-a',)] * 500)

        with self.assertRaisesRegex(Exception, 'Attempting to use finalized statement'):
            with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_libsqlite3=get_libsqlite3) as query:
                with query('SELECT my_col_a FROM my_table') as (columns, rows):
                    for row in rows:
                        break
                next(rows)

        with self.assertRaisesRegex(Exception, 'Attempting to use finalized statement'):
            with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_libsqlite3=get_libsqlite3) as query:
                with query('SELECT my_col_a FROM my_table') as (columns, rows):
                    pass
                next(rows)

    def test_select_with_named_params(self):
        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);", ())
        ] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500), ()),
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-c', 'some-text-d')"] * 100), ()),
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with query('SELECT COUNT(*) FROM my_table WHERE my_col_a = :first', named_params=((':first', 'some-text-a'),)) as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [(500,)])

    def test_select_large(self):
        empty = (bytes(4050),)

        def sqls():
            yield ("CREATE TABLE foo(content BLOB);",())
            for _ in range(0, 1200000):
                yield ("INSERT INTO foo VALUES (?);", empty)

        with get_db(sqls()) as db:
            length = 0
            for chunk in db():
                length += len(chunk)
            self.assertGreater(length, 4294967296)
            put_object_with_versioning('my-bucket', 'my.db', db)

        count = 0
        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with query('SELECT content FROM foo ORDER BY rowid LIMIT 1') as (columns, rows):
                for _ in rows:
                    count += 1

            self.assertEqual(count, 1)

            count = 0
            with query('SELECT content FROM foo ORDER BY rowid DESC LIMIT 1') as (columns, rows):
                for _ in rows:
                    count += 1

            self.assertEqual(count, 1)

    def test_select_multi(self):
        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);", ())
        ] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500), ()),
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query_multi('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            rows_list = [
                list(rows)
                for (columns, rows) in query('''
                    SELECT my_col_a FROM my_table;
                    SELECT my_col_a FROM my_table LIMIT 10;
                ''')
            ]

        self.assertEqual(rows_list, [[('some-text-a',)] * 500, [('some-text-a',)] * 10])

        with self.assertRaisesRegex(Exception, 'Just after creating context'):
            with sqlite_s3_query_multi('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_libsqlite3=get_libsqlite3) as query:
                raise Exception('Just after creating context')

        with self.assertRaisesRegex(Exception, 'Just after iterating statements'):
            with sqlite_s3_query_multi('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_libsqlite3=get_libsqlite3) as query:
                for (columns, rows) in query('''
                    SELECT my_col_a FROM my_table;
                    SELECT my_col_a FROM my_table LIMIT 10;
                '''):
                    raise Exception('Just after iterating statements')

        with self.assertRaisesRegex(Exception, 'Just after iterating first row'):
            with sqlite_s3_query_multi('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_libsqlite3=get_libsqlite3) as query:
                for (columns, rows) in query('''
                    SELECT my_col_a FROM my_table;
                    SELECT my_col_a FROM my_table LIMIT 10;
                '''):
                    for row in rows:
                        raise Exception('Just after iterating first row')

        with self.assertRaisesRegex(Exception, 'Multiple open statements'):
            with sqlite_s3_query_multi('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_libsqlite3=get_libsqlite3) as query:
                it = iter(query('''
                    SELECT my_col_a FROM my_table;
                    SELECT my_col_a FROM my_table LIMIT 10;
                '''))
                columns_1, rows_1 = next(it)
                for row in rows_1:
                    break

                columns_2, rows_2 = next(it)
                for row in rows_2:
                    raise Exception('Multiple open statements')

        with self.assertRaisesRegex(Exception, 'Attempting to use finalized statement'):
            with sqlite_s3_query_multi('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_libsqlite3=get_libsqlite3) as query:
                for columns, rows in query('''
                    SELECT my_col_a FROM my_table;
                    SELECT my_col_a FROM my_table LIMIT 10;
                '''):
                    pass

                rows_list = list(rows)

    def test_select_multi_with_named_params(self):
        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);", ())
        ] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500), ()),
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-c', 'some-text-d')"] * 100), ()),
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query_multi('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            rows_list = [
                list(rows)
                for (columns, rows) in query('''
                    SELECT COUNT(*) FROM my_table WHERE my_col_a = :first;
                    SELECT COUNT(*) FROM my_table WHERE my_col_a = :second;
                ''', named_params=(((':first', 'some-text-a'),),((':second', 'some-text-c'),)))
            ]

        self.assertEqual(rows_list, [[(500,)], [(100,)]])

    def test_select_multi_with_positional_params(self):
        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);", ())
        ] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500), ()),
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-c', 'some-text-d')"] * 100), ()),
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query_multi('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            rows_list = [
                list(rows)
                for (columns, rows) in query('''
                    SELECT COUNT(*) FROM my_table WHERE my_col_a = ?;
                    SELECT COUNT(*) FROM my_table WHERE my_col_a = ?;
                ''', params=(('some-text-a',), ('some-text-c',),))
            ]

        self.assertEqual(rows_list, [[(500,)], [(100,)]])

    def test_placeholder(self):
        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);",()),
        ] + [
            ("INSERT INTO my_table VALUES ('a','b'),('c','d')",()),
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with query("SELECT my_col_a FROM my_table WHERE my_col_b = ?", params=(('d',))) as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [('c',)])

    def test_partial(self):
        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);",()),
        ] + [
            ("INSERT INTO my_table VALUES ('a','b'),('c','d')",()),
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        query_my_db = functools.partial(sqlite_s3_query,
            url='http://localhost:9000/my-bucket/my.db',
            get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ),
            get_libsqlite3=get_libsqlite3,
        )

        with query_my_db() as query:
            with query("SELECT my_col_a FROM my_table WHERE my_col_b = ?", params=(('d',))) as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [('c',)])

    def test_time_and_non_python_identifier(self):
        with get_db([("CREATE TABLE my_table (my_col_a text, my_col_b text);",())]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            now = datetime.datetime.utcnow()
            with query("SELECT date('now'), time('now')") as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [(now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'))])
        self.assertEqual(columns, ("date('now')", "time('now')"))

    def test_non_existant_table(self):
        with get_db([("CREATE TABLE my_table (my_col_a text, my_col_b text);",())]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with self.assertRaisesRegex(Exception, 'no such table: non_table'):
                query("SELECT * FROM non_table").__enter__()

    def test_empty_object(self):
        put_object_with_versioning('my-bucket', 'my.db', lambda: (b'',))

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with self.assertRaisesRegex(Exception, 'disk I/O error'):
                query('SELECT 1').__enter__()

    def test_bad_db_header(self):
        put_object_with_versioning('my-bucket', 'my.db', lambda: (b'*' * 100,))

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with self.assertRaisesRegex(Exception, 'disk I/O error'):
                query("SELECT * FROM non_table").__enter__()

    def test_bad_db_second_half(self):
        with get_db([("CREATE TABLE my_table (my_col_a text, my_col_b text);",())] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),()),
        ] * 10) as db_full:
            db = b''.join(db_full())
            half_len = int(len(db) / 2)
            db = db[:half_len] + len(db[half_len:]) * b'-'
            put_object_with_versioning('my-bucket', 'my.db', lambda: (db,))

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_libsqlite3=get_libsqlite3) as query:
            with self.assertRaisesRegex(Exception, 'database disk image is malformed'):
                with query("SELECT * FROM my_table") as (columns, rows):
                    list(rows)

    def test_num_connections(self):
        num_connections = 0

        @contextmanager
        def server():
            nonlocal num_connections
            def _run(server_sock):
                nonlocal num_connections

                while True:
                    try:
                        downstream_sock, _ = server_sock.accept()
                    except Exception:
                        break
                    num_connections += 1
                    connection_t = threading.Thread(target=handle_downstream, args=(downstream_sock,))
                    connection_t.start()

            with shutdown(get_new_socket()) as server_sock:
                server_sock.bind(('127.0.0.1', 9001))
                server_sock.listen(socket.IPPROTO_TCP)
                threading.Thread(target=_run, args=(server_sock,)).start()
                yield server_sock

        def get_http_client():
            @contextmanager
            def client():
                with httpx.Client() as original_client:
                    class Client():
                        def stream(self, method, url, params, headers):
                            parsed_url = urllib.parse.urlparse(url)
                            url = urllib.parse.urlunparse(parsed_url._replace(netloc='localhost:9001'))
                            headers_proxy_host = tuple((key, value) for key, value in headers if key != 'host') + (('host', 'localhost:9000'),)
                            return original_client.stream(method, url, params=params, headers=headers_proxy_host)
                    yield Client()
            return client()

        with server() as server_sock:
            with get_db([
                ("CREATE TABLE my_table (my_col_a text, my_col_b text);",()),
            ] + [
                ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),()),
            ]) as db:
                put_object_with_versioning('my-bucket', 'my.db', db)

            with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_http_client=get_http_client, get_libsqlite3=get_libsqlite3) as query:
                with query('SELECT my_col_a FROM my_table') as (columns, rows):
                    rows = list(rows)

            self.assertEqual(rows, [('some-text-a',)] * 500)
            self.assertEqual(num_connections, 1)

    def test_streaming(self):
        rows_count = 0
        rows_yielded_at_request = []

        def get_http_client():
            @contextmanager
            def client():
                with httpx.Client() as original_client:
                    class Client():
                        @contextmanager
                        def stream(self, method, url, params, headers):
                            rows_yielded_at_request.append(
                                (rows_count, dict(headers).get('range'))
                            )
                            with original_client.stream(method, url,
                                params=params, headers=headers
                            ) as response:
                                yield response
                    yield Client()
            return client()

        with get_db([
            ("PRAGMA page_size = 4096;",()),
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);",()),
        ] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),()),
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_http_client=get_http_client, get_libsqlite3=get_libsqlite3) as query:
            with query('SELECT my_col_a FROM my_table') as (cols, rows):
                for row in rows:
                    rows_count += 1

        self.assertIn(rows_yielded_at_request, ([
            (0, None),
            (0, 'bytes=0-99'),
            (0, 'bytes=0-4095'),
            (0, 'bytes=24-39'),  # For older SQLite that doesn't support immutable files
            (0, 'bytes=4096-8191'),
            (0, 'bytes=8192-12287'),
            (140, 'bytes=12288-16383'),
            (276, 'bytes=16384-20479'),
            (412, 'bytes=20480-24575'),
        ], [
            (0, None),
            (0, 'bytes=0-99'),
            (0, 'bytes=0-4095'),
            (0, 'bytes=4096-8191'),
            (0, 'bytes=8192-12287'),
            (140, 'bytes=12288-16383'),
            (276, 'bytes=16384-20479'),
            (412, 'bytes=20480-24575'),
        ]))

        # Documenting the difference with the above and a query that is not streaming. In this
        # case, a query with an ORDER BY on a column that does not have an index requires SQLite to
        # fetch all the pages before yielding any rows to client code
        rows_count = 0
        rows_yielded_at_request.clear()
        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        ), get_http_client=get_http_client, get_libsqlite3=get_libsqlite3) as query:
            with query('SELECT my_col_a FROM my_table ORDER BY my_col_a') as (cols, rows):
                for row in rows:
                    rows_count += 1

        self.assertIn(rows_yielded_at_request, ([
            (0, None),
            (0, 'bytes=0-99'),
            (0, 'bytes=0-4095'),
            (0, 'bytes=24-39'),  # For older SQLite that doesn't support immutable files
            (0, 'bytes=4096-8191'),
            (0, 'bytes=8192-12287'),
            (0, 'bytes=12288-16383'),
            (0, 'bytes=16384-20479'),
            (0, 'bytes=20480-24575'),
        ], [
            (0, None),
            (0, 'bytes=0-99'),
            (0, 'bytes=0-4095'),
            (0, 'bytes=4096-8191'),
            (0, 'bytes=8192-12287'),
            (0, 'bytes=12288-16383'),
            (0, 'bytes=16384-20479'),
            (0, 'bytes=20480-24575'),
        ]))

    def test_too_many_bytes(self):
        @contextmanager
        def server():
            def _run(server_sock):
                while True:
                    try:
                        downstream_sock, _ = server_sock.accept()
                    except Exception:
                        break
                    connection_t = threading.Thread(target=handle_downstream, args=(downstream_sock,))
                    connection_t.start()

            with shutdown(get_new_socket()) as server_sock:
                server_sock.bind(('127.0.0.1', 9001))
                server_sock.listen(socket.IPPROTO_TCP)
                threading.Thread(target=_run, args=(server_sock,)).start()
                yield server_sock

        def get_http_client():
            @contextmanager
            def client():
                with httpx.Client() as original_client:
                    class Client():
                        @contextmanager
                        def stream(self, method, url, params, headers):
                            parsed_url = urllib.parse.urlparse(url)
                            url = urllib.parse.urlunparse(parsed_url._replace(netloc='localhost:9001'))
                            range_query = dict(headers).get('range')
                            is_query = range_query and range_query != 'bytes=0-99'
                            headers_proxy_host = tuple((key, value) for key, value in headers if key != 'host') + (('host', 'localhost:9000'),)
                            with original_client.stream(method, url,
                                params=params, headers=headers_proxy_host
                            ) as response:
                                chunks = response.iter_bytes()
                                def iter_bytes(chunk_size=None):
                                    yield from chunks
                                    if is_query:
                                        yield b'e'
                                response.iter_bytes = iter_bytes
                                yield response
                    yield Client()
            return client()

        with server() as server_sock:
            with get_db([
                ("CREATE TABLE my_table (my_col_a text, my_col_b text);",()),
            ] + [
                ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),()),
            ]) as db:
                put_object_with_versioning('my-bucket', 'my.db', db)

            with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_http_client=get_http_client, get_libsqlite3=get_libsqlite3) as query:
                with self.assertRaisesRegex(Exception, 'disk I/O error'):
                    query('SELECT my_col_a FROM my_table').__enter__()

    def test_disconnection(self):
        @contextmanager
        def server():
            def _run(server_sock):
                while True:
                    try:
                        downstream_sock, _ = server_sock.accept()
                    except Exception:
                        break
                    downstream_sock.close()
                    connection_t = threading.Thread(target=handle_downstream, args=(downstream_sock,))
                    connection_t.start()

            with shutdown(get_new_socket()) as server_sock:
                server_sock.bind(('127.0.0.1', 9001))
                server_sock.listen(socket.IPPROTO_TCP)
                threading.Thread(target=_run, args=(server_sock,)).start()
                yield server_sock

        def get_http_client():
            @contextmanager
            def client():
                with httpx.Client() as original_client:
                    class Client():
                        def stream(self, method, url, headers, params):
                            parsed_url = urllib.parse.urlparse(url)
                            url = urllib.parse.urlunparse(parsed_url._replace(netloc='localhost:9001'))
                            headers_proxy_host = tuple((key, value) for key, value in headers if key != 'host') + (('host', 'localhost:9000'),)
                            return original_client.stream(method, url, headers=headers_proxy_host)
                    yield Client()
            return client()

        with get_db([
            ("CREATE TABLE my_table (my_col_a text, my_col_b text);",()),
        ] + [
            ("INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),()),
        ]) as db:
            put_object_with_versioning('my-bucket', 'my.db', db)

        with server() as server_sock:
            with self.assertRaisesRegex(Exception, 'Server disconnected|Connection reset'):
                sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda now: (
                    'us-east-1',
                    'AKIAIOSFODNN7EXAMPLE',
                    'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                    None,
                ), get_http_client=get_http_client, get_libsqlite3=get_libsqlite3).__enter__()

def get_libsqlite3():
    return cdll.LoadLibrary(os.environ.get('LIBSQLITE3_PATH', find_library('sqlite3')))

def put_object_without_versioning(bucket, key, content):
    create_bucket(bucket)

    url = f'http://127.0.0.1:9000/{bucket}/{key}'
    sha = hashlib.sha256()
    length = 0
    for chunk in content():
        length += len(chunk)
        sha.update(chunk)
    body_hash = sha.hexdigest()
    parsed_url = urllib.parse.urlsplit(url)

    headers = aws_sigv4_headers(
        'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        (), 's3', 'us-east-1', parsed_url.netloc, 'PUT', parsed_url.path, (), body_hash,
    ) + ((b'content-length', str(length).encode()),)
    response = httpx.put(url, content=content(), headers=headers)
    response.raise_for_status()

def put_object_with_versioning(bucket, key, content):
    create_bucket(bucket)
    enable_versioning(bucket)

    url = f'http://127.0.0.1:9000/{bucket}/{key}'
    sha = hashlib.sha256()
    length = 0
    for chunk in content():
        length += len(chunk)
        sha.update(chunk)
    body_hash = sha.hexdigest()
    parsed_url = urllib.parse.urlsplit(url)

    headers = aws_sigv4_headers(
        'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        (), 's3', 'us-east-1', parsed_url.netloc, 'PUT', parsed_url.path, (), body_hash,
    ) + ((b'content-length', str(length).encode()),)

    response = httpx.put(url, content=content(), headers=headers)
    response.raise_for_status()

def create_bucket(bucket):
    url = f'http://127.0.0.1:9000/{bucket}/'
    content = b''
    body_hash = hashlib.sha256(content).hexdigest()
    parsed_url = urllib.parse.urlsplit(url)

    headers = aws_sigv4_headers(
        'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        (), 's3', 'us-east-1', parsed_url.netloc, 'PUT', parsed_url.path, (), body_hash,
    )
    response = httpx.put(url, content=content, headers=headers)

def enable_versioning(bucket):
    content = '''
        <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Status>Enabled</Status>
        </VersioningConfiguration>
    '''.encode()
    url = f'http://127.0.0.1:9000/{bucket}/?versioning'
    body_hash = hashlib.sha256(content).hexdigest()
    parsed_url = urllib.parse.urlsplit(url)

    headers = aws_sigv4_headers(
        'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        (), 's3', 'us-east-1', parsed_url.netloc, 'PUT', parsed_url.path, (('versioning', ''),), body_hash,
    )
    response = httpx.put(url, content=content, headers=headers)
    response.raise_for_status()


def aws_sigv4_headers(access_key_id, secret_access_key, pre_auth_headers,
                      service, region, host, method, path, params, body_hash):
    algorithm = 'AWS4-HMAC-SHA256'

    now = datetime.datetime.utcnow()
    amzdate = now.strftime('%Y%m%dT%H%M%SZ')
    datestamp = now.strftime('%Y%m%d')
    credential_scope = f'{datestamp}/{region}/{service}/aws4_request'

    pre_auth_headers_lower = tuple((
        (header_key.lower(), ' '.join(header_value.split()))
        for header_key, header_value in pre_auth_headers
    ))
    required_headers = (
        ('host', host),
        ('x-amz-content-sha256', body_hash),
        ('x-amz-date', amzdate),
    )
    headers = sorted(pre_auth_headers_lower + required_headers)
    signed_headers = ';'.join(key for key, _ in headers)

    def signature():
        def canonical_request():
            canonical_uri = urllib.parse.quote(path, safe='/~')
            quoted_params = sorted(
                (urllib.parse.quote(key, safe='~'), urllib.parse.quote(value, safe='~'))
                for key, value in params
            )
            canonical_querystring = '&'.join(f'{key}={value}' for key, value in quoted_params)
            canonical_headers = ''.join(f'{key}:{value}\n' for key, value in headers)

            return f'{method}\n{canonical_uri}\n{canonical_querystring}\n' + \
                   f'{canonical_headers}\n{signed_headers}\n{body_hash}'

        def sign(key, msg):
            return hmac.new(key, msg.encode('ascii'), hashlib.sha256).digest()

        string_to_sign = f'{algorithm}\n{amzdate}\n{credential_scope}\n' + \
                         hashlib.sha256(canonical_request().encode('ascii')).hexdigest()

        date_key = sign(('AWS4' + secret_access_key).encode('ascii'), datestamp)
        region_key = sign(date_key, region)
        service_key = sign(region_key, service)
        request_key = sign(service_key, 'aws4_request')
        return sign(request_key, string_to_sign).hex()

    return (
        (b'authorization', (
            f'{algorithm} Credential={access_key_id}/{credential_scope}, '
            f'SignedHeaders={signed_headers}, Signature=' + signature()).encode('ascii')
         ),
        (b'x-amz-date', amzdate.encode('ascii')),
        (b'x-amz-content-sha256', body_hash.encode('ascii')),
    ) + pre_auth_headers


@contextmanager
def get_db(sqls):
    with tempfile.NamedTemporaryFile() as fp:
        with sqlite3.connect(fp.name, isolation_level=None) as con:
            cur = con.cursor()
            cur.execute('BEGIN')
            for sql, params in sqls:
                cur.execute(sql, params)
            cur.execute('COMMIT')

        def db():
            with open(fp.name, 'rb') as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    yield chunk

        yield db


def get_new_socket():
    sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM,
                         proto=socket.IPPROTO_TCP)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock

def upstream_connect():
    upstream_sock = socket.create_connection(('127.0.0.1', 9000))
    upstream_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return upstream_sock

@contextmanager
def shutdown(sock):
    try:
        yield sock
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            sock.close()

def proxy(done, source, target):
    try:
        chunk = source.recv(1)
        while chunk:
            target.sendall(chunk)
            chunk = source.recv(1)
    except OSError:
        pass
    finally:
        done.set()

def handle_downstream(downstream_sock):
    with \
            shutdown(upstream_connect()) as upstream_sock, \
            shutdown(downstream_sock) as downstream_sock:

        done = threading.Event()
        threading.Thread(target=proxy, args=(done, upstream_sock, downstream_sock)).start()
        threading.Thread(target=proxy, args=(done, downstream_sock, upstream_sock)).start()
        done.wait()
