from contextlib import contextmanager
import datetime
import functools
import hashlib
import hmac
import socket
import sqlite3
import tempfile
import threading
import unittest
import urllib.parse
import uuid

import httpx

from sqlite_s3_query import sqlite_s3_query


class TestSqliteS3Query(unittest.TestCase):

    def test_select(self):
        db = get_db([
            "CREATE TABLE my_table (my_col_a text, my_col_b text);",
        ] + [
            "INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),
        ])

        put_object('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        )) as query:
            with query('SELECT my_col_a FROM my_table') as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [('some-text-a',)] * 500)

    def test_placeholder(self):
        db = get_db([
            "CREATE TABLE my_table (my_col_a text, my_col_b text);",
        ] + [
            "INSERT INTO my_table VALUES ('a','b'),('c','d')",
        ])

        put_object('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        )) as query:
            with query("SELECT my_col_a FROM my_table WHERE my_col_b = ?", params=(('d',))) as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [('c',)])

    def test_partial(self):
        db = get_db([
            "CREATE TABLE my_table (my_col_a text, my_col_b text);",
        ] + [
            "INSERT INTO my_table VALUES ('a','b'),('c','d')",
        ])

        put_object('my-bucket', 'my.db', db)

        query_my_db = functools.partial(sqlite_s3_query,
            url='http://localhost:9000/my-bucket/my.db',
            get_credentials=lambda: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            )
        )

        with query_my_db() as query:
            with query("SELECT my_col_a FROM my_table WHERE my_col_b = ?", params=(('d',))) as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [('c',)])

    def test_time_and_non_python_identifier(self):
        db = get_db(["CREATE TABLE my_table (my_col_a text, my_col_b text);"])

        put_object('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        )) as query:
            now = datetime.datetime.utcnow()
            with query("SELECT date('now'), time('now')") as (columns, rows):
                rows = list(rows)

        self.assertEqual(rows, [(now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'))])
        self.assertEqual(columns, ("date('now')", "time('now')"))

    def test_non_existant_table(self):
        db = get_db(["CREATE TABLE my_table (my_col_a text, my_col_b text);"])

        put_object('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        )) as query:
            with self.assertRaises(Exception):
                query("SELECT * FROM non_table").__enter__()

    def test_empty_object(self):
        db = get_db(["CREATE TABLE my_table (my_col_a text, my_col_b text);"])

        put_object('my-bucket', 'my.db', b'')

        with self.assertRaises(Exception):
            sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            )).__enter__()

    def test_bad_db_header(self):
        db = get_db(["CREATE TABLE my_table (my_col_a text, my_col_b text);"])

        put_object('my-bucket', 'my.db', b'*' * 100)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        )) as query:
            with self.assertRaises(Exception):
                query("SELECT * FROM non_table").__enter__()

    def test_bad_db_second_half(self):
        db = get_db(["CREATE TABLE my_table (my_col_a text, my_col_b text);"] + [
            "INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 5000),
        ])

        half_len = int(len(db) / 2)
        db = db[:half_len] + len(db[half_len:]) * b'-'
        put_object('my-bucket', 'my.db', db)

        with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda: (
            'us-east-1',
            'AKIAIOSFODNN7EXAMPLE',
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            None,
        )) as query:
            with self.assertRaises(Exception):
                with query("SELECT * FROM my_table") as (columns, rows):
                    list(rows)

    def test_num_connections(self):
        num_connections = 0

        def get_new_socket():
            sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM,
                                 proto=socket.IPPROTO_TCP)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return sock

        def upstream_connect():
            upstream_sock = socket.create_connection(('127.0.0.1', 9000))
            upstream_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return upstream_sock

        def proxy_both_directions(sock_a, sock_b):
            done = threading.Event()

            def _proxy(source, target):
                try:
                    chunk = source.recv(1)
                    while chunk:
                        target.sendall(chunk)
                        chunk = source.recv(1)
                except OSError:
                    pass
                finally:
                    done.set()

            threading.Thread(target=_proxy, args=(sock_a, sock_b)).start()
            threading.Thread(target=_proxy, args=(sock_b, sock_a)).start()
            done.wait()

        def handle_downstream(downstream_sock):
            upstream_sock = None

            try:
                upstream_sock = upstream_connect()
                proxy_both_directions(downstream_sock, upstream_sock)
            except:
                pass
            finally:
                if upstream_sock is not None:
                    try:
                        upstream_sock.close()
                    except OSError:
                        pass

                try:
                    downstream_sock.close()
                except OSError:
                    pass

        @contextmanager
        def server():
            nonlocal num_connections
            def _run(server_sock):
                nonlocal num_connections

                while True:
                    try:
                        downstream_sock, _ = server_sock.accept()
                    except Exception:
                        return
                    num_connections += 1
                    connection_t = threading.Thread(target=handle_downstream, args=(downstream_sock,))
                    connection_t.start()

            server_sock = get_new_socket()
            server_sock.bind(('127.0.0.1', 9001))
            server_sock.listen(socket.IPPROTO_TCP)
            threading.Thread(target=_run, args=(server_sock,)).start()

            try:
                yield server_sock
            finally:
                server_sock.close()

        def get_http_client():
            @contextmanager
            def client():
                with httpx.Client() as original_client:
                    class Client():
                        def stream(self, method, url, headers):
                            parsed_url = urllib.parse.urlparse(url)
                            url = urllib.parse.urlunparse(parsed_url._replace(netloc='localhost:9001'))
                            return original_client.stream(method, url, headers=headers + (('host', 'localhost:9000'),))
                    yield Client()
            return client()

        with server() as server_sock:
            db = get_db([
                "CREATE TABLE my_table (my_col_a text, my_col_b text);",
            ] + [
                "INSERT INTO my_table VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),
            ])

            put_object('my-bucket', 'my.db', db)

            with sqlite_s3_query('http://localhost:9000/my-bucket/my.db', get_credentials=lambda: (
                'us-east-1',
                'AKIAIOSFODNN7EXAMPLE',
                'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                None,
            ), get_http_client=get_http_client) as query:
                with query('SELECT my_col_a FROM my_table') as (columns, rows):
                    rows = list(rows)

            self.assertEqual(rows, [('some-text-a',)] * 500)
            self.assertEqual(num_connections, 1)

def put_object(bucket, key, content):
    create_bucket(bucket)
    enable_versioning(bucket)

    url = f'http://127.0.0.1:9000/{bucket}/{key}'
    body_hash = hashlib.sha256(content).hexdigest()
    parsed_url = urllib.parse.urlsplit(url)

    headers = aws_sigv4_headers(
        'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        (), 's3', 'us-east-1', parsed_url.netloc, 'PUT', parsed_url.path, (), body_hash,
    )
    response = httpx.put(url, content=content, headers=headers)
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


def get_db(sqls):
    with tempfile.NamedTemporaryFile() as fp:
        with sqlite3.connect(fp.name, isolation_level=None) as con:
            cur = con.cursor()
            for sql in sqls:
                cur.execute(sql)

        with open(fp.name, 'rb') as f:
            return f.read()
