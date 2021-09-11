from contextlib import contextmanager
from ctypes import CFUNCTYPE, POINTER, Structure, pointer, cast, memmove, memset, sizeof, addressof, cdll, byref, string_at, c_char_p, c_int, c_double, c_int64, c_void_p, c_char
from functools import partial
from hashlib import sha256
import hmac
from datetime import datetime
import os
from re import sub
from sys import platform
from time import time
from urllib.parse import urlencode, urlsplit, quote
from uuid import uuid4

import httpx


@contextmanager
def sqlite_s3_query(url, get_credentials=lambda: (
    os.environ['AWS_REGION'],
    os.environ['AWS_ACCESS_KEY_ID'],
    os.environ['AWS_SECRET_ACCESS_KEY'],
    os.environ.get('AWS_SESSION_TOKEN'),  # Only needed for temporary credentials
), get_http_client=lambda: httpx.Client(),
   get_libsqlite3=lambda: cdll.LoadLibrary({'linux': 'libsqlite3.so', 'darwin': 'libsqlite3.dylib'}[platform])):
    libsqlite3 = get_libsqlite3()
    libsqlite3.sqlite3_errstr.restype = c_char_p
    libsqlite3.sqlite3_errmsg.restype = c_char_p
    libsqlite3.sqlite3_column_name.restype = c_char_p
    libsqlite3.sqlite3_column_double.restype = c_double
    libsqlite3.sqlite3_column_int64.restype = c_int64
    libsqlite3.sqlite3_column_blob.restype = c_void_p
    libsqlite3.sqlite3_column_bytes.restype = c_int64
    SQLITE_OK = 0
    SQLITE_IOERR = 10
    SQLITE_NOTFOUND = 12
    SQLITE_ROW = 100
    SQLITE_DONE = 101
    SQLITE_TRANSIENT = -1
    SQLITE_OPEN_READONLY = 0x00000001
    SQLITE_OPEN_URI = 0x00000040

    bind = {
        type(0): libsqlite3.sqlite3_bind_int64,
        type(0.0): libsqlite3.sqlite3_bind_double,
        type(''): lambda pp_stmt, i, value: libsqlite3.sqlite3_bind_text(pp_stmt, i, value.encode('utf-8'), len(value.encode('utf-8')), SQLITE_TRANSIENT),
        type(b''): lambda pp_stmt, i, value: libsqlite3.sqlite3_bind_blob(pp_stmt, i, value, len(value), SQLITE_TRANSIENT),
        type(None): lambda pp_stmt, i, _: libsqlite3.sqlite3_bind_null(pp_stmt, i),
    }

    extract = {
        1: libsqlite3.sqlite3_column_int64,
        2: libsqlite3.sqlite3_column_double,
        3: lambda pp_stmt, i: string_at(
            libsqlite3.sqlite3_column_blob(pp_stmt, i),
            libsqlite3.sqlite3_column_bytes(pp_stmt, i),
        ).decode(),
        4: lambda pp_stmt, i: string_at(
            libsqlite3.sqlite3_column_blob(pp_stmt, i),
            libsqlite3.sqlite3_column_bytes(pp_stmt, i),
        ),
        5: lambda pp_stmt, i: None,
    }

    vfs_name = 's3-' + str(uuid4())
    file_name = 's3-' + str(uuid4())
    body_hash = sha256(b'').hexdigest()
    scheme, netloc, path, _, _ = urlsplit(url)

    def run(func, *args):
        res = func(*args)
        if res != 0:
            raise Exception(libsqlite3.sqlite3_errstr(res).decode())

    def run_with_db(db, func, *args):
        if func(*args) != 0:
            raise Exception(libsqlite3.sqlite3_errmsg(db).decode())

    @contextmanager
    def make_auth_request(http_client, method, params, headers):
        region, access_key_id, secret_access_key, session_token = get_credentials()
        to_auth_headers = headers + (
            (('x-amz-security-token', session_token),) if session_token is not None else \
            ()
        )
        request_headers = aws_sigv4_headers(
            access_key_id, secret_access_key, region, method, to_auth_headers, params,
        )
        url = f'{scheme}://{netloc}{path}?{urlencode(params)}'
        with http_client.stream(method, url, headers=request_headers) as response:
            response.raise_for_status()
            yield response

    def aws_sigv4_headers(
        access_key_id, secret_access_key, region, method, to_auth_headers, params,
    ):
        algorithm = 'AWS4-HMAC-SHA256'

        now = datetime.utcnow()
        amzdate = now.strftime('%Y%m%dT%H%M%SZ')
        datestamp = now.strftime('%Y%m%d')
        credential_scope = f'{datestamp}/{region}/s3/aws4_request'

        to_auth_headers_lower = tuple((
            (header_key.lower(), ' '.join(header_value.split()))
            for header_key, header_value in to_auth_headers
        ))
        required_headers = (
            ('host', netloc),
            ('x-amz-content-sha256', body_hash),
            ('x-amz-date', amzdate),
        )
        headers = sorted(to_auth_headers_lower + required_headers)
        signed_headers = ';'.join(key for key, _ in headers)

        def signature():
            def canonical_request():
                canonical_uri = quote(path, safe='/~')
                quoted_params = sorted(
                    (quote(key, safe='~'), quote(value, safe='~'))
                    for key, value in params
                )
                canonical_querystring = '&'.join(f'{key}={value}' for key, value in quoted_params)
                canonical_headers = ''.join(f'{key}:{value}\n' for key, value in headers)

                return f'{method}\n{canonical_uri}\n{canonical_querystring}\n' + \
                       f'{canonical_headers}\n{signed_headers}\n{body_hash}'

            def sign(key, msg):
                return hmac.new(key, msg.encode('ascii'), sha256).digest()

            string_to_sign = f'{algorithm}\n{amzdate}\n{credential_scope}\n' + \
                             sha256(canonical_request().encode('ascii')).hexdigest()

            date_key = sign(('AWS4' + secret_access_key).encode('ascii'), datestamp)
            region_key = sign(date_key, region)
            service_key = sign(region_key, 's3')
            request_key = sign(service_key, 'aws4_request')
            return sign(request_key, string_to_sign).hex()

        return (
            (b'authorization', (
                f'{algorithm} Credential={access_key_id}/{credential_scope}, '
                f'SignedHeaders={signed_headers}, Signature=' + signature()).encode('ascii')
             ),
            (b'x-amz-date', amzdate.encode('ascii')),
            (b'x-amz-content-sha256', body_hash.encode('ascii')),
        ) + to_auth_headers

    @contextmanager
    def get_vfs(http_client):
        with make_auth_request(http_client, 'HEAD', (), ()) as response:
            head_headers = response.headers
            next(response.iter_bytes())
        version_id = head_headers['x-amz-version-id']
        size = int(head_headers['content-length'])

        def get_range(bytes_from, bytes_to):
            with make_auth_request(http_client, 'GET',
                    (('versionId', version_id),),
                    (('range', f'bytes={bytes_from}-{bytes_to}'),)
                ) as response:

                # Handle the case of the server being broken or slightly evil, returning more than
                # the number of bytes that's asked for
                range_bytes = b''
                for chunk in response.iter_bytes(chunk_size=bytes_to - bytes_from + 1):
                    range_bytes += chunk
                    if len(range_bytes) > bytes_to - bytes_from + 1:
                        break

            return range_bytes

        def make_struct(fields):
            class Struct(Structure):
                _fields_ = [(field_name, field_type) for (field_name, field_type, _) in fields]
            return Struct(*tuple(value for (_, _, value) in fields))

        x_open_type = CFUNCTYPE(c_int, c_void_p, c_char_p, c_void_p, c_int, POINTER(c_int))
        def x_open(p_vfs, z_name, p_file, flags, p_out_flags):
            memmove(p_file, addressof(file), sizeof(file))
            p_out_flags[0] = flags
            return SQLITE_OK

        x_close_type = CFUNCTYPE(c_int, c_void_p)
        def x_close(p_file):
            return SQLITE_OK

        x_read_type = CFUNCTYPE(c_int, c_void_p, c_void_p, c_int, c_int64)
        def x_read(p_file, p_out, i_amt, i_ofst):
            try:
                data = get_range(i_ofst, i_ofst + i_amt - 1)
            except Exception:
                return SQLITE_IOERR

            if len(data) != i_amt:
                return SQLITE_IOERR

            memmove(p_out, data, i_amt)
            return SQLITE_OK

        x_file_size_type = CFUNCTYPE(c_int, c_void_p, POINTER(c_int))
        def x_file_size(p_file, p_size):
            p_size[0] = size
            return SQLITE_OK

        x_file_control_type = CFUNCTYPE(c_int, c_void_p, c_int, c_void_p)
        def x_file_control(p_file, op, p_arg):
            return SQLITE_NOTFOUND

        x_device_characteristics_type = CFUNCTYPE(c_int, c_void_p)
        def x_device_characteristics(p_file):
            return 0

        x_full_pathname_type = CFUNCTYPE(c_int, c_void_p, c_char_p, c_int, POINTER(c_char))
        def x_full_pathname(p_vfs, z_name, n_out, z_out):
            memmove(z_out, file_name.encode() + b'\0', len(file_name) + 1)
            return SQLITE_OK

        x_current_time_type = CFUNCTYPE(c_int, c_void_p, POINTER(c_double))
        def x_current_time(p_vfs, c_double_p):
            c_double_p[0] = time()/86400.0 + 2440587.5;
            return SQLITE_OK

        io_methods = make_struct((
            ('i_version', c_int, 1),
            ('x_close', x_close_type, x_close_type(x_close)),
            ('x_read', x_read_type, x_read_type(x_read)),
            ('x_write', c_void_p, None),
            ('x_truncate', c_void_p, None),
            ('x_sync', c_void_p, None),
            ('x_file_size', x_file_size_type, x_file_size_type(x_file_size)),
            ('x_lock', c_void_p, None),
            ('x_unlock', c_void_p, None),
            ('x_check_reserved_lock', c_void_p, None),
            ('x_file_control', x_file_control_type, x_file_control_type(x_file_control)),
            ('x_sector_size', c_void_p, None),
            ('x_device_characteristics', x_device_characteristics_type, x_device_characteristics_type(x_device_characteristics)),
        ))
        file = make_struct((
            ('p_methods', POINTER(type(io_methods)), pointer(io_methods)),
        ))
        vfs = make_struct((
            ('i_version', c_int, 1),
            ('sz_os_file', c_int, sizeof(file)),
            ('mx_pathname', c_int, 1024),
            ('p_next', c_void_p, None),
            ('z_name', c_char_p, vfs_name.encode() + b'\0'),
            ('p_app_data', c_char_p, None),
            ('x_open', x_open_type, x_open_type(x_open)),
            ('x_delete', c_void_p, None),
            ('x_access', c_void_p, None),
            ('x_full_pathname', x_full_pathname_type, x_full_pathname_type(x_full_pathname)),
            ('x_dl_open', c_void_p, None),
            ('x_dl_error', c_void_p, None),
            ('x_dl_sym', c_void_p, None),
            ('x_dl_close', c_void_p, None),
            ('x_randomness', c_void_p, None),
            ('x_sleep', c_void_p, None),
            ('x_current_time', x_current_time_type, x_current_time_type(x_current_time)),
            ('x_get_last_error', c_void_p, None),
        ))

        run(libsqlite3.sqlite3_vfs_register, byref(vfs), 0)
        try:
            yield vfs
        finally:
            run(libsqlite3.sqlite3_vfs_unregister, byref(vfs))

    @contextmanager
    def get_db(vfs):
        db = c_void_p()
        run(libsqlite3.sqlite3_open_v2, f'file:/{file_name}?immutable=1'.encode() + b'\0', byref(db), SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, vfs_name.encode() + b'\0')
        try:
            yield db
        finally:
            run_with_db(db, libsqlite3.sqlite3_close, db)

    @contextmanager
    def get_pp_stmt(db, sql):
        pp_stmt = c_void_p()
        run_with_db(db, libsqlite3.sqlite3_prepare_v3, db, sql.encode() + b'\0', -1, 0, byref(pp_stmt), None)
        try:
            yield pp_stmt
        finally:
            run_with_db(db, libsqlite3.sqlite3_finalize, pp_stmt)

    @contextmanager
    def query(db, sql, params=()):
        with get_pp_stmt(db, sql) as pp_stmt:
            for i, param in enumerate(params):
                run_with_db(db, bind[type(param)], pp_stmt, i + 1, param)

            columns = tuple(
                libsqlite3.sqlite3_column_name(pp_stmt, i).decode()
                for i in range(0, libsqlite3.sqlite3_column_count(pp_stmt))
            )

            def rows():
                while True:
                    res = libsqlite3.sqlite3_step(pp_stmt)
                    if res == SQLITE_DONE:
                        break
                    if res != SQLITE_ROW:
                        raise Exception(libsqlite3.sqlite3_errstr(res).decode())

                    yield tuple(
                        extract[libsqlite3.sqlite3_column_type(pp_stmt, i)](pp_stmt, i)
                        for i in range(0, len(columns))
                    )

            yield columns, rows()

    with \
            get_http_client() as http_client, \
            get_vfs(http_client) as vfs, \
            get_db(vfs) as db:

        yield partial(query, db)
