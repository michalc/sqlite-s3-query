from contextlib import contextmanager
from ctypes import CFUNCTYPE, POINTER, Structure, create_string_buffer, pointer, cast, memmove, memset, sizeof, addressof, cdll, byref, string_at, c_char_p, c_int, c_double, c_int64, c_void_p, c_char
from ctypes.util import find_library
from functools import partial
from hashlib import sha256
import hmac
from datetime import datetime
import os
from re import sub
from time import time
from urllib.parse import urlencode, urlsplit, quote
from uuid import uuid4

import httpx


@contextmanager
def sqlite_s3_query_multi(url, get_credentials=lambda now: (
    os.environ['AWS_REGION'],
    os.environ['AWS_ACCESS_KEY_ID'],
    os.environ['AWS_SECRET_ACCESS_KEY'],
    os.environ.get('AWS_SESSION_TOKEN'),  # Only needed for temporary credentials
), get_http_client=lambda: httpx.Client(transport=httpx.HTTPTransport(retries=3)),
   get_libsqlite3=lambda: cdll.LoadLibrary(find_library('sqlite3'))):
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
    SQLITE_OPEN_NOMUTEX = 0x00008000
    SQLITE_IOCAP_IMMUTABLE = 0x00002000

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

    vfs_name = b's3-' + str(uuid4()).encode()
    file_name = b's3-' + str(uuid4()).encode()
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
        now = datetime.utcnow()
        region, access_key_id, secret_access_key, session_token = get_credentials(now)
        to_auth_headers = headers + (
            (('x-amz-security-token', session_token),) if session_token is not None else \
            ()
        )
        request_headers = aws_sigv4_headers(
            now, access_key_id, secret_access_key, region, method, to_auth_headers, params,
        )
        url = f'{scheme}://{netloc}{path}'
        with http_client.stream(method, url, params=params, headers=request_headers) as response:
            response.raise_for_status()
            yield response

    def aws_sigv4_headers(
        now, access_key_id, secret_access_key, region, method, headers_to_sign, params,
    ):
        def sign(key, msg):
            return hmac.new(key, msg.encode('ascii'), sha256).digest()

        algorithm = 'AWS4-HMAC-SHA256'

        amzdate = now.strftime('%Y%m%dT%H%M%SZ')
        datestamp = amzdate[:8]
        credential_scope = f'{datestamp}/{region}/s3/aws4_request'

        headers = tuple(sorted(headers_to_sign + (
            ('host', netloc),
            ('x-amz-content-sha256', body_hash),
            ('x-amz-date', amzdate),
        )))
        signed_headers = ';'.join(key for key, _ in headers)

        canonical_uri = quote(path, safe='/~')
        quoted_params = sorted(
            (quote(key, safe='~'), quote(value, safe='~'))
            for key, value in params
        )
        canonical_querystring = '&'.join(f'{key}={value}' for key, value in quoted_params)
        canonical_headers = ''.join(f'{key}:{value}\n' for key, value in headers)
        canonical_request = f'{method}\n{canonical_uri}\n{canonical_querystring}\n' + \
                            f'{canonical_headers}\n{signed_headers}\n{body_hash}'

        string_to_sign = f'{algorithm}\n{amzdate}\n{credential_scope}\n' + \
                         sha256(canonical_request.encode('ascii')).hexdigest()

        date_key = sign(('AWS4' + secret_access_key).encode('ascii'), datestamp)
        region_key = sign(date_key, region)
        service_key = sign(region_key, 's3')
        request_key = sign(service_key, 'aws4_request')
        signature = sign(request_key, string_to_sign).hex()

        return (
            ('authorization', (
                f'{algorithm} Credential={access_key_id}/{credential_scope}, '
                f'SignedHeaders={signed_headers}, Signature={signature}')
            ),
        ) + headers

    @contextmanager
    def get_vfs(http_client):
        with make_auth_request(http_client, 'HEAD', (), ()) as response:
            head_headers = response.headers
            next(response.iter_bytes(), b'')

        try:
            version_id = head_headers['x-amz-version-id']
        except KeyError:
            raise Exception('The bucket must have versioning enabled')

        size = int(head_headers['content-length'])

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
            offset = 0

            try:
                with make_auth_request(http_client, 'GET',
                    (('versionId', version_id),),
                    (('range', f'bytes={i_ofst}-{i_ofst + i_amt - 1}'),)
                ) as response:
                    # Handle the case of the server being broken or slightly evil,
                    # returning more than the number of bytes that's asked for
                    for chunk in response.iter_bytes():
                        memmove(p_out + offset, chunk, min(i_amt - offset, len(chunk)))
                        offset += len(chunk)
                        if offset > i_amt:
                            break
            except Exception:
                return SQLITE_IOERR

            if offset != i_amt:
                return SQLITE_IOERR

            return SQLITE_OK

        x_file_size_type = CFUNCTYPE(c_int, c_void_p, POINTER(c_int64))
        def x_file_size(p_file, p_size):
            p_size[0] = size
            return SQLITE_OK

        x_lock_type = CFUNCTYPE(c_int, c_void_p, c_int)
        def x_lock(p_file, e_lock):
            return SQLITE_OK

        x_unlock_type = CFUNCTYPE(c_int, c_void_p, c_int)
        def x_unlock(p_file, e_lock):
            return SQLITE_OK

        x_file_control_type = CFUNCTYPE(c_int, c_void_p, c_int, c_void_p)
        def x_file_control(p_file, op, p_arg):
            return SQLITE_NOTFOUND

        x_device_characteristics_type = CFUNCTYPE(c_int, c_void_p)
        def x_device_characteristics(p_file):
            return SQLITE_IOCAP_IMMUTABLE

        x_access_type = CFUNCTYPE(c_int, c_void_p, c_char_p, c_int, POINTER(c_int))
        def x_access(p_vfs, z_name, flags, z_out):
            z_out[0] = 0
            return SQLITE_OK

        x_full_pathname_type = CFUNCTYPE(c_int, c_void_p, c_char_p, c_int, POINTER(c_char))
        def x_full_pathname(p_vfs, z_name, n_out, z_out):
            memmove(z_out, z_name, len(z_name) + 1)
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
            ('x_lock', x_lock_type, x_lock_type(x_lock)),
            ('x_unlock', x_unlock_type, x_unlock_type(x_unlock)),
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
            ('z_name', c_char_p, vfs_name),
            ('p_app_data', c_char_p, None),
            ('x_open', x_open_type, x_open_type(x_open)),
            ('x_delete', c_void_p, None),
            ('x_access', x_access_type, x_access_type(x_access)),
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
        run(libsqlite3.sqlite3_open_v2, file_name, byref(db), SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, vfs_name)
        try:
            yield db
        finally:
            run_with_db(db, libsqlite3.sqlite3_close, db)

    @contextmanager
    def get_pp_stmt_getter(db):
        # The purpose of this context manager is to make sure we finalize statements before
        # attempting to close the database, including in the case of unfinished interation

        statements = {}

        def get_pp_stmt(statement):
            try:
                return statements[statement]
            except KeyError:
                raise Exception('Attempting to use finalized statement') from None

        def finalize(statement):
            # In case there are errors, don't attempt to re-finalize the same statement
            try:
                pp_stmt = statements.pop(statement)
            except KeyError:
                return

            try:
                run_with_db(db, libsqlite3.sqlite3_finalize, pp_stmt)
            except:
                # The only case found where this errored is when we've already had an error due to
                # a malformed disk image, which will already bubble up to client code
                pass

        def get_pp_stmts(sql):
            p_encoded = POINTER(c_char)(create_string_buffer(sql.encode()))

            while True:
                pp_stmt = c_void_p()
                run_with_db(db, libsqlite3.sqlite3_prepare_v2, db, p_encoded, -1, byref(pp_stmt), byref(p_encoded))
                if not pp_stmt:
                    break

                # c_void_p is not hashable, and there is a theoretical possibility that multiple
                # exist at the same time pointing to the same memory, so use a plain object instead
                statement = object()
                statements[statement] = pp_stmt
                yield partial(get_pp_stmt, statement), partial(finalize, statement)

        try:
            yield get_pp_stmts
        finally:
            for statement in statements.copy().keys():
                finalize(statement)

    def rows(get_pp_stmt, columns):
        while True:
            pp_stmt = get_pp_stmt()
            res = libsqlite3.sqlite3_step(pp_stmt)
            if res == SQLITE_DONE:
                break
            if res != SQLITE_ROW:
                raise Exception(libsqlite3.sqlite3_errstr(res).decode())

            yield tuple(
                extract[libsqlite3.sqlite3_column_type(pp_stmt, i)](pp_stmt, i)
                for i in range(0, len(columns))
            )

    def query(vfs, sql, params=(), named_params=()):

        def zip_first(first_iterable, *iterables, default=()):
            iters = tuple(iter(iterable) for iterable in iterables)
            for value in first_iterable:
                yield (value,) + tuple(next(it, default) for it in iters)

        with \
                get_db(vfs) as db, \
                get_pp_stmt_getter(db) as get_pp_stmts:

            for (get_pp_stmt, finalize_stmt), statment_params, statement_named_params in zip_first(get_pp_stmts(sql), params, named_params):
                try:
                    pp_stmt = get_pp_stmt()
                    for i, param in enumerate(statment_params):
                        run_with_db(db, bind[type(param)], pp_stmt, i + 1, param)

                    for param_name, param_value in statement_named_params:
                        index = libsqlite3.sqlite3_bind_parameter_index(pp_stmt, param_name.encode('utf-8'))
                        run_with_db(db, bind[type(param_value)], pp_stmt, index, param_value)

                    columns = tuple(
                        libsqlite3.sqlite3_column_name(pp_stmt, i).decode()
                        for i in range(0, libsqlite3.sqlite3_column_count(pp_stmt))
                    )

                    yield columns, rows(get_pp_stmt, columns)
                finally:
                    finalize_stmt()

    with \
            get_http_client() as http_client, \
            get_vfs(http_client) as vfs:

        yield partial(query, vfs)


@contextmanager
def sqlite_s3_query(url, get_credentials=lambda now: (
    os.environ['AWS_REGION'],
    os.environ['AWS_ACCESS_KEY_ID'],
    os.environ['AWS_SECRET_ACCESS_KEY'],
    os.environ.get('AWS_SESSION_TOKEN'),  # Only needed for temporary credentials
), get_http_client=lambda: httpx.Client(),
   get_libsqlite3=lambda: cdll.LoadLibrary(find_library('sqlite3'))):

    @contextmanager
    def query(query_base, sql, params=(), named_params=()):
        for columns, rows in query_base(sql, (params,), (named_params,)):
            yield columns, rows
            break

    with sqlite_s3_query_multi(url,
            get_credentials=get_credentials,
            get_http_client=get_http_client,
            get_libsqlite3=get_libsqlite3,
    ) as query_base:

        yield partial(query, query_base)
