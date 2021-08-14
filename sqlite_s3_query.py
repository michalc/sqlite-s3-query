from functools import partial
from hashlib import sha256
import hmac
from datetime import datetime
import os
from urllib.parse import urlencode, urlsplit, quote
from uuid import uuid4

import apsw
import httpx


def sqlite_s3_query(sql, url, params=(), get_credentials=lambda: (
    os.environ['AWS_DEFAULT_REGION'],
    os.environ['AWS_ACCESS_KEY_ID'],
    os.environ['AWS_SECRET_ACCESS_KEY'],
    os.environ.get('AWS_SESSION_TOKEN'),  # Only needed for temporary credentials
)):
    vfs_name = 's3-' + str(uuid4())
    file_name = 's3-' + str(uuid4())
    body_hash = sha256(b'').hexdigest()
    scheme, netloc, path, _, _ = urlsplit(url)

    class S3VFS(apsw.VFS):
        def __init__(self, size, get_range):
            self.size = size
            self.get_range = get_range
            super().__init__(vfs_name)

        def xOpen(self, _, __):
            return S3VFSFile(size, get_range)

        def xFullPathname(self, p):
            return p

    class S3VFSFile():
        def __init__(self, size, get_range):
            self.size = size
            self.get_range = get_range

        def xRead(self, amount, offset):
            return get_range(offset, offset + amount)

        def xFileSize(self):
            return self.size

        def xClose(self):
            pass

        def xFileControl(self, _, __):
            return False

    def make_auth_request(http_client, method, params, headers):
        region, access_key_id, secret_access_key, session_token = get_credentials()
        to_auth_headers = headers + (
            (('x-amz-security-token', session_token),) if session_token is not None else \
            ()
        )
        request_headers = aws_sigv4_headers(
            access_key_id, secret_access_key, region, 'GET', to_auth_headers, params,
        )
        url = f'{scheme}://{netloc}{path}?{urlencode(params)}'
        response = http_client.get(url, headers=request_headers)
        response.raise_for_status()
        return response

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

    with httpx.Client() as http_client:
        head_headers = make_auth_request(http_client, 'HEAD', (), ()).headers
        version_id = head_headers['x-amz-version-id']
        size = int(head_headers['content-length'])
        get_range = lambda bytes_from, bytes_to: \
            make_auth_request(http_client, 'HEAD',
                (('versionId', version_id),),
                (('range', f'bytes={bytes_from}-{bytes_to}'),)
            ).content
        vfs = S3VFS(size, get_range)

        with apsw.Connection(f'file:/{file_name}?immutable=1',
            flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI,
            vfs=vfs_name,
        ) as conn:
            results = conn.cursor().execute(sql, params)
            yield from results

        del vfs
