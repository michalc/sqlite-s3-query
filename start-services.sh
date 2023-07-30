#!/bin/sh

set -e

docker run --rm -p 9000:9000 --name sqlite-s3-query-minio -d \
  -e 'MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE' \
  -e 'MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' \
  -e 'MINIO_REGION=us-east-1' \
  --entrypoint sh \
  minio/minio:RELEASE.2023-07-21T21-12-44Z \
  -c '
    mkdir -p /data
    minio server /data
  '
