#!/bin/sh
# Nạp credential S3A từ file secret vào env rồi exec lệnh; thiếu secret thì fail.
set -eu
AWS_ACCESS_KEY_ID=$(cat /run/secrets/minio_user)
AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/minio_password)
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
exec "$@"
