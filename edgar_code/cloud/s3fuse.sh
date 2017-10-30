#!/bin/bash

set -e

bucket=$(yq -r '.aws.cache.bucket' ~/Box\ Sync/config.yaml > ~/.passwd-s3fs)

yq -r '.aws.cache.bucket + ":" + .aws.cache.access_key_id + ":" + .aws.cache.secret_access_key' ~/Box\ Sync/config.yaml > ~/.passwd-s3fs

chmod 600 ~/.passwd-s3fs

mkdir -p /tmp/bucket
mkdir -p /tmp/cache

s3fs $bucket /tmp/bucket -o passwd_file=/tmp/s3_password -o allow_other -o complement_stat -o use_cache=/tmp/cache
