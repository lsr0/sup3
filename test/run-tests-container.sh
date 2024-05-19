#!/bin/bash

set -e

access_key=AKEXAMPLES3S
secret_key=SKEXAMPLES3S
container_runtime=${CONTAINER_RUNTIME:-podman}

echo "sup3-test: running container"
container_id=$($container_runtime run -d \
   -p 9000:9000 \
   -e "MINIO_ROOT_USER=$access_key" \
   -e "MINIO_ROOT_PASSWORD=$secret_key" \
   quay.io/minio/minio server /data --console-address ":9090")

function cleanup
{
   echo "sup3-test: killing container"
   $container_runtime kill $container_id
}
trap cleanup EXIT


export AWS_ACCESS_KEY_ID=$access_key
export AWS_SECRET_ACCESS_KEY=$secret_key
export AWS_ENDPOINT_URL=http://127.0.0.1:9000

test/run-tests.sh $1
