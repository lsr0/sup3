#!/bin/bash

sup3=${1:-sup3}

set -e

d=$(mktemp -d /tmp/bucketXXXX)
bucket_name=$(basename "$d")

mkdir $d/dir
echo 1 > $d/dir/a
echo 1 > $d/dir/bee
echo 1 > $d/dir/tertiary

bucket=s3://test

$sup3 mb $bucket
$sup3 cp -r $d/* $bucket

res_unix=$(cd $d && find dir | sort)
res_sup3=$($sup3 ls -r $bucket | sort)

if [ "$res_unix" != "$res_sup3" ];then
   echo "Output mismatch:" >&2
   diff <(echo "$res_unix") <(echo "$res_sup3") --color --unified
   exit 1
fi
echo "sup3-test: upload and ls test: success"

down_dir=$(mktemp -d)
$sup3 cp -r $bucket $down_dir
echo "sup3-test: $d ~= $down_dir"
find $d/
find $down_dir/
diff -urp $d $down_dir

down_dir2=$(mktemp -d)
$sup3 cp -r $bucket/dir $down_dir2/
echo "sup3-test: $d ~= $down_dir2"
find $d/
find $down_dir2/
diff -urp $d $down_dir2

echo "sup3-test: success"
