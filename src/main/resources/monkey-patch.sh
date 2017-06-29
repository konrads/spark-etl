#!/usr/bin/env bash

usage() {
  echo "  Usage:"
  echo "    <jar> <file_in_jar>"
  exit 1
}

if [[ $# -lt 2 ]]; then usage;  fi
jar=$1
file=$2

rm -rf jar_exploded
mkdir jar_exploded
pushd jar_exploded
unzip -q ../$jar
vi $file
zip -ur ../$jar $file
popd
rm -rf jar_exploded