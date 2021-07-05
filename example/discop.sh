#!/bin/bash
set -xe
N=50

example_dir=$(cd "$(dirname "$0")" && pwd)
main="$example_dir/main.wasm"
make_input="$example_dir/make_input.swift"
work_dir="$(mktemp -d)"
input_file="$work_dir/input.json"

program_id=$(curl --silent http://localhost:8080/upload_program --data-binary @"$main" | jq .program_id)

$make_input "$program_id" $N > "$input_file"

curl http://localhost:8080/run_job --data @"$input_file" > /dev/null
