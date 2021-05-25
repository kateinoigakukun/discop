#!/bin/bash
set -eu

NATIVE_IMAGE=${NATIVE_IMAGE:-native-image}

project_dir=$(cd $(dirname $0)/.. && pwd)
build_dir="$project_dir/build"
mkdir -p "$build_dir"

"$project_dir/gradlew" build shadowJar
"$NATIVE_IMAGE" --no-server --no-fallback -jar "$project_dir/discop-cli/build/libs/discop-cli-all.jar" --target "$build_dir/discop-cli"
"$NATIVE_IMAGE" --no-server --no-fallback -jar "$project_dir/discop-scheduler/build/libs/discop-scheduler-all.jar" --target "$build_dir/discop-scheduler"

