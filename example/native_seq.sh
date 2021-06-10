#!/bin/bash
set -e
CC="${CC:-clang}"
N=100

example_dir=$(cd "$(dirname "$0")" && pwd)
main_c="$example_dir/main.c"
work_dir="$(mktemp -d)"
main="$work_dir/main"

$CC "$main_c" -o "$main"
printf '0    10   20   30   40   50   60   70   80   90   100\n'
printf '|----|----|----|----|----|----|----|----|----|----|\n'
printf .

for i in $(seq 1 $N); do
  if [ $(($i * 100/$N % 2)) -eq 0 ]; then
    printf .
  fi
  "$main" 14 13 0 > /dev/null
done
printf "\n"
