#include <stdio.h>
#include <stdlib.h>

static long long counter = 0;
int tarai(int x, int y, int z) {
  counter++;
  if (x <= y){
    return y;
  }
  return tarai(
    tarai(x-1, y, z),
    tarai(y-1, z, x),
    tarai(z-1, x, y)
  );
}

int main(int argc, char *argv[]) {
  if (argc != 4) {
    fprintf(stderr, "Usage: %s x y z\n", argv[0]);
    return 1;
  }
  int x = atoi(argv[1]);
  int y = atoi(argv[2]);
  int z = atoi(argv[3]);
  tarai(x, y, z);
  printf("%lld\n", counter);
  return 0;
}
