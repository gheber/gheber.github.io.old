#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "mpi.h"

using namespace std::chrono;

int main(int argc, char** argv)
{
  // HACK: This is an artifact of how we are currently deploying Hermes.
  MPI_Init(&argc, &argv);

  FILE* fp;
  assert((fp = fopen("foo.bytes", "w+")) != NULL);

  size_t size = 4096;
  if (argc == 2)
    size = (size_t) atol(argv[1]);

  unsigned char* buf = (unsigned char*) malloc(size);
  assert(buf != NULL);

  auto start = high_resolution_clock::now();

  assert(fwrite(buf, sizeof(unsigned char), size, fp) == size);

  auto elapsed = high_resolution_clock::now() - start;
  uint64_t us1 = duration_cast<microseconds>(elapsed).count();

  assert(fclose(fp) == 0);
  elapsed = high_resolution_clock::now() - start;
  uint64_t us2 = duration_cast<microseconds>(elapsed).count();

  std::cout << "fwrite [us] : " << us1 << " : ~" << size / us1 << " MB/s\n";
  std::cout << "fwrite+fclose [us] : " << us2 << " : ~" << size / us2
            << " MB/s\n";

  free(buf);

  // HACK: Again...
  MPI_Finalize();

  return 0;
}
