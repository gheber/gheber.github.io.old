#include "hermes.h"
#include "bucket.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

#include <mpi.h>

namespace hapi = hermes::api;

using namespace std::chrono;


void hermes_app_main(int* argcp, char*** argvp,
                     std::shared_ptr<hapi::Hermes> const& hermes)
{
  
  if (*argcp != 5)
    exit(-1);
  
  size_t count = (size_t) atol((*argvp)[1]);
  assert(count >= 1);
  size_t size_mib = (size_t) atol((*argvp)[2]);
  assert(size_mib >= 1);
  int strategy = atoi((*argvp)[3]);
  assert(strategy >= 0 && strategy < 3);
  
  int app_rank = hermes->GetProcessRank();
  int app_size = hermes->GetNumProcesses();
  
  size_t my_count = (count - count%app_size) / app_size;
  
  
  hapi::Context ctx;
  
  switch (strategy)
    {
    case 0:
      ctx.policy = hapi::PlacementPolicy::kRandom;
      break;
    case 1:
      ctx.policy = hapi::PlacementPolicy::kRoundRobin;
      break;
    case 2:
      ctx.policy = hapi::PlacementPolicy::kMinimizeIoTime;
      break;
    default:
      break;
    }
  
  hapi::Bucket shared_bucket(std::string("Share the bucket!"), hermes, ctx);
  
  std::vector<uint8_t> MiB(size_mib*1024*1024);
  uint8_t* data = &MiB[0];
  
  
  auto sleep_duration = std::chrono::microseconds(150000);
  hermes->AppBarrier();
  auto start = high_resolution_clock::now();
  
  
  for (size_t i = app_rank; i < count; i += app_size)
    {
      std::string blob_name = ".hermes/" + std::to_string(i);
      if (shared_bucket.Put(blob_name, data, MiB.size(), ctx).Failed())
        throw("What the Bucket::Put?");
      // HACK: Give the system a chance to update its view state!
      std::this_thread::sleep_for(sleep_duration);
    }
  
  
  auto elapsed = high_resolution_clock::now() - start;
  uint64_t us = duration_cast<microseconds>(elapsed).count() -
    my_count * sleep_duration.count();
  std::cout << "Bucket->Put [us] : " << us << " : ~"
            << 1024 * 1024 * size_mib * my_count / us << " MB/s\n";
  
  hermes->AppBarrier();
  start = high_resolution_clock::now();
  
  
  for (size_t i = app_rank; i < count; i += app_size)
    {
      std::string blob_name = ".hermes/" + std::to_string(i);
      if (shared_bucket.Get(blob_name, MiB, ctx) != MiB.size())
        throw("What the Bucket::Get?");
    }
  
  
  hermes->AppBarrier();
  
  elapsed = high_resolution_clock::now() - start;
  us = duration_cast<microseconds>(elapsed).count();
  
  std::cout << "Bucket->Get [us] : " << us << " : ~"
            << 1024 * 1024 * size_mib * my_count / us << " MB/s\n";
  
  shared_bucket.Close(ctx);
  
}

int main(int argc, char **argv)
{
  
  try
    {
      int mpi_threads_provided;
      MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE,
                      &mpi_threads_provided);
      if (mpi_threads_provided < MPI_THREAD_MULTIPLE)
        {
          std::cerr << "Didn't receive appropriate MPI threading spec.\n";
          return 1;
        }
  
      char *config_file = 0;
      if (argc == 5)
        config_file = argv[argc-1];
      else
        {
          std::cerr << "demo <blob_count> <blob_size_mib> "
                    << "<placement_strategy> <hermes_config> !\n";
          exit(-1);
        }
  
  
  std::shared_ptr<hapi::Hermes> hermes = hapi::InitHermes(config_file);
  
  if (hermes->IsApplicationCore())
    {
      hermes_app_main(&argc, &argv, hermes);
  
      int app_size = hermes->GetNumProcesses();
      auto start = high_resolution_clock::now();
  
      hermes->Finalize();
  
      auto elapsed = high_resolution_clock::now() - start;
      uint64_t us = duration_cast<microseconds>(elapsed).count();
  
      std::cout << "hermes->Finalize [us] : " << us << " : ~"
                << (size_t) (atol(argv[1])*atol(argv[2])*1024*1024 /
                             (app_size * us)) << " MB/s\n";
    }
  else
    hermes->Finalize();
  
  
      MPI_Finalize();
    }
  catch (const std::exception& e)
    {
      std::cout << " a standard exception was caught, with message '"
                << e.what() << "'\n";
    }
  
  return 0;
  
}
