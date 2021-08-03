#include "hdf5.h"

#include <stdlib.h>
#include <stdio.h>

static herr_t visit_cb(hid_t obj, const char *name, const H5O_info2_t *info,
                       void *op_data);

int main(int argc, char **argv)
{
  int retval = EXIT_SUCCESS;
  hid_t file;
  char path[] = {"/"};

  if (argc < 2) {
    printf("HDF5 file name required!");
    return EXIT_FAILURE;
  }

  if ((file = H5Fopen(argv[1], H5F_ACC_RDONLY, H5P_DEFAULT)) ==
      H5I_INVALID_HID) {
    retval = EXIT_FAILURE;
    goto fail_file;
  }

  if (H5Ovisit(file, H5_INDEX_NAME , H5_ITER_NATIVE , &visit_cb, path,
               H5O_INFO_BASIC) < 0) {
    retval = EXIT_FAILURE;
    goto fail_visit;
  }

fail_visit:
  H5Fclose(file);
fail_file:
  return retval;
}

static int chunk_cb(const hsize_t *offset, uint32_t filter_mask, haddr_t addr,
                    uint32_t nbytes, void *op_data);

herr_t visit_cb(hid_t obj, const char *name, const H5O_info2_t *info,
                void *op_data)
{
  herr_t retval = 0;
  char* base_path = (char*) op_data;

  if (info->type == H5O_TYPE_DATASET)  // current object is a dataset
    {
      hid_t dset, dcpl;
      if ((dset = H5Dopen(obj, name, H5P_DEFAULT)) == H5I_INVALID_HID) {
        retval = -1;
        goto func_leave;
      }
      if ((dcpl = H5Dget_create_plist(dset)) == H5I_INVALID_HID) {
        retval = -1;
        goto fail_dcpl;
      }
      if (H5Pget_layout(dcpl) == H5D_CHUNKED) // dataset is chunked
        {
          __label__ fail_dtype, fail_dspace, fail_fig;
          hid_t dspace, dtype;
          size_t size, i;
          int rank;
          hsize_t cdims[H5S_MAX_RANK];

          // get resources
          if ((dtype = H5Dget_type(dset)) < 0) {
            retval = -1;
            goto fail_dtype;
          }
          if ((dspace = H5Dget_space(dset)) < 0) {
            retval = -1;
            goto fail_dspace;
          }
          // get the figures
          if ((size = H5Tget_size(dtype)) == 0 ||
              (rank = H5Sget_simple_extent_ndims(dspace)) < 0 ||
              H5Pget_chunk(dcpl, H5S_MAX_RANK, cdims) < 0) {
            retval = -1;
            goto fail_fig;
          }
          // calculate the nominal chunk size
          size = 1;
          for (i = 0; i < (size_t) rank; ++i)
            size *= cdims[i];
          // print dataset info
          printf("%s%s : nominal chunk size %lu [B] \n", base_path, name,
                 size);
          // get the allocated chunk sizes
          if (H5Dchunk_iter(dset, H5P_DEFAULT, &chunk_cb, NULL) < 0) {
              retval = -1;
              goto fail_fig;
            }

        fail_fig:
          H5Sclose(dspace);
        fail_dspace:
          H5Tclose(dtype);
        fail_dtype:;
        }

      H5Pclose(dcpl);
    fail_dcpl:
      H5Dclose(dset);
    }

func_leave:
  return retval;
}

int chunk_cb(const hsize_t *offset, uint32_t filter_mask, haddr_t addr,
             uint32_t nbytes, void *op_data)
{
  // for now we care only about the allocated chunk size
  printf("%d\n", nbytes);
  return EXIT_SUCCESS;
}
