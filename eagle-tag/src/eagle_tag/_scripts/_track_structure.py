# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import errno
import os
import socket
import sys
from typing import Callable

import dask
from dask import delayed, compute
from dask.distributed import LocalCluster
import xarray as xr
from xarray.core import dtypes as xr_dtypes
import dask.array as dask_array
from dask.utils import SerializableLock
import numpy as np
import h5py as h5
from QuasarCode import Console, Settings, Stopwatch
from QuasarCode.IO.Configurations import ConfigsBase, YamlConfig

from eagle_tag import EAGLE_Files, EAGLE_Snapshot, SnapshotTag, load_catalogue, cache_catalogue_as_single_file, clear_catalogue_cache, load_catalogue_cache, check_cache_exists, load_hdf5_files_with_xarray, make_aux_file_path
from ._calculate_reorder import make_file_path as make_reorder_file_path



NULL_INDEX = 2**30 # Used where an integer index needs to be NULL

def is_integer_type(datatype) -> bool:
    return datatype in (int, np.int8, np.int16, np.int32, np.int64, np.byte, np.short, np.intc, np.intp, np.int_, np.longlong, np.uint8, np.uint16, np.uint32, np.uint64, np.ubyte, np.ushort, np.uintc, np.uintp, np.uint, np.ulonglong)



def load_config_file(filepath: str) -> ConfigsBase:
    return YamlConfig.from_file(filepath)

def create_config_file(filepath: str) -> None:
    content = """\
# EAGLE-tag Structure Tracking Configuration

# Data Sources

eagle_data_directory:                  "./"
halo_membership_by_particle_directory: "./"
reorder_indexes_directory:             "./"
target_tags:                           "./targets.txt" # This should be a list of tags in the format "012_z345p678<newline>" - one per line
keep_catalogue_caches:                 false
keep_data_propagation_caches:          false

# Control Options

do_gas:         true
do_dark_matter: false
do_stars:       true
do_black_holes: true
start_tag:      "000_z020p000"
end_tag:        "028_z000p000"
skip_tags:      []
snipshots:      false

# Dask Options

dask_workers:                    null # Number of parallel workers to use, 0 will avoid instantiation of a dask cluster
dask_memory_per_worker:          null # in GB
dask_port:                       8787 # Port number or null to disable the dask dashboard
number_of_reads_per_large_array: 10
max_size_to_write:               null # Value in Gigabytes

# Tracked Quantities

# The following fields are automatically added:
#     ParticleIDs         <- From membership file
#     GroupNumber         <- From membership file
#     FirstSubhaloID
#     SubGroupNumber      <- From membership file
#     SubhaloID           <- FirstSubhaloID + SubGroupNumber
#     LastGroupRedshift
#     LastSubhaloRedshift
#     HaloMass            <- GroupMass
#     HaloM200Crit        <- Group_M_Crit200

group: # Values from the FOF table
    HaloM500Crit:      "Group_M_Crit500"
    NumberOfSubhaloes: "NumOfSubhalos"

central: # Values from the Subhalo table for the first subhalo of the group
    CentralMass_Total:                  "Mass"
    CentralMass_ByType:                 "MassType"
    CentralMassAtHalfMassRadius_ByType: "MassTwiceHalfMassRad"
    CentralHalfMassRadius_ByType:       "HalfMassRad"
    CentralMassAt30kpc_ByType:          "ApertureMeasurements/Mass/030kpc"

subhalo: # Values from the Subhalo table
    SubhaloMass_Total:                  "Mass"
    SubhaloMass_ByType:                 "MassType"
    SubhaloMassAtHalfMassRadius_ByType: "MassTwiceHalfMassRad"
    SubhaloHalfMassRadius_ByType:       "HalfMassRad"
    SubhaloMassAt30kpc_ByType:          "ApertureMeasurements/Mass/030kpc"
"""
    with open(filepath, "w") as file:
        file.write(content)



def make_file_path(directory: str, tag: SnapshotTag) -> str:
    return os.path.join(directory, f"tracked_particle_structure_properties_{tag.tag}.hdf5")

def make_output_file(
    filepath: str,
    allow_overwrite: bool,
    allow_update: bool,
    redshift: float,
    redshift_of_all_files: tuple[float, ...],
    tags_of_all_files: tuple[SnapshotTag, ...],
    number_of_gas_particles: int|None,
    number_of_dark_matter_particles: int|None,
    number_of_star_particles: int|None,
    number_of_black_hole_particles: int|None,
    field_widths: dict[str, int],
    field_datatypes: dict[str, object],
    pre_initialise_with_null: bool = False
) -> None:

    file_exists = os.path.exists(filepath)
    if not allow_update and not allow_overwrite and file_exists:
        raise FileExistsError(errno.EEXIST, f"File already exists: {filepath}")
    if allow_overwrite:
        file_exists = False # If the file will be overwritten, we can just pretend it doesn't actually exist

    with h5.File(filepath, "a" if file_exists else "w", rdcc_nbytes = 1024**3) as file: # rdcc_nbytes = 1024**2 is default (1MB)

        if not file_exists:

            Console.print_debug("Making Header group.")

            header = file.create_group("Header")
            header.attrs["Redshift"] = redshift

            header.create_dataset("FileNumbers",   data = [tag.number for tag in tags_of_all_files])
            header.create_dataset("FileTags",      data = [str(tag)   for tag in tags_of_all_files])
            header.create_dataset("FileRedshifts", data = redshift_of_all_files, dtype = np.float64)

        def create_particle_datasets(group: h5.Group, group_length: int) -> None:

            group.attrs["NumberOfParticles"] = group_length
            group.attrs["NumberInGroups"] = 0
            group.attrs["NumberInSubhaloes"] = 0

            #for field in field_widths:
            #    Console.print_debug(f"Making dataset {field} ({(group_length, field_widths[field])}, {field_datatypes[field]}).")
            #    d = group.create_dataset(
            #        field,
            #        shape = (group_length, field_widths[field]) if field_widths[field] > 1 else (group_length,), dtype = field_datatypes[field],
            #        fillvalue = None if not pre_initialise_with_null else np.nan if issubclass(field_datatypes[field], float) else NULL_INDEX,
            #        chunks = ((1024 * 8, field_widths[field]) if field_widths[field] > 1 else 1024 * 8) if group_length > 1024 * 8 else True, # alternatively, use: True -> Auto
            #        compression = "gzip",
            #        compression_opts = 8,
            #        shuffle = True,
            #        fletcher32 = True
            #    )

        if number_of_gas_particles is not None:
            if file_exists and "PartType0" in file:
                raise KeyError("Gas group already exists in the output file but is set to be updated. First remove groups that will be replaced, or overwrite the while file by specifying --overwrite.")
            Console.print_debug("Making gas group.")
            gas = file.create_group("PartType0")
            create_particle_datasets(gas, number_of_gas_particles)

        if number_of_dark_matter_particles is not None:
            if file_exists and "PartType1" in file:
                raise KeyError("Dark matter group already exists in the output file but is set to be updated. First remove groups that will be replaced, or overwrite the while file by specifying --overwrite.")
            Console.print_debug("Making dark matter group.")
            dark_matter = file.create_group("PartType1")
            create_particle_datasets(dark_matter, number_of_dark_matter_particles)

        if number_of_star_particles is not None:
            if file_exists and "PartType4" in file:
                raise KeyError("Star group already exists in the output file but is set to be updated. First remove groups that will be replaced, or overwrite the while file by specifying --overwrite.")
            Console.print_debug("Making star group.")
            stars = file.create_group("PartType4")
            create_particle_datasets(stars, number_of_star_particles)

        if number_of_black_hole_particles is not None:
            if file_exists and "PartType5" in file:
                raise KeyError("Black hole group already exists in the output file but is set to be updated. First remove groups that will be replaced, or overwrite the while file by specifying --overwrite.")
            Console.print_debug("Making black hole group.")
            black_holes = file.create_group("PartType5")
            create_particle_datasets(black_holes, number_of_black_hole_particles)

def load_output_file(filepath: str, *datasets: str) -> dict[str, xr.Dataset|None]:
    with h5.File(filepath, "r") as file:
        has_gas = "PartType0" in file
        has_dark_matter = "PartType1" in file
        has_stars = "PartType4" in file
        has_black_holes = "PartType5" in file
    return {
        "PartType0" : load_hdf5_files_with_xarray([filepath], "PartType0", datasets, dimension_sizes = { "snapshot_particle_index" : None, "box_axis_index" : 3, "particle_type_number" : 6 }) if has_gas         else None,
        "PartType1" : load_hdf5_files_with_xarray([filepath], "PartType1", datasets, dimension_sizes = { "snapshot_particle_index" : None, "box_axis_index" : 3, "particle_type_number" : 6 }) if has_dark_matter else None,
        "PartType4" : load_hdf5_files_with_xarray([filepath], "PartType4", datasets, dimension_sizes = { "snapshot_particle_index" : None, "box_axis_index" : 3, "particle_type_number" : 6 }) if has_stars       else None,
        "PartType5" : load_hdf5_files_with_xarray([filepath], "PartType5", datasets, dimension_sizes = { "snapshot_particle_index" : None, "box_axis_index" : 3, "particle_type_number" : 6 }) if has_black_holes else None,
    }

def write_to_output_field__old(filepath: str, particle_type: str, field: str, data: np.ndarray|int|float, mask: slice|np.ndarray[tuple[int], np.dtype[np.bool_]] = slice(None), memory_chunk_bits: int|None = None) -> None:

    with h5.File(filepath, "a") as file:

        if isinstance(data, np.ndarray) and len(data.shape) > 2:
            raise NotImplementedError(f"write_to_output_field only supports data arrays with 2 or fewer dimensions (or scalar data). The data provided had {len(data.shape)} dimensions.")
        if field not in file[particle_type]:
            raise KeyError(f"Field \"{field}\" does not exist in the output file \"{filepath}\".")
        if len(file[particle_type][field].shape) > 2:
            raise NotImplementedError(f"write_to_output_field only supports HDF5 fields with 2 or fewer dimensions. \"{particle_type}/{field}\" has {len(file[particle_type][field].shape)}")
        
        if isinstance(mask, slice):
            # scalar, 1D or 2D data and slice

            if memory_chunk_bits is None:

                if not isinstance(data, np.ndarray) or len(file[particle_type][field].shape) <= 1:
                    # No need to check for 1D or scalar data
                    # They can be handled the same way when not chunking
                    file[particle_type][field][mask] = data

                else:
                    # 2D data and slice
                    file[particle_type][field][mask, :] = data

            else:
                # Chunking is needed, so it matters if the data is a scalar value (that can't be indexed)

                start, final_stop_index, step = mask.indices(file[particle_type][field].shape[0])

                if not isinstance(data, np.ndarray):
                    # scalar data

                    chunk_length = int(memory_chunk_bits / 64) # assuming 64-bit data for scalar data
                    stop = chunk_length

                    while stop < final_stop_index:
                        true_stop = min(stop, final_stop_index)
                        chunk_indexes = np.arange(start = start, stop = true_stop, step = step, dtype = np.int64)

                        if len(file[particle_type][field].shape) == 1:
                            file[particle_type][field][chunk_indexes] = data
                        elif len(file[particle_type][field].shape) == 2:
                            file[particle_type][field][chunk_indexes, :] = data

                        start = stop
                        stop += chunk_length

                else:
                    # 1D or 2D data

                    chunk_length = int(memory_chunk_bits / data.dtype.itemsize)
                    stop = chunk_length

                    while stop < final_stop_index:
                        true_stop = min(stop, final_stop_index)
                        chunk_indexes = np.arange(start = start, stop = true_stop, step = step, dtype = np.int64)

                        if len(file[particle_type][field].shape) == 1:
                            file[particle_type][field][chunk_indexes] = data[start:true_stop]
                        elif len(file[particle_type][field].shape) == 2:
                            file[particle_type][field][chunk_indexes, :] = data[start:true_stop, :]

                        start = stop
                        stop += chunk_length

        else:
            # scalar, 1D or 2D data and 1D mask
            # The mask needs to be converted to target indexes

            if memory_chunk_bits is None:

                if not isinstance(data, np.ndarray) or len(file[particle_type][field].shape) <= 1:
                    # No need to check for 1D or scalar data
                    # They can be handled the same way when not chunking
                    file[particle_type][field][np.where(mask)[0]] = data

                else:
                    # 2D data and 1D mask
                    file[particle_type][field][np.where(mask)[0], :] = data

            else:
                # Chunking is needed, so it matters if the data is a scalar value (that can't be indexed)

                start = 0

                if not isinstance(data, np.ndarray) or len(data.shape) == 0:
                    # scalar data

                    chunk_length = int(memory_chunk_bits / 64) # assuming 64-bit data for scalar data
                    stop = chunk_length

                    while stop < mask.shape[0]:
                        true_stop = min(stop, mask.shape[0])
                        chunk_indexes = np.where(mask[start:true_stop])[0]

                        if len(file[particle_type][field].shape) == 1:
                            file[particle_type][field][chunk_indexes] = data
                        elif len(file[particle_type][field].shape) == 2:
                            file[particle_type][field][chunk_indexes, :] = data

                        start = stop
                        stop += chunk_length

                else:
                    # 1D or 2D data

                    chunk_length = int(memory_chunk_bits / data.dtype.itemsize)
                    stop = chunk_length

                    while stop < mask.shape[0]:
                        true_stop = min(stop, mask.shape[0])
                        chunk_indexes = np.where(mask[start:true_stop])[0]

                        if len(file[particle_type][field].shape) == 1:
                            file[particle_type][field][chunk_indexes] = data[start:true_stop]
                        elif len(file[particle_type][field].shape) == 2:
                            file[particle_type][field][chunk_indexes, :] = data[start:true_stop, :]

                        start = stop
                        stop += chunk_length

def xarray_write_to_output_field(filepath: str, particle_type: str, field: str, data: xr.DataArray) -> None:

    data.name = field

    encoding = {
        field: {
            "zlib": True,
            "complevel": 8,
            "fletcher32": True
        }
    }

    data.to_netcdf(
        filepath,
        mode = "a",
#        engine = "h5netcdf",
        engine = "netcdf4",
        group = particle_type,
        encoding = encoding,
#        invalid_netcdf = True
    )

def write_to_output_field(filepath: str, particle_type: str, field: str, data: np.ndarray|int|float, mask: slice|np.ndarray[tuple[int], np.dtype[np.bool_]] = slice(None), memory_chunk_bits: int|None = None) -> None:

    with h5.File(filepath, "a") as file:

        if isinstance(data, np.ndarray) and len(data.shape) > 2:
            raise NotImplementedError(f"write_to_output_field only supports data arrays with 2 or fewer dimensions (or scalar data). The data provided had {len(data.shape)} dimensions.")
        if field not in file[particle_type]:
            raise KeyError(f"Field \"{field}\" does not exist in the output file \"{filepath}\".")
        if len(file[particle_type][field].shape) > 2:
            raise NotImplementedError(f"write_to_output_field only supports HDF5 fields with 2 or fewer dimensions. \"{particle_type}/{field}\" has {len(file[particle_type][field].shape)}")
        
        if isinstance(mask, slice):
            # scalar, 1D or 2D data and slice

            if memory_chunk_bits is None:

                if not isinstance(data, np.ndarray) or len(file[particle_type][field].shape) <= 1:
                    # No need to check for 1D or scalar data
                    # They can be handled the same way when not chunking
                    file[particle_type][field][mask] = data

                else:
                    # 2D data and slice
                    file[particle_type][field][mask, :] = data

            else:
                # Chunking is needed, so it matters if the data is a scalar value (that can't be indexed)

                start, final_stop_index, step = mask.indices(file[particle_type][field].shape[0])

                if not isinstance(data, np.ndarray):
                    # scalar data

                    chunk_length = int(memory_chunk_bits / 64) # assuming 64-bit data for scalar data
                    stop = chunk_length

                    while stop < final_stop_index:
                        true_stop = min(stop, final_stop_index)

                        if len(file[particle_type][field].shape) == 1:
                            file[particle_type][field][start:stop] = data
                        elif len(file[particle_type][field].shape) == 2:
                            file[particle_type][field][start:stop, :] = data

                        start = stop
                        stop += chunk_length

                else:
                    # 1D or 2D data

                    chunk_length = int(memory_chunk_bits / data.dtype.itemsize)
                    stop = chunk_length
                    initial_start = start

                    while stop < final_stop_index:
                        true_stop = min(stop, final_stop_index)

                        if len(file[particle_type][field].shape) == 1:
                            file[particle_type][field][start:true_stop] = data[start - initial_start : true_stop - initial_start]
                        elif len(file[particle_type][field].shape) == 2:
                            file[particle_type][field][start:true_stop, :] = data[start - initial_start : true_stop - initial_start, :]

                        start = stop
                        stop += chunk_length

        else:
            # scalar, 1D or 2D data and 1D mask
            # The mask needs to be converted to target indexes

            if memory_chunk_bits is None:

                if not isinstance(data, np.ndarray) or len(file[particle_type][field].shape) <= 1:
                    # No need to check for 1D or scalar data
                    # They can be handled the same way when not chunking
                    all_data = file[particle_type][field][:]
                    all_data[mask] = data
                    file[particle_type][field][:] = all_data

                else:
                    # 2D data and 1D mask
                    all_data = file[particle_type][field][:, :]
                    all_data[mask, :] = data
                    file[particle_type][field][:, :] = all_data

            else:
                # Chunking is needed, so it matters if the data is a scalar value (that can't be indexed)

                start = 0

                if not isinstance(data, np.ndarray) or len(data.shape) == 0:
                    # scalar data

                    chunk_length = int(memory_chunk_bits / 64) # assuming 64-bit data for scalar data
                    stop = chunk_length

                    while stop < mask.shape[0]:
                        true_stop = min(stop, mask.shape[0])

                        if len(file[particle_type][field].shape) == 1:
                            all_data = file[particle_type][field][start:true_stop]
                            all_data[mask[start:true_stop]] = data
                            file[particle_type][field][start:true_stop] = all_data
                        elif len(file[particle_type][field].shape) == 2:
                            all_data = file[particle_type][field][start:true_stop, :]
                            all_data[mask[start:true_stop], :] = data
                            file[particle_type][field][start:true_stop, :] = all_data

                        start = stop
                        stop += chunk_length

                else:
                    # 1D or 2D data

                    chunk_length = int(memory_chunk_bits / data.dtype.itemsize)
                    stop = chunk_length
                    data_offset = 0

                    while stop < mask.shape[0]:
                        true_stop = min(stop, mask.shape[0])

                        amount_of_data = mask[start:true_stop].sum()

                        if amount_of_data == 0:
                            continue

                        if len(file[particle_type][field].shape) == 1:
                            all_data = file[particle_type][field][start:true_stop]
                            all_data[mask[start:true_stop]] = data[data_offset : data_offset + amount_of_data]
                            file[particle_type][field][start:true_stop] = all_data
                        elif len(file[particle_type][field].shape) == 2:
                            all_data = file[particle_type][field][start:true_stop, :]
                            all_data[mask[start:true_stop], :] = data[data_offset : data_offset + amount_of_data, :]
                            file[particle_type][field][start:true_stop, :] = all_data

                        start = stop
                        stop += chunk_length
                        data_offset += amount_of_data



def write_zarr_output_file(filepath: str, group: str|None, data: xr.Dataset) -> None:
    data.to_zarr(filepath, mode = "w", group = group)
def read_zarr_output_file(filepath: str, group: str|None = None) -> None:
    xr.open_zarr(filepath, engine = "zarr", group = group)


def main() -> None:
    print(
"""
--|| EAGLE-tag (track) ||--

Tags particles with the properties of the structure of which they were last a member.
""",
    flush = True)

    Console.show_times()
    Console.reset_stopwatch()

    #------------------------------|
    # Parse command line arguments |
    #------------------------------|
    Console.print_info("Parsing command line arguments.", flush = True)

    parser = argparse.ArgumentParser(description = "Run EAGLE halo membership tagging.")

    parser.add_argument("--settings",                  type = str, default = "settings.yaml", help = "File containing the settings for the run.")
#    parser.add_argument("--chunks",             "-c",  type = int, default = 1,               help = "Number of chunks to divide the snapshot data into.")
    parser.add_argument("--output-directory",   "-o",  type = str, default = ".",             help = "Directory in which to create the output file. Default is the current working directory.")
    parser.add_argument("--overwrite",                 action  = "store_true",                help = "Overwrite existing output files.")
    parser.add_argument("--update",                    action  = "store_true",                help = "Allow the use of an existing output file.")
    parser.add_argument("--new-settings-file",         action  = "store_true",                help = "Create a new settings file and exit.")
    parser.add_argument("--verbose",            "-v",  action  = "store_true",                help = "Display extra information.")
    parser.add_argument("--debug",              "-d",  action  = "store_true",                help = "Display extreme amounts of information.")

    # This will exit the program if -h or --help are specified
    args = parser.parse_args()

    if args.verbose:
        Settings.enable_verbose()
    if args.debug:
        Settings.enable_verbose()
        Settings.enable_debug()

    Console.print_verbose_info("Arguments:", flush = True)
    for key in args.__dict__:
        Console.print_verbose_info(f"    {key}: {getattr(args, key)}", flush = True)

    #------------------------------------------|
    # Check for requesting a new settings file |
    #------------------------------------------|

    if args.new_settings_file:
        Console.print_verbose_info("Creating new settings file.", flush = True)

        if os.path.exists(args.settings):
            Console.print_error(f"Settings file \"{args.settings}\" already exists. Please move/delete it before creating a new one.\nAlternatively, change the target file by specifying --settings", flush = True)
            sys.exit(1)

        Console.print_info(f"Creating new settings file \"{args.settings}\".", flush = True)
        create_config_file(args.settings)

        return

    #----------------------------------|
    # Check for a valid set of options |
    #----------------------------------|

    if not os.path.exists(args.output_directory):
        Console.print_error(f"Target output directory does not exist (\"{args.output_directory}\").", flush = True)
        sys.exit(1)

    # Make a tmp directory if one does not already exist
    # This is for storing intermediate caches
    if not os.path.exists(os.path.join(args.output_directory, "tmp")):
        os.makedirs(os.path.join(args.output_directory, "tmp"))

    if not os.path.exists(args.settings):
        Console.print_error(f"Unable to locate settings file at \"{args.settings}\". Create a new one using --new-settings-file", flush = True)
        sys.exit(1)
    
    if args.update and args.overwrite:
        Console.print_error("--update and --overwrite are mutually exclusive.", flush = True)
        sys.exit(1)

    #-----------------------------------|
    # Check for a valid set of settings |
    #-----------------------------------|

    settings = load_config_file(args.settings)

    if not os.path.exists(settings.eagle_data_directory):
        Console.print_error(f"Unable to locate EAGLE data at \"{settings.eagle_data_directory}\".", flush = True)
        sys.exit(1)

    if not os.path.exists(settings.halo_membership_by_particle_directory):
        Console.print_error(f"Unable to locate EAGLE-tag structure membership data at \"{settings.halo_membership_by_particle_directory}\".", flush = True)
        sys.exit(1)

    if not os.path.exists(settings.reorder_indexes_directory):
        Console.print_error(f"Unable to locate EAGLE-tag particle reordering data at \"{settings.reorder_indexes_directory}\".", flush = True)
        sys.exit(1)

    if not os.path.exists(settings.target_tags):
        Console.print_error(f"Unable to locate list of target tags at \"{settings.target_tags}\".", flush = True)
        sys.exit(1)

    if not (settings.do_gas or settings.do_dark_matter or settings.do_stars or settings.do_black_holes):
        Console.print_error("At least one particle type must be specified.", flush = True)
        sys.exit(1)

    if int(settings.start_tag.split("_")[0]) > int(settings.end_tag.split("_")[0]):
        Console.print_error("Start tag must be earlier than or equal to end tag.", flush = True)
        sys.exit(1)

    if settings.start_tag in settings.skip_tags:
        Console.print_error("Start tag must not be in the list of skipped tags.", flush = True)
        sys.exit(1)

    if settings.end_tag in settings.skip_tags:
        Console.print_error("End tag must not be in the list of skipped tags.", flush = True)
        sys.exit(1)

    if settings.dask_workers < 0:
        Console.print_error("Number of dask workers must be at least 0 (and ideally not 1).", flush = True)
        sys.exit(1)
    elif settings.dask_workers == 1:
        Console.print_warning("Number of dask workers is set to 1. This will work, but it is preferred to set the value to 0 and avoid using a dask cluster altogether.", flush = True)

    if settings.dask_workers > 0 and (settings.dask_memory_per_worker is None or settings.dask_memory_per_worker <= 0):
        Console.print_error(f"Invalid amount of memory when using dask cluster. The memory per worker must be a positive, nonzero value (current value was \"{settings.dask_memory_per_worker}\").", flush = True)
        sys.exit(1)

    if settings.dask_workers > 0 and settings.dask_port is not None and (settings.dask_port < 1 or settings.dask_port > 65535):
        Console.print_error(f"Invalid port for dask client. Provide either \"null\" or a value between 1 and 65535 (current value was \"{settings.dask_port}\").", flush = True)
        sys.exit(1)

    if settings.number_of_reads_per_large_array < 1:
        Console.print_error("Number of reads per large array must be at least 1.", flush = True)
        sys.exit(1)

    #---------------------------------|
    # Compile target catalogue fields |
    #---------------------------------|
    Console.print_verbose_info("Processing requested fields.")

    def parse_target_field(field_expression: str) -> tuple[str, tuple[int, ...]|None]:
        has_open_bracket = "[" in field_expression
        has_close_bracket = "]" in field_expression
        is_valid = (has_open_bracket and has_open_bracket) or (not has_open_bracket and not has_open_bracket)
        if not is_valid:
            raise ValueError(f"Name of target field (\"{field_expression}\") contained either \"[\" or \"]\" but not both.")

        if has_open_bracket:
            name, indexes = field_expression.replace("]", "").split("[", 1)
            if indexes.strip() == "":
                raise ValueError("Target field contained indexer expression, but no indexes.")
            return (name, tuple([int(index) for index in indexes.split(",")]))

        else:
            return (field_expression, None)

    fof_group_fields_default: dict[str, tuple[str, tuple[int, ...]|None]] = {
        "HaloMass"     : ("GroupMass",       None),
        "HaloM200Crit" : ("Group_M_Crit200", None),
    }
    fof_group_fields_user: dict[str, tuple[str, tuple[int, ...]|None]] = { field : parse_target_field(settings.group[field]) for field in settings.group.keys if field not in fof_group_fields_default } if settings.group is not None else {}
    fof_group_fields: dict[str, tuple[str, tuple[int, ...]|None]] = fof_group_fields_default | fof_group_fields_user
    fof_group_field_names_for_reading: tuple[str, ...] = tuple(list(set([v[0] for v in fof_group_fields.values()]) | {"NumOfSubhalos"}) + ["FirstSubhaloID"]) # FirstSubhaloID is needed to calculate the true index for subhaloes

    subgroup_fields_user_all: dict[str, tuple[str, tuple[int, ...]|None]] = { field : parse_target_field(settings.subhalo[field]) for field in settings.subhalo.keys } if settings.subhalo is not None else {}
    subgroup_fields_user_centrals: dict[str, tuple[str, tuple[int, ...]|None]] = { field : parse_target_field(settings.central[field]) for field in settings.central.keys } if settings.central is not None else {}
    subgroup_field_names_for_reading: tuple[str, ...] = tuple(set([v[0] for v in subgroup_fields_user_all.values()]) | set([v[0] for v in subgroup_fields_user_centrals.values()]))
    subgroups_with_nested_fields: tuple[bool, ...] = tuple(["/" in field_path.strip("/") for field_path in subgroup_field_names_for_reading])
    subgroup_fields_include_nested: bool = any(subgroups_with_nested_fields)
    if subgroup_fields_include_nested:
        subgroups_with_nested_fields__by_root: dict[str|None, dict[str, str]] = {}
        subgroups_with_nested_fields__by_root[None] = { field : field for field, is_nested in zip(subgroup_field_names_for_reading, subgroups_with_nested_fields) if not is_nested }
        nested_fields = [field for field, is_nested in zip(subgroup_field_names_for_reading, subgroups_with_nested_fields) if is_nested]
        for field_path in nested_fields:
            root, field = field_path.rsplit("/", 1)
            if root not in subgroups_with_nested_fields__by_root:
                subgroups_with_nested_fields__by_root[root] = {}
            subgroups_with_nested_fields__by_root[root][field] = field_path

    Console.print_verbose_info(f"Reading from FOF:\n{"\n".join(fof_group_field_names_for_reading)}")
    Console.print_verbose_info(f"Reading from Subhalo:\n{"\n".join(subgroup_field_names_for_reading)}")

    #--------------------------|
    # Read list of target tags |
    #--------------------------|
    Console.print_info("Loading target tags.", flush = True)

    available_tags: list[str]
    with open(settings.target_tags, "r") as file:
        available_tags = [line.strip() for line in file if line.strip()]

    if len(available_tags) == 0:
        Console.print_error("No target tags found in the target tags file.", flush = True)
        sys.exit(1)

    if settings.start_tag not in available_tags:
        Console.print_error(f"Start tag \"{settings.start_tag}\" not found in the target tags file.", flush = True)
        sys.exit(1)

    if settings.end_tag not in available_tags:
        Console.print_error(f"End tag \"{settings.end_tag}\" not found in the target tags file.", flush = True)
        sys.exit(1)

    #---------------------|
    # Narrow list of tags |
    #---------------------|

    target_tags = tuple([SnapshotTag.from_string(tag) for tag in available_tags if tag not in settings.skip_tags])
    target_tags_start_index = available_tags.index(settings.start_tag)
    target_tags_end_index   = available_tags.index(settings.end_tag)

    #---------------------------------|
    # Create directory and file paths |
    #---------------------------------|
    Console.print_info("Creating directory paths.", flush = True)

    eagle_files = EAGLE_Files(directory = settings.eagle_data_directory)
    def select_snapshot(tag: SnapshotTag) -> EAGLE_Snapshot:
        return eagle_files.snapshot(tag = tag, snipshot = settings.snipshots)

    target_snapshots_info = tuple([select_snapshot(tag) for tag in target_tags]) # Do all in order to get full list of redshifts

    #-------------------------|
    # Load snapshot redshifts |
    #-------------------------|
    Console.print_info("Loading snapshot redshifts.", flush = True)

    _snapshot_redshifts: list[float] = []
    for snapshot_info in target_snapshots_info:
        with h5.File(snapshot_info.snapshot_file_template.format(0), "r") as file:
            _snapshot_redshifts.append(float(file["Header"].attrs["Redshift"]))
    snapshot_redshifts: tuple[float, ...] = tuple(_snapshot_redshifts) # This just ensures that the list doesn't get accidentally altered
    del _snapshot_redshifts


    #--------------------|
    # Start dask cluster |
    #--------------------|
    if settings.dask_workers > 0:
        Console.print_info("Starting dask cluster.", flush = True)

        cluster = LocalCluster(
            n_workers = settings.dask_workers,
            memory_limit = f"{settings.dask_memory_per_worker}GB",
            dashboard_address = f":{settings.dask_port}" if settings.dask_port is not None else None
        )
        client = cluster.get_client()

        Console.print_info(f"Dask cluster running with {settings.dask_workers} workers each allocated {settings.dask_memory_per_worker} GB of memory.")

        if settings.dask_port is not None:
            Console.print_info(f"Dask dashboard available at {socket.gethostname()}:{settings.dask_port}")
        else:
            Console.print_verbose_info("No dask dashboard (dask_port was set to null).")

    #---------------------|
    # Loop over snapshots |
    #---------------------|
    Console.print_info("Looping over snapshots:", flush = True)
    previous_filepath: str|None = None
    for snapshot_index, (tag, snapshot_files) in enumerate(zip(target_tags, target_snapshots_info)):

        if snapshot_index > target_tags_end_index:
            Console.print_info(f"Stopping before {tag} due to end condition.", flush = True)
            break

        #-------------------------|
        # Check restart condition |
        #-------------------------|

        if snapshot_index < target_tags_start_index:
            Console.print_info(f"Skipping {tag} as it is before the start tag.", flush = True)
            continue
        else:
            Console.print_info(f"Doing {tag}:", flush = True)

        if snapshot_index == target_tags_start_index and snapshot_index > 0:
            restart_file_path = make_file_path(args.output_directory, target_tags[snapshot_index - 1])
            if os.path.exists(restart_file_path):
                previous_filepath = restart_file_path
            else:
                Console.print_warning("Start tag is not the first target and no output file found to restart from.\nIf that is intentional then this warning may be safely ignored.")
        Console.print_debug(f"    Previous filepath: {previous_filepath}")

        #--------------------|
        # Create output file |
        #--------------------|
        Console.print_info("    Creating output file.")

        # Get number of columns and datatype of each field
        field_widths: dict[str, int] = {
            "ParticleIDs"         : 1,
            "GroupNumber"         : 1,
            "FirstSubhaloID"      : 1,
            "SubGroupNumber"      : 1,
            "SubhaloID"           : 1,
            "LastGroupRedshift"   : 1,
            "LastSubhaloRedshift" : 1,
            "HaloMass"            : 1,
            "HaloM200Crit"        : 1,
        }
        field_datatypes: dict[str, object] = {
            "ParticleIDs"         : np.uint64,
            "GroupNumber"         : np.uint32,
            "FirstSubhaloID"      : np.uint32,
            "SubGroupNumber"      : np.uint32,
            "SubhaloID"           : np.uint32,
            "LastGroupRedshift"   : np.float64,
            "LastSubhaloRedshift" : np.float64,
            "HaloMass"            : np.float32,
            "HaloM200Crit"        : np.float32,
        }
        with h5.File(snapshot_files.catalogue_file_template.format(0), "r") as file:
            for output_name, (catalogue_field, _) in fof_group_fields_user.items():
                field_widths[output_name]    = file["FOF"][catalogue_field].shape[-1] if len(file["FOF"][catalogue_field].shape) > 1 else 1
                field_datatypes[output_name] = file["FOF"][catalogue_field].dtype.type
            for output_name, (catalogue_field, _) in subgroup_fields_user_all.items():
                field_widths[output_name]    = file["Subhalo"][catalogue_field].shape[-1] if len(file["Subhalo"][catalogue_field].shape) > 1 else 1
                field_datatypes[output_name] = file["Subhalo"][catalogue_field].dtype.type
            for output_name, (catalogue_field, _) in subgroup_fields_user_centrals.items():
                field_widths[output_name]    = file["Subhalo"][catalogue_field].shape[-1] if len(file["Subhalo"][catalogue_field].shape) > 1 else 1
                field_datatypes[output_name] = file["Subhalo"][catalogue_field].dtype.type

        with h5.File(snapshot_files.snapshot_file_template.format(0), "r") as file:
            numbers_of_particles = list(file["Header"].attrs["NumPart_Total"])

        current_output_filepath: str = make_file_path(args.output_directory, tag)
        try:
            make_output_file(
                filepath = current_output_filepath,
                allow_overwrite = args.overwrite,
                allow_update = args.update,
                redshift = snapshot_redshifts[snapshot_index],
                redshift_of_all_files = snapshot_redshifts,
                tags_of_all_files = target_tags,
                number_of_gas_particles = numbers_of_particles[0] if settings.do_gas else None,
                number_of_dark_matter_particles = numbers_of_particles[1] if settings.do_dark_matter else None,
                number_of_star_particles = numbers_of_particles[4] if settings.do_stars else None,
                number_of_black_hole_particles = numbers_of_particles[5] if settings.do_black_holes else None,
                field_widths = field_widths,
                field_datatypes = field_datatypes,
                pre_initialise_with_null = previous_filepath is None
            )
        except FileExistsError:
            Console.print_error(f"A file already exists at \"{current_output_filepath}\" and --overwrite has not been set or ")
            sys.exit(1)
        except KeyError as e:
            Console.print_error(f"Output file being updated already contains a requested dataset. See below error message:\n{e}")
            sys.exit(1)

        if check_cache_exists(os.path.join(args.output_directory, "tmp"), tag.tag):

            Console.print_warning("        Using existing cached catalogue data - this should usually only occur during testing or restarting.")

        else:

            #--------------------|
            # Load new catalogue |
            #--------------------|
            Console.print_info("    Loading catalogue.")

            if not subgroup_fields_include_nested:
                catalogue_data = load_catalogue(snapshot_files, group_fields = fof_group_field_names_for_reading, subfind_fields = subgroup_field_names_for_reading)
            else:
                catalogue_data = load_catalogue(snapshot_files, group_fields = fof_group_field_names_for_reading, subfind_fields = None)
                subsets = [
                    load_catalogue(snapshot_files, group_fields = None, subfind_fields = list(fields.keys()), subfind_alternate_group_path = root)["Subhalo"].rename(
                        fields
                    )
                    for root, fields
                    in subgroups_with_nested_fields__by_root.items()
                ]
                merge_datasets = lambda *datasets: datasets[0].merge(merge_datasets(*datasets[1:])) if len(datasets) > 1 else datasets[0]
                catalogue_data["Subhalo"] = merge_datasets(*subsets)

            #--------------------------------|
            # Cache catalogue as single file |
            #--------------------------------|
            Console.print_info("    Caching target catalogue data.")

            cache_catalogue_as_single_file(
                cache_directory = os.path.join(args.output_directory, "tmp"),
                identifier = tag.tag,
                fof_dataset = catalogue_data["FOF"],
                subhalo_dataset = catalogue_data["Subhalo"]
            )

        #----------------------|
        # Reload new catalogue |
        #----------------------|
        Console.print_info("    Loading catalogue again (from single file cache).")

        catalogue_data = load_catalogue_cache(os.path.join(args.output_directory, "tmp"), tag.tag)

        fof_groups_present: bool = catalogue_data["FOF"].sizes["catalogue_fof_index"] > 0
        subhaloes_present: bool = catalogue_data["Subhalo"].sizes["catalogue_subhalo_index"] > 0
        if not fof_groups_present or not subhaloes_present:
            Console.print_info("    No structures in this snapshot. Data will be propagated if necessary.")
            Console.print_error("    No structures in this snapshot. This is not currently supported. Start from a snapshot that contains structures.")
            Console.print_error("Terminating.")
            return#TODO: fix this!!!

        #-----------------------|
        # Load previous outputs |
        #-----------------------|
        if snapshot_index > 0 and previous_filepath is not None:
            Console.print_info("    Loading previous results.")

            #last_snapshot_output = load_output_file(
            #    previous_filepath,
            #    "GroupNumber", "FirstSubhaloID", "SubGroupNumber", "SubhaloID", "LastGroupRedshift", "LastSubhaloRedshift",
            #    *fof_group_fields.keys(),
            #    *subgroup_fields_user_centrals.keys(),
            #    *subgroup_fields_user_all.keys()
            #)

            #last_snapshot_output = {
            #    "PartType0" : xr.open_dataset(previous_filepath, engine = "h5netcdf", group = "PartType0", chunks = "auto") if settings.do_gas         else None,
            #    "PartType1" : xr.open_dataset(previous_filepath, engine = "h5netcdf", group = "PartType1", chunks = "auto") if settings.do_dark_matter else None,
            #    "PartType4" : xr.open_dataset(previous_filepath, engine = "h5netcdf", group = "PartType4", chunks = "auto") if settings.do_stars       else None,
            #    "PartType5" : xr.open_dataset(previous_filepath, engine = "h5netcdf", group = "PartType5", chunks = "auto") if settings.do_black_holes else None
            #}

            Console.print_debug(f"        File: \"{previous_filepath}.zarr\"")
            last_snapshot_output = {
                "PartType0" : xr.open_zarr(f"{previous_filepath.rsplit(".", 1)[0]}.zarr", group = "PartType0", chunks = "auto") if settings.do_gas         else None,
                "PartType1" : xr.open_zarr(f"{previous_filepath.rsplit(".", 1)[0]}.zarr", group = "PartType1", chunks = "auto") if settings.do_dark_matter else None,
                "PartType4" : xr.open_zarr(f"{previous_filepath.rsplit(".", 1)[0]}.zarr", group = "PartType4", chunks = "auto") if settings.do_stars       else None,
                "PartType5" : xr.open_zarr(f"{previous_filepath.rsplit(".", 1)[0]}.zarr", group = "PartType5", chunks = "auto") if settings.do_black_holes else None
            }

        #--------------------------|
        # Loop over particle types |
        #--------------------------|

        for particle_type in ("PartType0", "PartType1", "PartType4", "PartType5"):

            # Skip unspecified particle types
            if particle_type == "PartType0" and not settings.do_gas:
                continue
            if particle_type == "PartType1" and not settings.do_dark_matter:
                continue
            if particle_type == "PartType4" and not settings.do_stars:
                continue
            if particle_type == "PartType5" and not settings.do_black_holes:
                continue

            Console.print_info(f"    Doing {particle_type}:")

            #-----------------------------|
            # Load membership information |
            #-----------------------------|
            Console.print_info("        Loading membership.")

            membership = load_hdf5_files_with_xarray(
                [make_aux_file_path(settings.halo_membership_by_particle_directory, tag, settings.snipshots)],
                particle_type,
                ["ParticleIDs", "GroupNumber", "SubGroupNumber"],
                override_chunks_in_all_dimensions = "auto",
                dimension_sizes = { "snapshot_particle_index" : None }
            )

            Console.print_debug(membership)

            Console.print_info("        Computing membership masks.")

            Console.print_verbose_info("            FOF mask.")
            fof_update_mask: xr.DataArray
            if fof_groups_present:
                groups_with_subgroups: xr.DataArray = xr.where(catalogue_data["FOF"]["HaloM200Crit"].isel(catalogue_fof_index = membership["GroupNumber"] - 1) > 0.0, True, False)
                fof_update_mask = xr.where(membership["GroupNumber"] != NULL_INDEX, groups_with_subgroups, False)
            else:
                fof_update_mask = xr.DataArray(
                    data = dask_array.full_like(membership["GroupNumber"].data, fill_value = False, dtype = np.bool_),
                    dims = membership["GroupNumber"].dims
                )

#            Console.print_verbose_info("            Inverse FOF.")
#            fof_update_mask_inverse: xr.DataArray = ~ fof_update_mask#TODO: not needed if using delayed results???

            Console.print_verbose_info("            Subhalo mask.")
            subhalo_update_mask: xr.DataArray
            if subhaloes_present:
                subhalo_update_mask = xr.where(membership["SubGroupNumber"] != NULL_INDEX, True, False)
            else:
                subhalo_update_mask = xr.DataArray(
                    data = dask_array.full_like(membership["SubGroupNumber"].data, fill_value = False, dtype = np.bool_),
                    dims = membership["SubGroupNumber"].dims
                )

#            Console.print_verbose_info("            Inverse Subhalo.")
#            subhalo_update_mask_inverse: xr.DataArray = ~ subhalo_update_mask#TODO: not needed if using delayed results???

            Console.print_verbose_info("        Counting number of particles in structures.")
            with h5.File(current_output_filepath, "a") as file:
                file[particle_type].attrs["NumberInGroups"] = fof_update_mask.sum().values
                file[particle_type].attrs["NumberInSubhaloes"] = subhalo_update_mask.sum().values
                Console.print_debug(f"        FOF: {file[particle_type].attrs["NumberInGroups"]}, Subhalo: {file[particle_type].attrs["NumberInSubhaloes"]}")

            #-------------------------|
            # Propagate existing data |
            #-------------------------|
            reorder_cache_filepath: str|None = None
            if snapshot_index > 0 and previous_filepath is not None:

                #----------------------|
                # Cache reorder result |
                #----------------------|

                reorder_cache_filepath = os.path.join(args.output_directory, "tmp", f"{tag}-reorder-cache.zarr")

                if os.path.exists(reorder_cache_filepath):
                    Console.print_warning("        Using existing cached reorder data - this should usually only occur during testing or restarting.")

                else:

                    #--------------------------|
                    # Load reordering sequence |
                    #--------------------------|
                    Console.print_info("        Loading reordering indexes.")

                    reorder_data = load_hdf5_files_with_xarray(
                        [make_reorder_file_path(settings.reorder_indexes_directory, target_tags[snapshot_index - 1], tag)],
                        particle_type,
                        ["ForwardsIndexes"],
                        override_chunks_in_all_dimensions = "auto",
                        dimension_sizes = { "snapshot_particle_index" : None }
                    )

                    Console.print_debug(reorder_data)

                    #--------------------------------------|
                    # Compute and cache the reordered data |
                    #--------------------------------------|
                    # Compute the reordered data and cache it on disk to avoid needing to do this later
                    Console.print_info("        Computing reorder and caching.")

                    def load_large_particle_array(delayed_data: xr.DataArray, chunks = settings.number_of_reads_per_large_array) -> np.ndarray[tuple[int], np.dtype[np.integer|np.floating]]:

                        if delayed_data.chunks is None:
                            raise ValueError("Data load target has no chunks!")

                        total_chunks = chunks if len(delayed_data.dims) < 2 else chunks * np.prod([delayed_data.sizes[dim_name] for dim_name in delayed_data.dims[1:]])

                        chunk_size = delayed_data.sizes["snapshot_particle_index"] // total_chunks
                        if total_chunks * chunk_size < delayed_data.sizes["snapshot_particle_index"]:
                            total_chunks += 1

                        return np.concatenate(
                            [
                                # Load each small chunk with xarray
                                delayed_data[chunk_size * i : min(chunk_size * (i + 1), delayed_data.sizes["snapshot_particle_index"])].compute()
                                for i
                                in range(total_chunks)
                            ],
                            axis = 0
                        )

                    def load_large_particle_array_as_xarray(delayed_data: xr.DataArray, chunks = settings.number_of_reads_per_large_array) -> xr.DataArray:

                        if delayed_data.chunks is None:
                            raise ValueError("Data load target has no chunks!")

                        chunk_size = delayed_data.sizes["snapshot_particle_index"] // chunks
                        if chunks * chunk_size < delayed_data.sizes["snapshot_particle_index"]:
                            chunks += 1

                        return xr.DataArray(
                            load_large_particle_array(delayed_data, chunks),
                            name = field,
                            dims = delayed_data.dims
                        ).chunk({ delayed_data.dims[i] : delayed_data.chunks[i] for i in range(len(delayed_data.dims)) })

                    def load_previous_data(field: str) -> np.ndarray[tuple[int], np.dtype[np.integer|np.floating]]:
                        return load_large_particle_array(last_snapshot_output[particle_type][field])

                    def load_previous_data_as_xarray(field: str) -> xr.DataArray:
                        return load_large_particle_array_as_xarray(last_snapshot_output[particle_type][field])

                    Console.print_verbose_info("            Loading reorder indexes.")
                    #reorder_indexes = load_large_particle_array_as_xarray(reorder_data["ForwardsIndexes"])
                    reorder_indexes = load_large_particle_array(reorder_data["ForwardsIndexes"])

                    rechunking_layout = { membership["ParticleIDs"].dims[i] : membership["ParticleIDs"].chunks[i] for i in range(len(membership["ParticleIDs"].dims)) }
                    def reorder_and_cache_field(field: str):
                        Console.print_info(f"            {field}:")
                        Console.print_debug("                Loading previous data.")
                        #existing_data = load_previous_data_as_xarray(field)
                        existing_data = load_previous_data(field)
                        Console.print_debug("                Reordering data.")
                        #new_data = existing_data.isel(snapshot_particle_index = reorder_indexes)
                        new_data = existing_data[reorder_indexes]
                        del existing_data
                        Console.print_debug("                Wrapping with xarray.")
                        cached_data = xr.Dataset()
                        cached_data[field] = xr.DataArray(
                            name = field,
                            #dims = existing_data.dims,
                            dims = last_snapshot_output[particle_type][field].dims,
                            data = new_data,
                            attrs = {}
                        ).chunk(rechunking_layout)
                        Console.print_verbose_info(f"                Shape: {cached_data[field].shape}")
                        Console.print_debug("                Writing to cache.")
                        #Console.print_debug("                Calculating and writing to cache.")

                        # Number of gigabytes of data per row * number of rows
                        total_gigabytes: Callable[[int], float] = lambda rows: rows * np.array(cached_data[field].shape[1:]).prod() * cached_data[field].dtype.itemsize / 1024**3

                        if settings.max_size_to_write is None or total_gigabytes(cached_data[field].sizes["snapshot_particle_index"]) <= settings.max_size_to_write:
                            # Just write the data in one go using xarray
                            cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
                            # This may fail if there is little remaining memory available.
                            # In such a case, set a maximum size that is sufficiently small to enable the below section.

                        else:
                            # (at least) One of the data sets is too large to write in one go using xarray due to memory constrains.
                            # This is most likley to happen when tracking 2D data sets.
                            # In this case, the maximum amount of data that can be written in one go is limited by the user.
                            # Each write operation using xarray should only attempt to write a number of rows with a total data size less than the limit.
                            # Note: in the event the chunks are too large / limit is too small, a warning is issued and the entirety of the oversize chunk(s) will be written as a single operation.
    
                            # Prepare somwhere to put the data on disk - dosen't actually write any data!
                            cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type, compute = False)

                            # Get information about the dask chunking of the target data.
                            # We only want to write whole chunks!
                            #TODO: should this be zarr chunks instead of dask ones?!
                            chunk_lengths = np.array(cached_data.chunksizes["snapshot_particle_index"], dtype = np.int64)
                            chunk_lengths_endpoints = np.cumsum(chunk_lengths)
                            # Raise an error if any chunks exceed the limit.
                            if (total_gigabytes(chunk_lengths) > settings.max_size_to_write).any():
                                Console.print_debug(f"Chunk lengths: {", ".join(map(str, chunk_lengths))}")
                                Console.print_warning(f"One or more data chunks exceed the maximum write size of {settings.max_size_to_write} GB.")

                            # Pre-calculate what the start and endpoints are for each write operation.
                            start_positions: list[int] = []
                            end_positions:   list[int] = []
                            chunk_offset:    int       = 0
                            while (end_positions[-1] if len(end_positions) > 0 else 0) < cached_data.dims["snapshot_particle_index"]:
                                # Initialise counters for this operation.
                                number_of_chunks: int = 0
                                selected_length:  int = 0
                                # Increment counters until they overflow the data limit or pass the number of chunks avalible.
                                while total_gigabytes(selected_length) < settings.max_size_to_write and chunk_offset + number_of_chunks < len(chunk_lengths):
                                    number_of_chunks += 1
                                    selected_length = chunk_lengths[chunk_offset : chunk_offset + number_of_chunks].sum()
                                if number_of_chunks == 0:
                                    # If the next chunk is too large, do one chunk anyway!
                                    number_of_chunks = 1
                                if total_gigabytes(selected_length) > settings.max_size_to_write:
                                    # Chances are the last chunk's data didn't reach the limit exactly, so walk back by one chunk.
                                    number_of_chunks -= 1
                                    # The `selected_length` isn't used after this point, so no need to fix its value.
                                # Update the list of start and endpoints.
                                start_positions.append(chunk_lengths_endpoints[chunk_offset] - chunk_lengths[chunk_offset]) # The start index of the current start chunk
                                end_positions.append(chunk_lengths_endpoints[chunk_offset + number_of_chunks - 1]) # The end index of the last chunk in the selected group
                                Console.print_debug(f"Chunks: {number_of_chunks}, Size: {total_gigabytes(end_positions[-1] - start_positions[-1])} GB, i: {start_positions[-1]} -> {end_positions[-1]}")

                                # Move the offset to the next unselected chunk.
                                chunk_offset += number_of_chunks

                            # Loop over each write operation.
                            for start, end in zip(start_positions, end_positions):
                                cached_data.isel(
                                    snapshot_particle_index = slice(start, end) # Select only the section of the total data that should be written.
                                ).to_zarr(
                                    reorder_cache_filepath, mode = "a", group = particle_type,
                                    region = { "snapshot_particle_index" : slice(start, end) } # Write to only the target section of the zarr data store.
                                )

                        Console.print_debug("                Done.") # Just to make the debug statements clear.

                    # Run a test to ensure data reordering is working correctly:
                    #Console.print_debug("Running reorder test:")
                    #Console.print_debug("    Loading previous data.")
                    #existing_particle_ids = load_previous_data("ParticleIDs")
                    #Console.print_debug("    Reordering data.")
                    #reordered_particle_ids = existing_particle_ids[reorder_indexes]
                    #Console.print_debug("    Wrapping with xarray.")
                    #test_reordered_particle_ids = xr.DataArray(
                    #    name = "ParticleIDs",
                    #    dims = last_snapshot_output[particle_type]["ParticleIDs"].dims,
                    #    data = reordered_particle_ids,
                    #    attrs = {}
                    #).chunk(rechunking_layout)
                    #Console.print_debug("    Testing for mismatches.")
                    #Console.print_debug("    Number of mismatched IDs:", (test_reordered_particle_ids != membership["ParticleIDs"]).sum().values)

                    reorder_and_cache_field("GroupNumber")
                    reorder_and_cache_field("LastGroupRedshift")
                    reorder_and_cache_field("FirstSubhaloID")
                    reorder_and_cache_field("SubGroupNumber")
                    reorder_and_cache_field("SubhaloID")
                    reorder_and_cache_field("LastSubhaloRedshift")
                    for field in fof_group_fields:
                        reorder_and_cache_field(field)
                    for field in subgroup_fields_user_centrals:
                        reorder_and_cache_field(field)
                    for field in subgroup_fields_user_all:
                        reorder_and_cache_field(field)

#                    cached_data = xr.Dataset()
#                    Console.print_info("            GroupNumber")
#                    cached_data["GroupNumber"] = xr.DataArray(
#                        name = "GroupNumber",
#                        dims = "snapshot_particle_index",
#                        data = load_previous_data("GroupNumber").isel(snapshot_particle_index = load_large_particle_array(reorder_data["ForwardsIndexes"])),
#                        attrs = {}
#                    )
#                    Console.print_verbose_info(f"                Shape: {cached_data["GroupNumber"].shape}")
#                    cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
#
#                    cached_data = xr.Dataset()
#                    Console.print_info("            LastGroupRedshift")
#                    cached_data["LastGroupRedshift"] = xr.DataArray(
#                        name = "LastGroupRedshift",
#                        dims = "snapshot_particle_index",
#                        data = load_previous_data("LastGroupRedshift").isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]),
#                        attrs = {}
#                    )
#                    Console.print_verbose_info(f"                Shape: {cached_data["LastGroupRedshift"].shape}")
#                    cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
#
#                    cached_data = xr.Dataset()
#                    Console.print_info("            FirstSubhaloID")
#                    cached_data["FirstSubhaloID"] = xr.DataArray(
#                        name = "FirstSubhaloID",
#                        dims = "snapshot_particle_index",
#                        data = load_previous_data("FirstSubhaloID").isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]),
#                        attrs = {}
#                    )
#                    Console.print_verbose_info(f"                Shape: {cached_data["FirstSubhaloID"].shape}")
#                    cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
#
#                    cached_data = xr.Dataset()
#                    Console.print_info("            SubGroupNumber")
#                    cached_data["SubGroupNumber"] = xr.DataArray(
#                        name = "SubGroupNumber",
#                        dims = "snapshot_particle_index",
#                        data = load_previous_data("SubGroupNumber").isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]),
#                        attrs = {}
#                    )
#                    Console.print_verbose_info(f"                Shape: {cached_data["SubGroupNumber"].shape}")
#                    cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
#
#                    cached_data = xr.Dataset()
#                    Console.print_info("            SubhaloID")
#                    cached_data["SubhaloID"] = xr.DataArray(
#                        name = "SubhaloID",
#                        dims = "snapshot_particle_index",
#                        data = load_previous_data("SubhaloID").isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]),
#                        attrs = {}
#                    )
#                    Console.print_verbose_info(f"                Shape: {cached_data["SubhaloID"].shape}")
#                    cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
#
#                    cached_data = xr.Dataset()
#                    Console.print_info("            LastSubhaloRedshift")
#                    cached_data["LastSubhaloRedshift"] = xr.DataArray(
#                        name = "LastSubhaloRedshift",
#                        dims = "snapshot_particle_index",
#                        data = load_previous_data("LastSubhaloRedshift").isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]),
#                        attrs = {}
#                    )
#                    Console.print_verbose_info(f"                Shape: {cached_data["LastSubhaloRedshift"].shape}")
#                    cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
#
#                    for field, catalogue_field in fof_group_fields.items():
#                        cached_data = xr.Dataset()
#                        Console.print_info(f"            {field} ({catalogue_field})")
#                        cached_data[field] = xr.DataArray(
#                            name = field,
#                            dims = "snapshot_particle_index" if len(catalogue_data["FOF"][catalogue_field].shape) == 1 else ("snapshot_particle_index", "particle_type_number"),
#                            data = load_previous_data(field).isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]),
#                            attrs = {}
#                        )
#                        Console.print_verbose_info(f"                Shape: {cached_data[field].shape}")
#                        cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
#
#                    for field, catalogue_field in subgroup_fields_user_centrals.items():
#                        cached_data = xr.Dataset()
#                        Console.print_info(f"            {field} ({catalogue_field})")
#                        cached_data[field] = xr.DataArray(
#                            name = field,
#                            dims = "snapshot_particle_index" if len(catalogue_data["Subhalo"][catalogue_field].shape) == 1 else ("snapshot_particle_index", "particle_type_number"),
#                            data = load_previous_data(field).isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]),
#                            attrs = {}
#                        )
#                        Console.print_verbose_info(f"                Shape: {cached_data[field].shape}")
#                        cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)
#
#                    for field, catalogue_field in subgroup_fields_user_all.items():
#                        cached_data = xr.Dataset()
#                        Console.print_info(f"            {field} ({catalogue_field})")
#                        cached_data[field] = xr.DataArray(
#                            name = field,
#                            dims = "snapshot_particle_index" if len(catalogue_data["Subhalo"][catalogue_field].shape) == 1 else ("snapshot_particle_index", "particle_type_number"),
#                            data = load_previous_data(field).isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]),
#                            attrs = {}
#                        )
#                        Console.print_verbose_info(f"                Shape: {cached_data[field].shape}")
#                        cached_data.to_zarr(reorder_cache_filepath, mode = "a", group = particle_type)

                #--------------------------------|
                # Load reordered data from cache |
                #--------------------------------|

                cached_data = xr.open_zarr(reorder_cache_filepath, group = particle_type, chunks = "auto")

                def insert_existing_data(field: str, updates: xr.DataArray|int|float, update_mask: xr.DataArray) -> xr.DataArray:
                    return xr.where(update_mask, updates, cached_data[field])

                #reorder_indexes = reorder_data["ForwardsIndexes"][fof_update_mask_inverse]#TODO: this will fail later as there are other update masks and its getting masked twice!!!

#                for field in (
#                    "GroupNumber",
#                    "LastGroupRedshift",
#                    "FirstSubhaloID",
#                    "SubGroupNumber",
#                    "SubhaloID",
#                    "LastSubhaloRedshift",
#                    *list(fof_group_fields.keys()) + list(subgroup_fields_user_centrals.keys()),
#                    *list(subgroup_fields_user_all.keys())
#                ):
#                    data = last_snapshot_output[particle_type][field].isel(file_order = reorder_data["ForwardsIndexes"])
#                    data.to_netcdf(
#                        os.path.join(args.output_directory, "tmp", f"reordered-data-{tag.tag}-{particle_type}.hdf5"),
#                        mode = "a",
#                        engine = "h5netcdf"
#                    )
#
#                reordered_existing_data: xr.Dataset = xr.open_dataset(os.path.join(args.output_directory, "tmp", f"reordered-data-{tag.tag}-{particle_type}.hdf5"), engine = "h5netcdf", dims=("file_order",))
#
#                def insert_existing_data(field: str, updates: xr.DataArray|int|float, update_mask: xr.DataArray, integer: bool) -> xr.DataArray:
#                    if integer:
#                        Console.print_debug(update_mask)
#                        Console.print_debug(updates)
#                        Console.print_debug(reordered_existing_data[field])
#                        return xr.where(update_mask, updates, reordered_existing_data[field])
#                    elif isinstance(updates, (int, float)):
#                        return xr.where(update_mask, updates, reordered_existing_data[field])
#                    else:
#                        return updates.where(update_mask, other = reordered_existing_data[field])
#
#                def insert_existing_data(field: str, updates: xr.DataArray|int|float, update_mask: xr.DataArray, integer: bool) -> xr.DataArray:
#                    if integer:
#                        return xr.where(update_mask, updates, last_snapshot_output[particle_type][field].isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]))
#                    elif isinstance(updates, (int, float)):
#                        return xr.where(update_mask, updates, last_snapshot_output[particle_type][field].isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]))
#                    else:
#                        return updates.where(update_mask, other = last_snapshot_output[particle_type][field].isel(snapshot_particle_index = reorder_data["ForwardsIndexes"]))

#                #-------------------------|
#                # Propagate existing data |
#                #-------------------------|
#                Console.print_info("        Propagating existing data.")
#
#                for field in ("GroupNumber", "LastGroupRedshift"):
#                    # These fields are not read from catalogue data or are computed in some way
#
#                    #-----------------------|
#                    # Reorder existing data |
#                    #-----------------------|
#                    Console.print_verbose_info(f"            {field}")
#
#                    write_to_output_field(current_output_filepath, particle_type, field, last_snapshot_output[particle_type][field][reorder_indexes[fof_update_mask_inverse]].compute(), mask = fof_update_mask_inverse)
#
#                for field in ("FirstSubhaloID", "SubGroupNumber", "SubhaloID", "LastSubhaloRedshift"):
#                    # These fields are not read from catalogue data or are computed in some way
#
#                    #-----------------------|
#                    # Reorder existing data |
#                    #-----------------------|
#                    Console.print_verbose_info(f"            {field}")
#
#                    write_to_output_field(current_output_filepath, particle_type, field, last_snapshot_output[particle_type][field][reorder_indexes[subhalo_update_mask_inverse]].compute(), mask = subhalo_update_mask_inverse)
#
#                for field in list(fof_group_fields.keys()) + list(subgroup_fields_user_centrals.keys()):
#                    # These are raw data fields
#
#                    #-----------------------|
#                    # Reorder existing data |
#                    #-----------------------|
#                    Console.print_verbose_info(f"            {field}")
#
#                    write_to_output_field(current_output_filepath, particle_type, field, last_snapshot_output[particle_type][field][reorder_indexes[fof_update_mask_inverse]].compute(), mask = fof_update_mask_inverse)
#
#                for field in list(subgroup_fields_user_all.keys()):
#                    # These are raw data fields
#
#                    #-----------------------|
#                    # Reorder existing data |
#                    #-----------------------|
#                    Console.print_verbose_info(f"            {field}")
#
#                    write_to_output_field(current_output_filepath, particle_type, field, last_snapshot_output[particle_type][field][reorder_indexes[subhalo_update_mask_inverse]].compute(), mask = subhalo_update_mask_inverse)

            else:
                Console.print_debug(f"        snapshot_index={snapshot_index}, previous_filepath={previous_filepath}.")
                Console.print_info("        No previous data to propagate.")

                def insert_existing_data(field: str, updates: xr.DataArray|int|float, update_mask: xr.DataArray) -> xr.DataArray:
                    datatype: type|np.dtype
                    try:
                        datatype = updates.dtype
                    except AttributeError:
                        datatype = type(updates)
                    if is_integer_type(datatype):
                        return xr.where(update_mask, updates, NULL_INDEX)
                    elif isinstance(updates, float):
                        return xr.where(update_mask, updates, np.nan)#xr_dtypes.NA
                    else:
                        return updates.where(update_mask)

            #-------------------------|
            # Compute subhalo indexes |
            #-------------------------|
            Console.print_info("        Calculating global catalogue indexes for FOF and Subhalo datasets.")

            Console.print_verbose_info("            FOF indexes from GroupNumber.")
            halo_update_indexes = (membership["GroupNumber"] - 1).where(fof_update_mask, other = NULL_INDEX)

            def get_catalogue_fof_data_by_particle(field: str, fill_value = None, indexes: tuple[int, ...]|None = None) -> xr.DataArray:
                target = catalogue_data["FOF"][field]
                if indexes is not None:
                    target = target.isel(particle_type_number = list(indexes) if len(indexes) > 1 else indexes[0], drop = True)
                return target.isel(catalogue_fof_index = halo_update_indexes).where(fof_update_mask, other = fill_value)

            Console.print_verbose_info("            Subhalo indexes for centrals.")
            central_subhalo_update_indexes = get_catalogue_fof_data_by_particle("FirstSubhaloID", fill_value = NULL_INDEX)

            def get_catalogue_central_subhalo_data_by_particle(field: str, fill_value = None, indexes: tuple[int, ...]|None = None) -> xr.DataArray:
                target = catalogue_data["Subhalo"][field]
                if indexes is not None:
                    target = target.isel(particle_type_number = list(indexes) if len(indexes) > 1 else indexes[0], drop = True)
                result = target.isel(catalogue_subhalo_index = central_subhalo_update_indexes).where(fof_update_mask, other = fill_value)
                #if len(result.dims) > 1:
                #    result = result.rename({
                #        result.dims[1] : "particle_type_number"
                #    })
                return result

            Console.print_verbose_info("            Subhalo indexes from FirstSubhaloID and SubGroupNumber.")
            subhalo_update_indexes = (catalogue_data["FOF"]["FirstSubhaloID"].isel(catalogue_fof_index = membership["GroupNumber"] - 1) + membership["SubGroupNumber"]).where(subhalo_update_mask, other = NULL_INDEX)

            def get_catalogue_subhalo_data_by_particle(field: str, fill_value = None, indexes: tuple[int, ...]|None = None) -> xr.DataArray:
                target = catalogue_data["Subhalo"][field]
                if indexes is not None:
                    target = target.isel(particle_type_number = list(indexes) if len(indexes) > 1 else indexes[0], drop = True)
                result = target.isel(catalogue_subhalo_index = subhalo_update_indexes).where(subhalo_update_mask, other = fill_value)
                #if len(result.dims) > 1:
                #    result = result.rename({
                #        result.dims[1] : "particle_type_number"
                #    })
                return result

            #----------------------------------------------------------|
            # Locate and update new values for particles in structures |
            #----------------------------------------------------------|
            Console.print_info("        Determining update operations:")

            updated_data = xr.Dataset()

            Console.print_info("            ParticleIDs")
            updated_data["ParticleIDs"] = xr.DataArray(
                name = "ParticleIDs",
                dims = "snapshot_particle_index",
                data = membership["ParticleIDs"],
                attrs = {
                }
            )
            Console.print_verbose_info(f"                Shape: {updated_data["ParticleIDs"].shape}")

            Console.print_info("            GroupNumber")
            updated_data["GroupNumber"] = xr.DataArray(
                name = "GroupNumber",
                dims = "snapshot_particle_index",
                data = insert_existing_data("GroupNumber", membership["GroupNumber"], fof_update_mask),
                attrs = {
                }
            )
            Console.print_verbose_info(f"                Shape: {updated_data["GroupNumber"].shape}")

            Console.print_info("            LastGroupRedshift")
            updated_data["LastGroupRedshift"] = xr.DataArray(
                name = "LastGroupRedshift",
                dims = "snapshot_particle_index",
                data = insert_existing_data("LastGroupRedshift", snapshot_redshifts[snapshot_index], fof_update_mask),
                attrs = {
                }
            )
            Console.print_verbose_info(f"                Shape: {updated_data["LastGroupRedshift"].shape}")

            Console.print_info("            FirstSubhaloID")
            updated_data["FirstSubhaloID"] = xr.DataArray(
                name = "FirstSubhaloID",
                dims = "snapshot_particle_index",
                data = insert_existing_data("FirstSubhaloID", central_subhalo_update_indexes, fof_update_mask),
                attrs = {
                }
            )
            Console.print_verbose_info(f"                Shape: {updated_data["FirstSubhaloID"].shape}")

            Console.print_info("            SubGroupNumber")
            updated_data["SubGroupNumber"] = xr.DataArray(
                name = "SubGroupNumber",
                dims = "snapshot_particle_index",
                data = insert_existing_data("SubGroupNumber", membership["SubGroupNumber"], subhalo_update_mask),
                attrs = {
                }
            )
            Console.print_verbose_info(f"                Shape: {updated_data["SubGroupNumber"].shape}")

            Console.print_info("            SubhaloID")
            updated_data["SubhaloID"] = xr.DataArray(
                name = "SubhaloID",
                dims = "snapshot_particle_index",
                data = insert_existing_data("SubhaloID", subhalo_update_indexes, subhalo_update_mask),
                attrs = {
                }
            )
            Console.print_verbose_info(f"                Shape: {updated_data["SubhaloID"].shape}")

            Console.print_info("            LastSubhaloRedshift")
            updated_data["LastSubhaloRedshift"] = xr.DataArray(
                name = "LastSubhaloRedshift",
                dims = "snapshot_particle_index",
                data = insert_existing_data("LastSubhaloRedshift", snapshot_redshifts[snapshot_index], subhalo_update_mask),
                attrs = {
                }
            )
            Console.print_verbose_info(f"                Shape: {updated_data["LastSubhaloRedshift"].shape}")

            for field, (catalogue_field, column_indexes) in fof_group_fields.items():
                Console.print_info(f"            {field} ({catalogue_field})")
                updated_data[field] = xr.DataArray(
                    name = field,
                    dims = "snapshot_particle_index" if ((column_indexes is not None and len(column_indexes) == 1) or len(catalogue_data["FOF"][catalogue_field].shape) == 1) else ("snapshot_particle_index", "particle_type_number"),
                    data = insert_existing_data(field, get_catalogue_fof_data_by_particle(catalogue_field, indexes = column_indexes), fof_update_mask),
                    attrs = {
                    }
                )
                Console.print_verbose_info(f"                Shape: {updated_data[field].shape}")

            for field, (catalogue_field, column_indexes) in subgroup_fields_user_centrals.items():
                Console.print_info(f"            {field} ({catalogue_field})")
                updated_data[field] = xr.DataArray(
                    name = field,
                    dims = "snapshot_particle_index" if ((column_indexes is not None and len(column_indexes) == 1) or len(catalogue_data["Subhalo"][catalogue_field].shape) == 1) else ("snapshot_particle_index", "particle_type_number"),
                    data = insert_existing_data(field, get_catalogue_central_subhalo_data_by_particle(catalogue_field, indexes = column_indexes), fof_update_mask),
                    attrs = {
                    }
                )
                Console.print_verbose_info(f"                Shape: {updated_data[field].shape}")

            for field, (catalogue_field, column_indexes) in subgroup_fields_user_all.items():
                Console.print_info(f"            {field} ({catalogue_field})")
                updated_data[field] = xr.DataArray(
                    name = field,
                    dims = "snapshot_particle_index" if ((column_indexes is not None and len(column_indexes) == 1) or len(catalogue_data["Subhalo"][catalogue_field].shape) == 1) else ("snapshot_particle_index", "particle_type_number"),
                    data = insert_existing_data(field, get_catalogue_subhalo_data_by_particle(catalogue_field, indexes = column_indexes), subhalo_update_mask),
                    attrs = {
                    }
                )
                Console.print_verbose_info(f"                Shape: {updated_data[field].shape}")

            #----------------------------------------------------------|
            # Computing and writing data in paralel with dask and zarr |
            #----------------------------------------------------------|
            Console.print_info("        Computing and writing.")

            updated_data.to_zarr(f"{current_output_filepath.rsplit(".", 1)[0]}.zarr", mode = "w", group = particle_type)

#            #----------------------------------------------------------|
#            # Locate and update new values for particles in structures |
#            #----------------------------------------------------------|
#            Console.print_info("        Updating fields:")
#
#            Console.print_info("            ParticleIDs")
#            xarray_write_to_output_field(current_output_filepath, particle_type, "ParticleIDs", membership["ParticleIDs"])
#
#            Console.print_info("            GroupNumber")
#            xarray_write_to_output_field(current_output_filepath, particle_type, "GroupNumber", insert_existing_data("GroupNumber", membership["GroupNumber"], fof_update_mask, integer = True))
#            Console.print_info("            LastGroupRedshift")
#            xarray_write_to_output_field(current_output_filepath, particle_type, "LastGroupRedshift", insert_existing_data("LastGroupRedshift", snapshot_redshifts[snapshot_index], fof_update_mask, integer = False))
#            Console.print_info("            FirstSubhaloID")
#            xarray_write_to_output_field(current_output_filepath, particle_type, "FirstSubhaloID", insert_existing_data("FirstSubhaloID", central_subhalo_update_indexes, fof_update_mask, integer = True))
#
#            Console.print_info("            SubGroupNumber")
#            xarray_write_to_output_field(current_output_filepath, particle_type, "SubGroupNumber", insert_existing_data("SubGroupNumber", membership["SubGroupNumber"], subhalo_update_mask, integer = True))
#            Console.print_info("            SubhaloID")
#            xarray_write_to_output_field(current_output_filepath, particle_type, "SubhaloID", insert_existing_data("SubhaloID", subhalo_update_indexes, subhalo_update_mask, integer = True))
#            Console.print_info("            LastSubhaloRedshift")
#            xarray_write_to_output_field(current_output_filepath, particle_type, "LastSubhaloRedshift", insert_existing_data("LastSubhaloRedshift", snapshot_redshifts[snapshot_index], subhalo_update_mask, integer = False))
#
#            for field, catalogue_field in fof_group_fields.items():
#                Console.print_info(f"            {field} ({catalogue_field})")
#                xarray_write_to_output_field(current_output_filepath, particle_type, field, insert_existing_data(field, get_catalogue_fof_data_by_particle(catalogue_field), fof_update_mask, integer = not issubclass(field_datatypes[field], float)))#TODO: the integer check is broken!!!
#
#            for field, catalogue_field in subgroup_fields_user_centrals.items():
#                Console.print_info(f"            {field} ({catalogue_field})")
#                xarray_write_to_output_field(current_output_filepath, particle_type, field, insert_existing_data(field, get_catalogue_central_subhalo_data_by_particle(catalogue_field), fof_update_mask, integer = not issubclass(field_datatypes[field], float)))
#
#            for field, catalogue_field in subgroup_fields_user_all.items():
#                Console.print_info(f"            {field} ({catalogue_field})")
#                xarray_write_to_output_field(current_output_filepath, particle_type, field, insert_existing_data(field, get_catalogue_subhalo_data_by_particle(catalogue_field), subhalo_update_mask, integer = not issubclass(field_datatypes[field], float)))


#            #----------------------------------------------------------|
#            # Locate and update new values for particles in structures |
#            #----------------------------------------------------------|
#            Console.print_info("        Updating fields:")
#
#            Console.print_info("            ParticleIDs")
#            write_to_output_field(current_output_filepath, particle_type, "ParticleIDs", membership["ParticleIDs"].compute())
#
#            Console.print_info("            GroupNumber")
#            write_to_output_field(current_output_filepath, particle_type, "GroupNumber", membership["GroupNumber"][fof_update_mask].compute(), mask = fof_update_mask)
#            Console.print_info("            LastGroupRedshift")
#            write_to_output_field(current_output_filepath, particle_type, "LastGroupRedshift", snapshot_redshifts[snapshot_index], mask = fof_update_mask)
#            Console.print_info("            FirstSubhaloID")
#            write_to_output_field(current_output_filepath, particle_type, "FirstSubhaloID", central_subhalo_update_indexes, mask = fof_update_mask)
#
#            Console.print_info("            SubGroupNumber")
#            write_to_output_field(current_output_filepath, particle_type, "SubGroupNumber", membership["SubGroupNumber"][subhalo_update_mask].compute(), mask = subhalo_update_mask)
#            Console.print_info("            SubhaloID")
#            write_to_output_field(current_output_filepath, particle_type, "SubhaloID", subhalo_update_indexes.compute(), mask = subhalo_update_mask)
#            Console.print_info("            LastSubhaloRedshift")
#            write_to_output_field(current_output_filepath, particle_type, "LastSubhaloRedshift", snapshot_redshifts[snapshot_index], mask = subhalo_update_mask)
#
#            for field, catalogue_field in fof_group_fields.items():
#                Console.print_info(f"            {field}")
#                write_to_output_field(current_output_filepath, particle_type, field, catalogue_data["FOF"][catalogue_field][halo_update_indexes].compute(), mask = fof_update_mask)
#
#            for field, catalogue_field in subgroup_fields_user_centrals.items():
#                Console.print_info(f"            {field}")
#                write_to_output_field(current_output_filepath, particle_type, field, catalogue_data["Subhalo"][catalogue_field][central_subhalo_update_indexes].compute(), mask = fof_update_mask)
#
#            for field, catalogue_field in subgroup_fields_user_all.items():
#                Console.print_info(f"            {field}")
#                write_to_output_field(current_output_filepath, particle_type, field, catalogue_data["Subhalo"][catalogue_field][subhalo_update_indexes].compute(), mask = subhalo_update_mask)

        #-----------------------|
        # Clear catalogue cache |
        #-----------------------|
        if not settings.keep_catalogue_caches:
            Console.print_info("    Clearing catalogue cache.")

            clear_catalogue_cache(os.path.join(args.output_directory, "tmp"), tag.tag)

        #-----------------------|
        # Clear catalogue cache |
        #-----------------------|
        if not settings.keep_data_propagation_caches:
            Console.print_info("    Clearing propogation cache.")

            if reorder_cache_filepath is not None:
                if os.path.exists(reorder_cache_filepath):
                    os.remove(reorder_cache_filepath)#TODO: this will fail as zarr uses directories of files instead of a single file
                reorder_cache_filepath = None

        #----------------------------------------|
        # Store filepath ready for next snapshot |
        #----------------------------------------|

        previous_filepath = current_output_filepath


    #---------------------------------------------------|
    # Close the dask cluster (also keeps them in scope) |
    #---------------------------------------------------|
    if settings.dask_workers > 0:
        Console.print_verbose_info("Closing dask client.")
        client.close()
        Console.print_verbose_info("Closing dask cluster.")
        cluster.close()

    Console.print_info("DONE", flush = True)
    return



if __name__ == "__main__":
    main()
