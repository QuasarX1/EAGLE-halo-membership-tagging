# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import errno
import os
import socket
import sys

import dask
from dask import delayed, compute
from dask.distributed import LocalCluster
import xarray as xr
import dask.array as dask_array
from dask.utils import SerializableLock
import numpy as np
import h5py as h5
from QuasarCode import Console, Settings, Stopwatch
from QuasarCode.IO.Configurations import ConfigsBase, YamlConfig

from eagle_tag import EAGLE_Files, EAGLE_Snapshot, SnapshotTag, load_catalogue, load_hdf5_files_with_xarray, make_aux_file_path
from ._calculate_reorder import make_file_path as make_reorder_file_path



NULL_INDEX = 2**30 # Used where an integer index needs to be NULL



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

dask_workers: null           # Number of parallel workers to use, 0 will avoid instantiation of a dask cluster
dask_memory_per_worker: null # in GB
dask_port: 8787              # Port number or null to disable the dask dashboard

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

            for field in field_widths:
                Console.print_debug(f"Making dataset {field} ({(group_length, field_widths[field])}, {field_datatypes[field]}).")
                d = group.create_dataset(
                    field,
                    shape = (group_length, field_widths[field]) if field_widths[field] > 1 else (group_length,), dtype = field_datatypes[field],
                    fillvalue = None if not pre_initialise_with_null else np.nan if issubclass(field_datatypes[field], float) else NULL_INDEX,
                    chunks = ((1024 * 8, field_widths[field]) if field_widths[field] > 1 else 1024 * 8) if group_length > 1024 * 8 else True, # alternatively, use: True -> Auto
                    compression = "gzip",
                    compression_opts = 8,
                    shuffle = True,
                    fletcher32 = True
                )

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
        "PartType0" : load_hdf5_files_with_xarray([filepath], "PartType0", datasets) if has_gas         else None,
        "PartType1" : load_hdf5_files_with_xarray([filepath], "PartType1", datasets) if has_dark_matter else None,
        "PartType4" : load_hdf5_files_with_xarray([filepath], "PartType4", datasets) if has_stars       else None,
        "PartType5" : load_hdf5_files_with_xarray([filepath], "PartType5", datasets) if has_black_holes else None,
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

#TODO: DATA MUST BE UNMASKED!!!!!!!!
def xarray_write_to_output_field(filepath: str, particle_type: str, field: str, data: xr.DataArray, mask: slice|np.ndarray[tuple[int], np.dtype[np.bool_]] = slice(None)) -> None:
    existing_data = load_hdf5_files_with_xarray([filepath], particle_type, [field])[field]
    updated_data = xr.where(mask, data, existing_data)
    updated_data.to_netcdf(
        filepath,
        mode = "a",
        engine = "h5netcdf",
        group = particle_type
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

    #---------------------------------|
    # Compile target catalogue fields |
    #---------------------------------|
    Console.print_verbose_info("Processing requested fields.")

    fof_group_fields_default: dict[str, str] = {
        "HaloMass"     : "GroupMass",
        "HaloM200Crit" : "Group_M_Crit200",
    }
    fof_group_fields_user: dict[str, str] = { field : settings.group[field] for field in settings.group.keys if field not in fof_group_fields_default }
    fof_group_fields: dict[str, str] = fof_group_fields_default | fof_group_fields_user
    fof_group_field_names_for_reading: tuple[str, ...] = tuple(list(fof_group_fields.values()) + ["FirstSubhaloID"]) # FirstSubhaloID is needed to calculate the true index for subhaloes

    subgroup_fields_user_all: dict[str, str] = { field : settings.subhalo[field] for field in settings.subhalo.keys }
    subgroup_fields_user_centrals: dict[str, str] = { field : settings.central[field] for field in settings.central.keys }
    subgroup_field_names_for_reading: tuple[str, ...] = tuple(set(subgroup_fields_user_all.values()) | set(subgroup_fields_user_centrals.values()))
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


    if settings.dask_workers > 0:
        #--------------------|
        # Start dask cluster |
        #--------------------|
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

        previous_filepath: str|None = None
        if snapshot_index == target_tags_start_index and snapshot_index > 0:
            restart_file_path = make_file_path(args.output_directory, target_tags[snapshot_index - 1])
            if os.path.exists(restart_file_path):
                previous_filepath = restart_file_path
            else:
                Console.print_warning("Start tag is not the first target and no output file found to restart from.\nIf that is intentional then this warning may be safely ignored.")

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
            for output_name, catalogue_field in fof_group_fields_user.items():
                field_widths[output_name]    = file["FOF"][catalogue_field].shape[-1] if len(file["FOF"][catalogue_field].shape) > 1 else 1
                field_datatypes[output_name] = file["FOF"][catalogue_field].dtype.type
            for output_name, catalogue_field in subgroup_fields_user_all.items():
                field_widths[output_name]    = file["Subhalo"][catalogue_field].shape[-1] if len(file["Subhalo"][catalogue_field].shape) > 1 else 1
                field_datatypes[output_name] = file["Subhalo"][catalogue_field].dtype.type
            for output_name, catalogue_field in subgroup_fields_user_centrals.items():
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

        if snapshot_index > 0 and previous_filepath is not None:

            #-----------------------|
            # Load previous outputs |
            #-----------------------|
            Console.print_info("    Loading previous results.")

            last_snapshot_output = load_output_file(
                previous_filepath,
                "GroupNumber", "FirstSubhaloID", "SubGroupNumber", "SubhaloID", "LastGroupRedshift", "LastSubhaloRedshift",
                *fof_group_fields.keys(),
                *subgroup_fields_user_centrals.keys(),
                *subgroup_fields_user_all.keys()
            )

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
                #override_chunks_in_all_dimensions = 100#1024 * 8
            )

            Console.print_info("        Computing membership masks.")

#            Console.print_verbose_info("            Initial FOF mask.")
#            #fof_update_mask: np.ndarray[tuple[int], np.dtype[np.bool_]] = (membership["GroupNumber"] != NULL_INDEX).compute() # This is a boolean mask where True means the particle is in a FOF group
#            fof_update_mask: np.ndarray[tuple[int], np.dtype[np.bool_]] = xr.where(membership["GroupNumber"] != NULL_INDEX, True, False).compute() # This is a boolean mask where True means the particle is in a FOF group
#            Console.print_verbose_info("            Updated FOF mask - to remove group with no subhaloes.")
#            #fof_update_mask[fof_update_mask] = (catalogue_data["FOF"]["NumOfSubhalos"][membership["GroupNumber"][fof_update_mask] - 1] > 0).compute()
#
#            locations_to_read = membership["GroupNumber"][fof_update_mask] - 1
#            subhalo_counts = catalogue_data["FOF"]["NumOfSubhalos"][locations_to_read]
#            subhalos_mask = subhalo_counts > 0
#            fof_update_mask[fof_update_mask] = xr.where(subhalos_mask, True, False).compute()

            Console.print_verbose_info("            FOF mask.")
            groups_with_subgroups = catalogue_data["FOF"]["NumOfSubhalos"][membership["GroupNumber"] - 1] > 0
            fof_update_mask = xr.where(membership["GroupNumber"] != NULL_INDEX, groups_with_subgroups, False).compute()

            Console.print_verbose_info("            Inverse FOF.")
            fof_update_mask_inverse = ~ fof_update_mask
            Console.print_verbose_info("            Subhalo mask.")
            #subhalo_update_mask: np.ndarray[tuple[int], np.dtype[np.bool_]] = (membership["SubGroupNumber"] != NULL_INDEX).compute() # This is a boolean mask where True means the particle is in a subhalo
            subhalo_update_mask: np.ndarray[tuple[int], np.dtype[np.bool_]] = xr.where(membership["SubGroupNumber"] != NULL_INDEX, True, False).compute() # This is a boolean mask where True means the particle is in a subhalo
            Console.print_verbose_info("            Inverse Subhalo.")
            subhalo_update_mask_inverse = ~ subhalo_update_mask

            Console.print_verbose_info("        Counting number of particles in structures.")
            with h5.File(current_output_filepath, "a") as file:
                file[particle_type].attrs["NumberInGroups"] = np.sum(fof_update_mask)
                file[particle_type].attrs["NumberInSubhaloes"] = np.sum(subhalo_update_mask)
                Console.print_debug(f"        FOF: {file[particle_type].attrs["NumberInGroups"]}, Subhalo: {file[particle_type].attrs["NumberInSubhaloes"]}")

            #-------------------------|
            # Propagate existing data |
            #-------------------------|

            if snapshot_index > 0 and previous_filepath is not None:

                #--------------------------|
                # Load reordering sequence |
                #--------------------------|
                Console.print_info("        Loading reordering indexes.")

                reorder_data = load_hdf5_files_with_xarray(
                    [make_reorder_file_path(settings.reorder_indexes_directory, target_tags[snapshot_index - 1], tag)],
                    particle_type,
                    ["ForwardsIndexes"],
                    override_chunks_in_all_dimensions = 1024 * 8
                )

                reorder_indexes = reorder_data[particle_type]["ForwardsIndexes"][fof_update_mask_inverse]#TODO: this will fail later as there are other update masks and its getting masked twice!!!

                #-------------------------|
                # Propagate existing data |
                #-------------------------|
                Console.print_info("        Propagating existing data.")

                for field in ("GroupNumber", "LastGroupRedshift"):
                    # These fields are not read from catalogue data or are computed in some way

                    #-----------------------|
                    # Reorder existing data |
                    #-----------------------|
                    Console.print_verbose_info(f"            {field}")

                    write_to_output_field(current_output_filepath, particle_type, field, last_snapshot_output[particle_type][field][reorder_indexes[fof_update_mask_inverse]].compute(), mask = fof_update_mask_inverse)

                for field in ("FirstSubhaloID", "SubGroupNumber", "SubhaloID", "LastSubhaloRedshift"):
                    # These fields are not read from catalogue data or are computed in some way

                    #-----------------------|
                    # Reorder existing data |
                    #-----------------------|
                    Console.print_verbose_info(f"            {field}")

                    write_to_output_field(current_output_filepath, particle_type, field, last_snapshot_output[particle_type][field][reorder_indexes[subhalo_update_mask_inverse]].compute(), mask = subhalo_update_mask_inverse)

                for field in list(fof_group_fields.keys()) + list(subgroup_fields_user_centrals.keys()):
                    # These are raw data fields

                    #-----------------------|
                    # Reorder existing data |
                    #-----------------------|
                    Console.print_verbose_info(f"            {field}")

                    write_to_output_field(current_output_filepath, particle_type, field, last_snapshot_output[particle_type][field][reorder_indexes[fof_update_mask_inverse]].compute(), mask = fof_update_mask_inverse)

                for field in list(subgroup_fields_user_all.keys()):
                    # These are raw data fields

                    #-----------------------|
                    # Reorder existing data |
                    #-----------------------|
                    Console.print_verbose_info(f"            {field}")

                    write_to_output_field(current_output_filepath, particle_type, field, last_snapshot_output[particle_type][field][reorder_indexes[subhalo_update_mask_inverse]].compute(), mask = subhalo_update_mask_inverse)

            else:
                Console.print_debug(f"        snapshot_index={snapshot_index}, previous_filepath={previous_filepath}.")
                Console.print_info("        No previous data to propagate.")

            #-------------------------|
            # Compute subhalo indexes |
            #-------------------------|
            Console.print_info("        Calculating global catalogue indexes for FOF and Subhalo datasets.")

            Console.print_verbose_info("            FOF indexes from GroupNumber.")
            halo_update_indexes = membership["GroupNumber"][fof_update_mask] - 1
            Console.print_verbose_info("            Subhalo indexes for centrals.")
            central_subhalo_update_indexes = catalogue_data["FOF"]["FirstSubhaloID"][halo_update_indexes].compute()
            Console.print_verbose_info("            Subhalo indexes from FirstSubhaloID and SubGroupNumber.")
            subhalo_update_indexes = catalogue_data["FOF"]["FirstSubhaloID"][membership["GroupNumber"][subhalo_update_mask]] + membership["SubGroupNumber"][subhalo_update_mask]

            #----------------------------------------------------------|
            # Locate and update new values for particles in structures |
            #----------------------------------------------------------|
            Console.print_info("        Updating fields:")

            Console.print_info("            ParticleIDs")
            write_to_output_field(current_output_filepath, particle_type, "ParticleIDs", membership["ParticleIDs"].compute())

            Console.print_info("            GroupNumber")
            write_to_output_field(current_output_filepath, particle_type, "GroupNumber", membership["GroupNumber"][fof_update_mask].compute(), mask = fof_update_mask)
            Console.print_info("            LastGroupRedshift")
            write_to_output_field(current_output_filepath, particle_type, "LastGroupRedshift", snapshot_redshifts[snapshot_index], mask = fof_update_mask)
            Console.print_info("            FirstSubhaloID")
            write_to_output_field(current_output_filepath, particle_type, "FirstSubhaloID", central_subhalo_update_indexes, mask = fof_update_mask)

            Console.print_info("            SubGroupNumber")
            write_to_output_field(current_output_filepath, particle_type, "SubGroupNumber", membership["SubGroupNumber"][subhalo_update_mask].compute(), mask = subhalo_update_mask)
            Console.print_info("            SubhaloID")
            write_to_output_field(current_output_filepath, particle_type, "SubhaloID", subhalo_update_indexes.compute(), mask = subhalo_update_mask)
            Console.print_info("            LastSubhaloRedshift")
            write_to_output_field(current_output_filepath, particle_type, "LastSubhaloRedshift", snapshot_redshifts[snapshot_index], mask = subhalo_update_mask)

            for field, catalogue_field in fof_group_fields.items():
                Console.print_info(f"            {field}")
                write_to_output_field(current_output_filepath, particle_type, field, catalogue_data["FOF"][catalogue_field][halo_update_indexes].compute(), mask = fof_update_mask)

            for field, catalogue_field in subgroup_fields_user_centrals.items():
                Console.print_info(f"            {field}")
                write_to_output_field(current_output_filepath, particle_type, field, catalogue_data["Subhalo"][catalogue_field][central_subhalo_update_indexes].compute(), mask = fof_update_mask)

            for field, catalogue_field in subgroup_fields_user_all.items():
                Console.print_info(f"            {field}")
                write_to_output_field(current_output_filepath, particle_type, field, catalogue_data["Subhalo"][catalogue_field][subhalo_update_indexes].compute(), mask = subhalo_update_mask)

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
