# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later


import argparse
import errno
import os

import dask
from dask import delayed, compute
from dask.distributed import LocalCluster
import xarray as xr
import dask.array as dask_array
from dask.distributed import LocalCluster
from dask.utils import SerializableLock
import numpy as np
import h5py as h5
from QuasarCode import Console

from eagle_tag import EAGLE_Files, EAGLE_Snapshot, SnapshotTag, load_snapshot, calculate_reorder



NULL_INDEX = 2**30 # Used where an integer index needs to be NULL

DASK_WORKERS = 64
DASK_MEMORY_LIMIT_PER_WORKER = 8 # GB
DASK_PORT = 8787



def make_file_path(directory: str, source: SnapshotTag, target: SnapshotTag) -> str:
    return os.path.join(directory, f"reorder_from_{source.tag}_to_{target.tag}.hdf5")

def make_reorder_file(
    filepath: str,
    number_of_particles_per_file__source: np.ndarray[tuple[int, int], np.dtype[np.int64]],
    number_of_particles_per_file__target: np.ndarray[tuple[int, int], np.dtype[np.int64]],
    number_of_gas: int|None = None, number_of_dark_matter: int|None = None, number_of_stars: int|None = None, number_of_black_holes: int|None = None,
    overwrite: bool = False, update: bool = False
) -> None:
    file_exists: bool = os.path.exists(filepath)
    if file_exists and not (overwrite or update):
        raise FileExistsError(errno.EEXIST, f"Reorder file {filepath} already exists. Use --overwrite to overwrite or --update to update the existing file.", filepath)
    
    if file_exists and update:

        # Check that the particle types indicated do not yet exist
        with h5.File(filepath, "r") as file:
            if number_of_gas is not None and "PartType0" in file:
                raise ValueError(f"Reorder file {filepath} already contains gas particles (PartType0).")
            if number_of_dark_matter is not None and "PartType1" in file:
                raise ValueError(f"Reorder file {filepath} already contains dark matter particles (PartType1).")
            if number_of_stars is not None and "PartType4" in file:
                raise ValueError(f"Reorder file {filepath} already contains star particles (PartType4).")
            if number_of_black_holes is not None and "PartType5" in file:
                raise ValueError(f"Reorder file {filepath} already contains black hole particles (PartType5).")
        
    number_of_particles__source: np.ndarray[tuple[int], np.dtype[np.int64]] = number_of_particles_per_file__source.sum(axis = 0)
    number_of_particles__target: np.ndarray[tuple[int], np.dtype[np.int64]] = number_of_particles_per_file__target.sum(axis = 0)

    with h5.File(filepath, "a" if file_exists and update else "w") as file:
        header:      h5.Group
        gas:         h5.Group
        dark_matter: h5.Group
        stars:       h5.Group
        black_holes: h5.Group

        if not (file_exists and update):
            # No need to re-create the header

            header = file.create_group("Header")

            header.attrs["NumPart_Total_Source"] = number_of_particles__source
            header.attrs["NumPart_Total_Target"] = number_of_particles__target

            header.create_dataset(name = "NumPart_PerFile_Source", data = number_of_particles_per_file__source)
            header.create_dataset(name = "NumPart_PerFile_Target", data = number_of_particles_per_file__target)

        if number_of_gas is not None:

            gas = file.create_group("PartType0")

            gas_forwards  = gas.create_dataset("ForwardsIndexes",  shape = (number_of_particles__target[0],), dtype = np.int64)
            gas_backwards = gas.create_dataset("BackwardsIndexes", shape = (number_of_particles__source[0],), dtype = np.int64)

            gas_forwards.attrs["CGSConversionFactor"] = np.float64(1.0)
            gas_forwards.attrs["aexp-scale-exponent"] = np.float64(0.0)
            gas_forwards.attrs["h-scale-exponent"]    = np.float64(0.0)

            gas_backwards.attrs["CGSConversionFactor"] = np.float64(1.0)
            gas_backwards.attrs["aexp-scale-exponent"] = np.float64(0.0)
            gas_backwards.attrs["h-scale-exponent"]    = np.float64(0.0)

        if number_of_dark_matter is not None:

            dark_matter = file.create_group("PartType1")

            dark_matter_forwards  = dark_matter.create_dataset("ForwardsIndexes",  shape = (number_of_particles__target[1],), dtype = np.int64)
            dark_matter_backwards = dark_matter.create_dataset("BackwardsIndexes", shape = (number_of_particles__source[1],), dtype = np.int64)

            dark_matter_forwards.attrs["CGSConversionFactor"] = np.float64(1.0)
            dark_matter_forwards.attrs["aexp-scale-exponent"] = np.float64(0.0)
            dark_matter_forwards.attrs["h-scale-exponent"]    = np.float64(0.0)

            dark_matter_backwards.attrs["CGSConversionFactor"] = np.float64(1.0)
            dark_matter_backwards.attrs["aexp-scale-exponent"] = np.float64(0.0)
            dark_matter_backwards.attrs["h-scale-exponent"]    = np.float64(0.0)

        if number_of_stars is not None:

            stars = file.create_group("PartType4")

            stars_forwards  = stars.create_dataset("ForwardsIndexes",  shape = (number_of_particles__target[4],), dtype = np.int64)
            stars_backwards = stars.create_dataset("BackwardsIndexes", shape = (number_of_particles__source[4],), dtype = np.int64)

            stars_forwards.attrs["CGSConversionFactor"] = np.float64(1.0)
            stars_forwards.attrs["aexp-scale-exponent"] = np.float64(0.0)
            stars_forwards.attrs["h-scale-exponent"]    = np.float64(0.0)

            stars_backwards.attrs["CGSConversionFactor"] = np.float64(1.0)
            stars_backwards.attrs["aexp-scale-exponent"] = np.float64(0.0)
            stars_backwards.attrs["h-scale-exponent"]    = np.float64(0.0)

        if number_of_black_holes is not None:

            black_holes = file.create_group("PartType5")

            black_holes_forwards  = black_holes.create_dataset("ForwardsIndexes",  shape = (number_of_particles__target[5],), dtype = np.int64)
            black_holes_backwards = black_holes.create_dataset("BackwardsIndexes", shape = (number_of_particles__source[5],), dtype = np.int64)

            black_holes_forwards.attrs["CGSConversionFactor"] = np.float64(1.0)
            black_holes_forwards.attrs["aexp-scale-exponent"] = np.float64(0.0)
            black_holes_forwards.attrs["h-scale-exponent"]    = np.float64(0.0)

            black_holes_backwards.attrs["CGSConversionFactor"] = np.float64(1.0)
            black_holes_backwards.attrs["aexp-scale-exponent"] = np.float64(0.0)
            black_holes_backwards.attrs["h-scale-exponent"]    = np.float64(0.0)

def save_forwards_data(filepath: str, particle_type: str, forwards_indexes: np.ndarray) -> None:

    with h5.File(filepath, "a") as file:

        if particle_type not in file:
            raise ValueError(f"{particle_type} does not exist in file {filepath}.")

        file[particle_type]["ForwardsIndexes" ][:] = forwards_indexes

def save_backwards_data(filepath: str, particle_type: str, backwards_indexes: np.ndarray) -> None:

    with h5.File(filepath, "a") as file:

        if particle_type not in file:
            raise ValueError(f"{particle_type} does not exist in file {filepath}.")

        file[particle_type]["BackwardsIndexes"][:] = backwards_indexes



def main():
    print(
"""
--|| EAGLE-tag (reorder calculation) ||--

Calculates the indexing order to move from one set of particle IDs to another.
""",
    flush = True)

    Console.show_times()
    Console.reset_stopwatch()

    #------------------------------|
    # Parse command line arguments |
    #------------------------------|
    Console.print_info("Parsing command line arguments.", flush = True)

    parser = argparse.ArgumentParser(description = "Run EAGLE halo membership tagging.")

    parser.add_argument("simulation_directory", type = str,                help = "Directory containing the EAGLE simulation data.")
    parser.add_argument("snapshot_tag_source",  type = str,                help = "Snapshot number (e.g. \"012\").")
    parser.add_argument("snapshot_tag_target",  type = str,                help = "Snapshot redshift tag (e.g. \"z012p345\").")
    parser.add_argument("output_directory",     type = str, default = ".", help = "Alternate directory in which to create the output file.")
    parser.add_argument("--source-is-snipshot", action  = "store_true",    help = "Source is a snapshot.")
    parser.add_argument("--target-is-snipshot", action  = "store_true",    help = "target is a snapshot.")
    parser.add_argument("--snipshots",          action  = "store_true",    help = "Both source and target files are snipshots. This is the same as specifying both --source-is-snipshot and --target-is-snipshot.")
    parser.add_argument("--overwrite",          action  = "store_true",    help = "Overwrite existing output files.")
    parser.add_argument("--update",             action  = "store_true",    help = "Allow the use of an existing output file.")
    parser.add_argument("--gas",                action  = "store_true",    help = "Include gas particles.")
    parser.add_argument("--darkmatter",         action  = "store_true",    help = "Include dark matter particles.")
    parser.add_argument("--stars",              action  = "store_true",    help = "Include star particles.")
    parser.add_argument("--blackholes",         action  = "store_true",    help = "Include black hole particles.")
    parser.add_argument("--verbose",            action  = "store_true",    help = "Display extra information.")

    # This will exit the program if -h or --help are specified
    args = parser.parse_args()

    Console.print_info("Arguments:", flush = True)
    for key in args.__dict__:
        Console.print_info(f"    {key}: {getattr(args, key)}", flush = True)

    #----------------------------------|
    # Check for a valid set of options |
    #----------------------------------|

    if not (args.gas or args.darkmatter or args.stars or args.blackholes):
        raise ValueError("At least one of --gas, --dark_matter, --stars or --black_holes must be specified.")
    
    if args.update and args.overwrite:
        raise ValueError("--update and --overwrite are mutually exclusive.")

    #---------------------------------|
    # Create directory and file paths |
    #---------------------------------|
    Console.print_info("Creating directory paths.", flush = True)

    files = EAGLE_Files(directory = args.simulation_directory)
    tag__source = SnapshotTag.from_string(args.snapshot_tag_source)
    tag__target = SnapshotTag.from_string(args.snapshot_tag_target)
    snapshot_files__source: EAGLE_Snapshot = files.snapshot(tag = tag__source, snipshot = args.snipshots or args.source_is_snipshot)
    snapshot_files__target: EAGLE_Snapshot = files.snapshot(tag = tag__target, snipshot = args.snipshots or args.target_is_snipshot)

#    #--------------------|
#    # Start dask cluster |
#    #--------------------|
#    Console.print_info("Creating directory paths.", flush = True)
#
#    cluster = LocalCluster(
#        n_workers = DASK_WORKERS,
#        memory_limit = DASK_MEMORY_LIMIT_PER_WORKER,
#        dashboard_address = f":{DASK_PORT}"
#    )
#    client = cluster.get_client()

    #----------------|
    # Load snapshots |
    #----------------|
    Console.print_info("Loading snapshots.", flush = True)

    # xarray is used to load EAGLE data in a delayed fashion using dask.
    # This will not actually 'load' the data - just the structure and some metadata.
    # Data will be loaded from disk only when it is actually needed.

    snapshot__source = load_snapshot(snapshot_files__source)
    Console.print_info(snapshot__source, flush = True)
    snapshot__target = load_snapshot(snapshot_files__target)
    Console.print_info(snapshot__target, flush = True)

    n_total_gas__source   = int(snapshot__source["PartType0"]["ParticleIDs"].shape[0]) if snapshot__source["PartType0"] is not None else 0
    n_total_dm__source    = int(snapshot__source["PartType1"]["ParticleIDs"].shape[0]) if snapshot__source["PartType1"] is not None else 0
    n_total_stars__source = int(snapshot__source["PartType4"]["ParticleIDs"].shape[0]) if snapshot__source["PartType4"] is not None else 0
    n_total_bh__source    = int(snapshot__source["PartType5"]["ParticleIDs"].shape[0]) if snapshot__source["PartType5"] is not None else 0
    snapshot_particle_numbers__source = { "PartType0" : n_total_gas__source, "PartType1" : n_total_dm__source, "PartType4" : n_total_stars__source, "PartType5" : n_total_bh__source }

    n_total_gas__target   = int(snapshot__target["PartType0"]["ParticleIDs"].shape[0]) if snapshot__target["PartType0"] is not None else 0
    n_total_dm__target    = int(snapshot__target["PartType1"]["ParticleIDs"].shape[0]) if snapshot__target["PartType1"] is not None else 0
    n_total_stars__target = int(snapshot__target["PartType4"]["ParticleIDs"].shape[0]) if snapshot__target["PartType4"] is not None else 0
    n_total_bh__target    = int(snapshot__target["PartType5"]["ParticleIDs"].shape[0]) if snapshot__target["PartType5"] is not None else 0
    snapshot_particle_numbers__target = { "PartType0" : n_total_gas__target, "PartType1" : n_total_dm__target, "PartType4" : n_total_stars__target, "PartType5" : n_total_bh__target }

    Console.print_info(f"Number of gas particles:         {n_total_gas__source:11.0f} -> {n_total_gas__target:11.0f}",     flush = True)
    Console.print_info(f"Number of dark matter particles: {n_total_dm__source:11.0f} -> {n_total_dm__target:11.0f}",       flush = True)
    Console.print_info(f"Number of star particles:        {n_total_stars__source:11.0f} -> {n_total_stars__target:11.0f}", flush = True)
    Console.print_info(f"Number of black hole particles:  {n_total_bh__source:11.0f} -> {n_total_bh__target:11.0f}",       flush = True)

    #---------------------------------|
    # Get snapshot chunk file lengths |
    #---------------------------------|
    Console.print_info("Getting snapshot partitioning chunk sizes:", flush = True)

    # Source
    Console.print_info("    Source.", flush = True)

    number_of_files__source: int
    with h5.File(snapshot_files__source.snapshot_file_template.format(0), "r") as file:
        number_of_files__source = int(file["Header"].attrs["NumFilesPerSnapshot"])

    data_in_files__source: np.ndarray[tuple[int, int], np.dtype[np.int64]]
    data_in_files__source = np.full(shape = (number_of_files__source, 6), fill_value = 0, dtype = np.int64)
    for i in range(number_of_files__source):
        with h5.File(snapshot_files__source.snapshot_file_template.format(i), "r") as file:
            data_in_files__source[i][:] = file["Header"].attrs["NumPart_ThisFile"]

    # Target
    Console.print_info("    Target.", flush = True)

    number_of_files__target: int
    with h5.File(snapshot_files__target.snapshot_file_template.format(0), "r") as file:
        number_of_files__target = int(file["Header"].attrs["NumFilesPerSnapshot"])

    data_in_files__target: np.ndarray[tuple[int, int], np.dtype[np.int64]]
    data_in_files__target = np.full(shape = (number_of_files__target, 6), fill_value = 0, dtype = np.int64)
    for i in range(number_of_files__target):
        with h5.File(snapshot_files__target.snapshot_file_template.format(i), "r") as file:
            data_in_files__target[i][:] = file["Header"].attrs["NumPart_ThisFile"]

    #------------------------|
    # Create the output file |
    #------------------------|
    Console.print_info("Preparing output file.", flush = True)

    output_filepath: str = make_file_path(args.output_directory, tag__source, tag__target)

    Console.print_info(f"Output file: {output_filepath}", flush = True)

    make_reorder_file(
        filepath = output_filepath,
        number_of_particles_per_file__source = data_in_files__source,
        number_of_particles_per_file__target = data_in_files__target,
        number_of_gas = n_total_gas__source if args.gas else None,
        number_of_dark_matter = n_total_dm__source if args.darkmatter else None,
        number_of_stars = n_total_stars__source if args.stars else None,
        number_of_black_holes = n_total_bh__source if args.blackholes else None,
        overwrite = args.overwrite,
        update = args.update
    )

    #--------------------------|
    # Loop over particle types |
    #--------------------------|

    for particle_type in ["PartType0", "PartType1", "PartType4", "PartType5"]:

        #-----------------------------------|
        # Skip particle types not requested |
        #-----------------------------------|

        if particle_type == "PartType0" and not args.gas:
            continue
        if particle_type == "PartType1" and not args.darkmatter:
            continue
        if particle_type == "PartType4" and not args.stars:
            continue
        if particle_type == "PartType5" and not args.blackholes:
            continue

        Console.print_info(f"Doing {particle_type}:", flush = True)

        if snapshot__source[particle_type] is None or snapshot__source[particle_type]["ParticleIDs"].shape[0] == 0:
            Console.print_info(f"    No {particle_type} particles in source snapshot. Skipping.", flush = True)
            continue
        if snapshot__target[particle_type] is None or snapshot__target[particle_type]["ParticleIDs"].shape[0] == 0:
            Console.print_info(f"    No {particle_type} particles in target snapshot. Skipping.", flush = True)
            continue

        #-----------|
        # Load data |
        #-----------|

        Console.print_info(f"    Loading source IDs.", flush = True)
        particle_ids__source = snapshot__source[particle_type]["ParticleIDs"].values

        Console.print_info(f"    Loading target IDs.", flush = True)
        particle_ids__target = snapshot__target[particle_type]["ParticleIDs"].values

        #------------------------------|
        # Calculate reorder - forwards |
        #------------------------------|
        Console.print_info(f"    Calculating reorder.", flush = True)
    
        forwards_indexes = calculate_reorder(particle_ids__source, particle_ids__target)

        #----------------------|
        # Save data - forwards |
        #----------------------|
        Console.print_info(f"    Saving data.", flush = True)
    
        save_forwards_data(output_filepath, particle_type, forwards_indexes)

        #-------------------------------|
        # Calculate reorder - backwards |
        #-------------------------------|
        Console.print_info(f"    Calculating reverse reorder.", flush = True)
    
        backwards_indexes = calculate_reorder(particle_ids__target, particle_ids__source)

        #-----------------------|
        # Save data - backwards |
        #-----------------------|
        Console.print_info(f"    Saving reverse data.", flush = True)
    
        save_backwards_data(output_filepath, particle_type, backwards_indexes)

        Console.print_info(f"    Done.", flush = True)

    Console.print_info("DONE", flush = True)
    return



if __name__ == "__main__":
    main()
