# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later


import argparse
import os

import dask
from dask import delayed, compute
from dask.distributed import LocalCluster
import xarray as xr
import dask.array as dask_array
from dask.utils import SerializableLock
import numpy as np
import h5py as h5
from QuasarCode import Console

from eagle_tag import Metadata, load_snapshot, load_catalogue_membership, make_aux_file, save_chunk



NULL_INDEX = 2**30 # Used where an integer index needs to be NULL

DASK_WORKERS = 64
DASK_MEMORY_LIMIT_PER_WORKER = 8 # GB
DASK_PORT = 8787



def main():
    print(
"""
--|| EAGLE-tag ||--

Creates snapshot-length files with the catalogue membership
information for FOF groups and SUBFIND haloes.
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
    parser.add_argument("snapshot_number",      type = str,                help = "Snapshot number (e.g. \"012\").")
    parser.add_argument("snapshot_tag",         type = str,                help = "Snapshot redshift tag (e.g. \"z012p345\").")
    parser.add_argument("chunks",               type = int,                help = "Number of chunks to divide the snapshot data into.")
    parser.add_argument("catalogue_chunks",     type = int,                help = "Number of chunks to divide the catalogue data into. Can usually be set to 1 for all but the largest datasets.")
    #parser.add_argument("output_directory",     required = False, default = ".", type = str, help = "Alternate directory in which to create the output file.")
    parser.add_argument("output_directory",     type = str, default = ".", help = "Alternate directory in which to create the output file.")
    parser.add_argument("--snipshot",           action  = "store_true",    help = "Target a snipshot.")
    parser.add_argument("--overwrite",          action  = "store_true",    help = "Overwrite existing output files.")
    parser.add_argument("--update",             action  = "store_true",    help = "Allow the use of an existing output file.")
    parser.add_argument("--gas",                action  = "store_true",    help = "Include gas particles.")
    parser.add_argument("--darkmatter",        action  = "store_true",    help = "Include dark matter particles.")
    parser.add_argument("--stars",              action  = "store_true",    help = "Include star particles.")
    parser.add_argument("--blackholes",        action  = "store_true",    help = "Include black hole particles.")
    parser.add_argument("--verbose",            action  = "store_true",    help = "Display extra information.")

    # This will exit the program if -h or --help are specified
    args = parser.parse_args()

    Console.print_info(f"Arguments: {args}", flush = True)

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

    snapshot_directory = os.path.join(args.simulation_directory, f"sn{'i' if args.snipshot else 'a'}pshot_{args.snapshot_number}_{args.snapshot_tag}")
    #catalogue_data_directory = os.path.join(args.simulation_directory, f"groups_{'snip_' if args.snipshot else ''}{args.snapshot_number}_{args.snapshot_tag}")
    catalogue_membership_directory = os.path.join(args.simulation_directory, f"particledata_{'snip_' if args.snipshot else ''}{args.snapshot_number}_{args.snapshot_tag}")

    # We need to get these manually so that we can load metadata with h5py
    snapshot_first_file_path = os.path.join(snapshot_directory, f"sn{'i' if args.snipshot else 'a'}p_{args.snapshot_number}_{args.snapshot_tag}.0.hdf5")
    catalogue_membership_first_file_path = os.path.join(catalogue_membership_directory, f"eagle_subfind_{'snip_' if args.snipshot else ''}particles_{args.snapshot_number}_{args.snapshot_tag}.0.hdf5")

    #--------------------|
    # Start dask cluster |
    #--------------------|
    Console.print_info("Creating directory paths.", flush = True)

    cluster = LocalCluster(
        n_workers = DASK_WORKERS,
        memory_limit = DASK_MEMORY_LIMIT_PER_WORKER,
        dashboard_address = f":{DASK_PORT}"
    )
    client = cluster.get_client()

    #----------------------------------------|
    # Load snapshot and catalogue membership |
    #----------------------------------------|
    Console.print_info("Loading snapshot and catalogue membership.", flush = True)

    # scida is used to load EAGLE data in a delayed fashion using dask.
    # This will not actually 'load' the data - just the structure and some metadata.
    # Data will be loaded from disk only when it is actually needed.

    snapshot = load_snapshot(snapshot_directory, number = args.snapshot_number, redshift_tag = args.snapshot_tag, is_snipshot = args.snipshot)
    Console.print_info(snapshot, flush = True)
#    catalogue_data = load_catalogue_data(catalogue_data_directory, number = args.snapshot_number, redshift_tag = args.snapshot_tag)
    catalogue_membership = load_catalogue_membership(catalogue_membership_directory, number = args.snapshot_number, redshift_tag = args.snapshot_tag, is_snipshot = args.snipshot)
    Console.print_info(catalogue_membership, flush = True)

    n_total_gas   = int(snapshot["PartType0"]["ParticleIDs"].shape[0])
    n_total_dm    = int(snapshot["PartType1"]["ParticleIDs"].shape[0])
    n_total_stars = int(snapshot["PartType4"]["ParticleIDs"].shape[0])
    n_total_bh    = int(snapshot["PartType5"]["ParticleIDs"].shape[0])
    snapshot_particle_numbers = { "PartType0" : n_total_gas, "PartType1" : n_total_dm, "PartType4" : n_total_stars, "PartType5" : n_total_bh }
    Console.print_info(f"Number of gas particles:         {n_total_gas}",   flush = True)
    Console.print_info(f"Number of dark matter particles: {n_total_dm}",    flush = True)
    Console.print_info(f"Number of star particles:        {n_total_stars}", flush = True)
    Console.print_info(f"Number of black hole particles:  {n_total_bh}",    flush = True)

    #---------------|
    # Load metadata |
    #---------------|
    Console.print_info("Loading metadata:", flush = True)

    metadata = Metadata()

    Console.print_info("    Snapshot.", flush = True)
    with h5.File(snapshot_first_file_path, "r") as file:

        metadata.constant_boltzmann    = np.float64(file["Constants"].attrs["BOLTZMANN"])
        metadata.constant_gamma        = np.float64(file["Constants"].attrs["GAMMA"])
        metadata.constant_protonmass   = np.float64(file["Constants"].attrs["PROTONMASS"])
        metadata.constant_sec_per_year = np.float64(file["Constants"].attrs["SEC_PER_YEAR"])
        metadata.constant_solar_mass   = np.float64(file["Constants"].attrs["SOLAR_MASS"])

        metadata.header_expansion_factor       = np.float64(file["Header"].attrs["ExpansionFactor"])
        metadata.header_hubble_param           = np.float64(file["Header"].attrs["HubbleParam"])
        metadata.header_mass_table             = np.array(file["Header"].attrs["MassTable"], dtype = np.float64)
        metadata.header_num_files_per_snapshot = np.int32(1) # Only one aux file
        metadata.header_num_part_this_file     = np.array(file["Header"].attrs["NumPart_Total"], dtype = np.int32) # Only one aux file so all the particles are here
        metadata.header_num_part_total         = np.array(file["Header"].attrs["NumPart_Total"], dtype = np.int32)

    Console.print_info("    Catalogue.", flush = True)
    with h5.File(catalogue_membership_first_file_path, "r") as file:

        metadata.header_num_part_sub = np.array(file["Header"].attrs["NumPart_Total"], dtype = np.int32)

    #-----------------------|
    # Create auxiliary file |
    #-----------------------|
    Console.print_info("Creating auxiliary file.", flush = True)

    output_filepath: str
    try:
        output_filepath = make_aux_file(
            directory                       = args.output_directory,
            number                          = args.snapshot_number,
            redshift_tag                    = args.snapshot_tag,
            metadata                        = metadata,
            number_of_gas_particles         = n_total_gas   if args.gas         else None,
            number_of_dark_matter_particles = n_total_dm    if args.darkmatter else None,
            number_of_star_particles        = n_total_stars if args.stars       else None,
            number_of_black_hole_particles  = n_total_bh    if args.blackholes else None,
            allow_overwrite                 = args.overwrite,
            is_snipshot                     = args.snipshot
        )
    except FileExistsError as e:
        if args.update:
            output_filepath = e.filename
            Console.print_info(f"Found file at {output_filepath}. This will be updated with new data.", flush = True)
        else:
            Console.print_info(f"Unable to create new auxiliary file at {e.filename}.\nA file already exists at this location.\nTo overwrite, specify --overwrite.\nTo update this file in-place, use --update.", flush = True)
            return
        
    #--------------------------|
    # Loop over particle types |
    #--------------------------|

    for particle_type, particle_type_name in zip(["PartType0", "PartType1", "PartType4", "PartType5"], ["gas", "dark_matter", "star", "black_hole"]):

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

        Console.print_info(f"Tagging {particle_type_name} particles:", flush = True)

        #--------------------------------------------|
        # Compute the chunking for the snapshot data |
        #--------------------------------------------|
        Console.print_info("    Computing the chunking for the snapshot data.", flush = True)

        # How many chunks should be used?
        n_chunks: int = args.chunks

        # Given the number of chunks, what is the minimum number of elements each chunk must contain?
        chunk_size_min = snapshot_particle_numbers[particle_type] // n_chunks

        # Some elements will be left over, how many are there?
        number_with_extra_element = snapshot_particle_numbers[particle_type] % n_chunks

        # Set the lengths to the minimum length
        chunk_lengths = np.full(shape = (n_chunks,), fill_value = chunk_size_min, dtype = np.int64)

        # Assign an extra element to the first few chunks to account for the leftover elements
        chunk_lengths[:number_with_extra_element] += 1

        # Compute the offsets for each chunk (1 more element than the number of chunks)
        chunk_offsets = np.array([0, *chunk_lengths.cumsum()], dtype = np.int64)

        #-------------------------|
        # Get the membership data |
        #-------------------------|
        Console.print_info("    Getting membership data.", flush = True)

        catalogue_particle_ids  = catalogue_membership[particle_type]["ParticleIDs"]
        catalogue_group_numbers = catalogue_membership[particle_type]["GroupNumber"]
        catalogue_subhalo_ids   = catalogue_membership[particle_type]["SubGroupNumber"]
        Console.print_info(f"    Number of {particle_type_name} particles in FOF groups: {catalogue_membership[particle_type]["ParticleIDs"].shape[0]}", flush = True)

        #----------------------------------|
        # Handle chunking of the catalogue |
        #----------------------------------|

        # This can be a simple chunking as it is only to reduce the memory load

        n_cat_chunks = args.catalogue_chunks
        catalogue_chunk_size = catalogue_membership[particle_type]["ParticleIDs"].shape[0] // n_cat_chunks
        catalogue_chunk_sizes = np.full(shape = (n_cat_chunks,), fill_value = catalogue_chunk_size, dtype = np.int64)
        catalogue_chunk_sizes[-1] += catalogue_membership[particle_type]["ParticleIDs"].shape[0] % n_cat_chunks
        catalogue_chunk_offsets = np.array([0, *catalogue_chunk_sizes.cumsum()], dtype = np.int64)

        #------------------------------------------|
        # Calculate how to sort the membership IDs |
        #------------------------------------------|
        Console.print_info("    Computing argsort of catalogue membership particle IDs.", flush = True)

        # This makes searching the data easier

        if n_cat_chunks == 1:
            # Simple version to do the argsort in one go

            Console.print_info("    Loading particle id data.", flush = True)
            t = catalogue_particle_ids.values
            Console.print_info("    Computing argsort of catalogue membership particle IDs.", flush = True)
            sorted_indexes__catalogue_particle_ids = dask_array.from_array(np.argsort(t), chunks = catalogue_particle_ids.chunks)

        else:
            # Compute the argsort in chunks
            # This assumes argsort has a time complexity of O(n log n)
            # making the chunked time complexity O( O(n) [ 1 - ( log x / log n ) ] )
            # making the new runtime 1 - ( log x / log n ) times faster

            sorted_indexes__catalogue_particle_ids = np.empty(shape = (catalogue_membership[particle_type]["ParticleIDs"].shape[0],), dtype = np.int64)
            for catalogue_chunk_index in range(n_cat_chunks):
                Console.print_info(f"        Doing chunk {catalogue_chunk_index + 1} / {n_cat_chunks}.", flush = True)
                chunk_region = slice(catalogue_chunk_offsets[catalogue_chunk_index], catalogue_chunk_offsets[catalogue_chunk_index + 1])
                # Add the offset of this chunk to the resulting argsort to allow direct indexing of the original data
                sorted_indexes__catalogue_particle_ids[chunk_region] = np.argsort(catalogue_particle_ids[chunk_region].values) + catalogue_chunk_offsets[catalogue_chunk_index]
            sorted_indexes__catalogue_particle_ids = dask_array.from_array(sorted_indexes__catalogue_particle_ids, chunks = catalogue_particle_ids.chunks)

        #------------------------------|
        # Define how to update a chunk |
        #------------------------------|

        file_lock = SerializableLock()

        @delayed
        def process_chunk(chunk_index) -> None:

            if chunk_index == 0:
                Console.print_info(f"    Chunk {chunk_index + 1}/{n_chunks}:", flush = True)
                Console.print_info(f"        Start index: {chunk_offsets[chunk_index]}, Length: {chunk_lengths[chunk_index]}", flush = True)

            snapshot_slice = slice(chunk_offsets[chunk_index], chunk_offsets[chunk_index + 1])
            snapshot_ids = snapshot[particle_type]["ParticleIDs"][snapshot_slice]

            snapshot_membership_mask = np.full(shape = (chunk_lengths[chunk_index],), fill_value = False,      dtype = np.bool_)
            snapshot_group_numbers   = np.full(shape = (chunk_lengths[chunk_index],), fill_value = NULL_INDEX, dtype = catalogue_group_numbers.dtype)
            snapshot_subhalo_ids     = np.full(shape = (chunk_lengths[chunk_index],), fill_value = NULL_INDEX, dtype = catalogue_subhalo_ids.dtype)

            #--------------------------------|
            # Loop over each catalogue chunk |
            #--------------------------------|

            for catalogue_chunk_index in range(n_cat_chunks):
                if True:#chunk_index == 0:
                    Console.print_info(f"        Doing chunk {catalogue_chunk_index + 1} / {n_cat_chunks}:", flush = True)

                if catalogue_chunk_index > 0:
                    # No need to do this for the first chunk
                    snapshot_membership_mask[:] = False # Reset the mask for each chunk

                catalogue_chunk_region = slice(catalogue_chunk_offsets[catalogue_chunk_index], catalogue_chunk_offsets[catalogue_chunk_index + 1])

                if True:#chunk_index == 0:
                    if n_cat_chunks > 1:
                        Console.print_info("            Determining locations.", flush = True)
                    else:
                        Console.print_info("        Determining locations.", flush = True)
                # Figure out where the data needs to be drawn from.
                # WARNING: this uses searchsorted, which means any valid ID will have a return value - even if it doesn't appear in the list!
                snap_target_indexes = dask_array.take(
                    sorted_indexes__catalogue_particle_ids,
                    dask_array.searchsorted( # This must ONLY be performed on each chunk!
                        dask_array.take(
                            catalogue_particle_ids,
                            sorted_indexes__catalogue_particle_ids[catalogue_chunk_region]
                        ),
                        snapshot_ids
                    ) + catalogue_chunk_offsets[catalogue_chunk_index] # Add the snapshot chunk offset to allow direct access to the original data
                )
                # The only trustworthy indexes are where the IDs actually match!
                if True:#chunk_index == 0:
                    if n_cat_chunks > 1:
                        Console.print_info("            Finding matches.", flush = True)
                    else:
                        Console.print_info("        Finding matches.", flush = True)
                snapshot_membership_mask[(snapshot_ids == dask_array.take(catalogue_particle_ids, snap_target_indexes)).compute()] = True

                # Update the data arrays where a match is found
                if True:#chunk_index == 0:
                    if n_cat_chunks > 1:
                        Console.print_info("    Updating group numbers.", flush = True)
                    else:
                        Console.print_info("        Updating group numbers.", flush = True)
                snapshot_group_numbers[snapshot_membership_mask] = dask_array.take(catalogue_group_numbers, snap_target_indexes[snapshot_membership_mask])
                if True:#chunk_index == 0:
                    if n_cat_chunks > 1:
                        Console.print_info("    Updating subhalo IDs.", flush = True)
                    else:
                        Console.print_info("        Updating subhalo IDs.", flush = True)
                snapshot_subhalo_ids[snapshot_membership_mask] = dask_array.take(catalogue_subhalo_ids, snap_target_indexes[snapshot_membership_mask])

                if True:#chunk_index == 0:
                    if n_cat_chunks > 1:
                        Console.print_info("            Ensuring all values are computed.", flush = True)
                    else:
                        Console.print_info("        Ensuring all values are computed.", flush = True)

            #-----------------------------|
            # Write the chunk to the disk |
            #-----------------------------|

            snapshot_ids = compute(snapshot_ids) # Make sure the IDs are available in memory

            with file_lock:
                if args.verbose or chunk_index == 0:
                    Console.print_info(f"        Runner {chunk_index + 1} / {n_chunks} writing:", flush = True)

                if args.verbose or chunk_index == 0:
                    Console.print_info("            Writing PartType0/ParticleIDs.", flush = True)
                save_chunk(
                    filepath      = output_filepath,
                    particle_type = particle_type,
                    field         = "ParticleIDs",
                    offset        = chunk_offsets[chunk_index],
                    length        = chunk_lengths[chunk_index],
                    data          = snapshot_ids
                )

                if args.verbose or chunk_index == 0:
                    Console.print_info("            Writing PartType0/GroupNumber.", flush = True)
                save_chunk(
                    filepath      = output_filepath,
                    particle_type = particle_type,
                    field         = "GroupNumber",
                    offset        = chunk_offsets[chunk_index],
                    length        = chunk_lengths[chunk_index],
                    data          = snapshot_group_numbers
                )

                if args.verbose or chunk_index == 0:
                    Console.print_info("            Writing PartType0/SubGroupNumber.", flush = True)
                save_chunk(
                    filepath      = output_filepath,
                    particle_type = particle_type,
                    field         = "SubGroupNumber",
                    offset        = chunk_offsets[chunk_index],
                    length        = chunk_lengths[chunk_index],
                    data          = snapshot_subhalo_ids
                )

        #------------------|
        # Loop over chunks |
        #------------------|

        delayed_calls = [process_chunk(chunk_index) for chunk_index in range(n_chunks)]
        compute(*delayed_calls)
        Console.print_info(f"    {particle_type_name.title()} particles done.", flush = True)

    Console.print_info("DONE", flush = True)
    return



if __name__ == "__main__":
    main()
