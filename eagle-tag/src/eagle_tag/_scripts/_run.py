# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later


import argparse
import os

import dask
from dask import delayed, compute
import dask.array as dask_array
from dask.utils import SerializableLock
import numpy as np
import h5py as h5

from eagle_tag import Metadata, load_snapshot, load_catalogue_membership, load_catalogue_data, make_aux_file, save_chunk



NULL_INDEX = 2**30 # Used where an integer index needs to be NULL



def main():
    print(
"""
--|| EAGLE-tag ||--

Creates snapshot-length files with the catalogue membership
information for FOF groups and SUBFIND haloes.
""",
    flush = True)

    #------------------------------|
    # Parse command line arguments |
    #------------------------------|
    print("Parsing command line arguments.", flush = True)

    parser = argparse.ArgumentParser(description = "Run EAGLE halo membership tagging.")

    parser.add_argument("simulation_directory", type = str,                help = "Directory containing the EAGLE simulation data.")
    parser.add_argument("snapshot_number",      type = str,                help = "Snapshot number (e.g. \"012\").")
    parser.add_argument("snapshot_tag",         type = str,                help = "Snapshot redshift tag (e.g. \"z012p345\").")
    parser.add_argument("chunks",               type = int,                help = "Number of chunks to divide the data into.")
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

    print(f"Arguments: {args}", flush = True)

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
    print("Creating directory paths.", flush = True)

    snapshot_directory = os.path.join(args.simulation_directory, f"sn{'i' if args.snipshot else 'a'}pshot_{args.snapshot_number}_{args.snapshot_tag}")
    #catalogue_data_directory = os.path.join(args.simulation_directory, f"groups_{'snip_' if args.snipshot else ''}{args.snapshot_number}_{args.snapshot_tag}")
    catalogue_membership_directory = os.path.join(args.simulation_directory, f"particledata_{'snip_' if args.snipshot else ''}{args.snapshot_number}_{args.snapshot_tag}")

    # We need to get these manually so that we can load metadata with h5py
    snapshot_first_file_path = os.path.join(snapshot_directory, f"sn{'i' if args.snipshot else 'a'}p_{args.snapshot_number}_{args.snapshot_tag}.0.hdf5")
    catalogue_membership_first_file_path = os.path.join(catalogue_membership_directory, f"eagle_subfind_{'snip_' if args.snipshot else ''}particles_{args.snapshot_number}_{args.snapshot_tag}.0.hdf5")

    #----------------------------------------|
    # Load snapshot and catalogue membership |
    #----------------------------------------|
    print("Loading snapshot and catalogue membership.", flush = True)

    # scida is used to load EAGLE data in a delayed fashion using dask.
    # This will not actually 'load' the data - just the structure and some metadata.
    # Data will be loaded from disk only when it is actually needed.

    snapshot = load_snapshot(snapshot_directory)
    print(snapshot, flush = True)
#    catalogue_data = load_catalogue_data(catalogue_data_directory, number = args.snapshot_number, redshift_tag = args.snapshot_tag)
    catalogue_membership = load_catalogue_membership(catalogue_membership_directory, number = args.snapshot_number, redshift_tag = args.snapshot_tag)
    print(catalogue_membership, flush = True)

    n_total_gas   = int(snapshot.data["PartType0"].fieldlength)
    n_total_dm    = int(snapshot.data["PartType1"].fieldlength)
    n_total_stars = int(snapshot.data["PartType4"].fieldlength)
    n_total_bh    = int(snapshot.data["PartType5"].fieldlength)
    print(f"Number of gas particles:         {n_total_gas}",   flush = True)
    print(f"Number of dark matter particles: {n_total_dm}",    flush = True)
    print(f"Number of star particles:        {n_total_stars}", flush = True)
    print(f"Number of black hole particles:  {n_total_bh}",    flush = True)

    #---------------|
    # Load metadata |
    #---------------|
    print("Loading metadata:", flush = True)

    metadata = Metadata()

    print("    Snapshot.", flush = True)
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

    print("    Catalogue.", flush = True)
    with h5.File(catalogue_membership_first_file_path, "r") as file:

        metadata.header_num_part_sub = np.array(file["Header"].attrs["NumPart_Total"], dtype = np.int32)

    #-----------------------|
    # Create auxiliary file |
    #-----------------------|
    print("Creating auxiliary file.", flush = True)

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
            print(f"Found file at {output_filepath}. This will be updated with new data.", flush = True)
        else:
            print(f"Unable to create new auxiliary file at {e.filename}.\nA file already exists at this location.\nTo overwrite, specify --overwrite.\nTo update this file in-place, use --update.", flush = True)
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

        print(f"Tagging {particle_type_name} particles:", flush = True)

        #--------------------------------------------|
        # Compute the chunking for the snapshot data |
        #--------------------------------------------|
        print("    Computing the chunking for the snapshot data.", flush = True)

        # How many chunks should be used?
        n_chunks: int = args.chunks

        # Given the number of chunks, what is the minimum number of elements each chunk must contain?
        chunk_size_min = snapshot.data[particle_type].fieldlength // n_chunks

        # Some elements will be left over, how many are there?
        number_with_extra_element = snapshot.data[particle_type].fieldlength % n_chunks

        # Set the lengths to the minimum length
        chunk_lengths = np.full(shape = (n_chunks,), fill_value = chunk_size_min, dtype = np.int64)

        # Assign an extra element to the first few chunks to account for the leftover elements
        chunk_lengths[:number_with_extra_element] += 1

        # Compute the offsets for each chunk (1 more element than the number of chunks)
        chunk_offsets = np.array([0, *chunk_lengths.cumsum()], dtype = np.int64)

        #-------------------------|
        # Get the membership data |
        #-------------------------|
        print("    Getting membership data.", flush = True)

        catalogue_particle_ids  = catalogue_membership.data[particle_type]["ParticleIDs"]
        catalogue_group_numbers = catalogue_membership.data[particle_type]["GroupNumber"]
        catalogue_subhalo_ids   = catalogue_membership.data[particle_type]["SubGroupNumber"]
        print(f"    Number of {particle_type_name} particles in FOF groups: {catalogue_membership.data[particle_type].fieldlength}", flush = True)

        #------------------------------------------|
        # Calculate how to sort the membership IDs |
        #------------------------------------------|
        print("    Computing argsort.", flush = True)

        # This makes searching the data easier
        sorted_indexes__catalogue_particle_ids = dask_array.from_array(np.argsort(catalogue_particle_ids), chunks = catalogue_particle_ids.chunks)

        #------------------------------|
        # Define how to update a chunk |
        #------------------------------|

        file_lock = SerializableLock()

        @delayed
        def process_chunk(chunk_index) -> None:

            if chunk_index == 0:
                print(f"    Chunk {chunk_index + 1}/{n_chunks}:", flush = True)
                print(f"        Start index: {chunk_offsets[chunk_index]}, Length: {chunk_lengths[chunk_index]}", flush = True)

            snapshot_slice = slice(chunk_offsets[chunk_index], chunk_offsets[chunk_index + 1])
            snapshot_ids = snapshot.data[particle_type]["ParticleIDs"][snapshot_slice]

            snapshot_membership_mask = np.full( shape = (chunk_lengths[chunk_index],), fill_value = False, dtype = np.bool_)
            snapshot_group_numbers   = np.empty(shape = (chunk_lengths[chunk_index],), dtype = catalogue_group_numbers.dtype)
            snapshot_subhalo_ids     = np.empty(shape = (chunk_lengths[chunk_index],), dtype = catalogue_subhalo_ids.dtype)

            
            if chunk_index == 0:
                print("        Determining locations.", flush = True)
            # Figure out where the data needs to be drawn from.
            # WARNING: this uses searchsorted, which means any valid ID will have a return value - even if it dosen't appear in the list!
            snap_target_indexes = dask_array.take(
                sorted_indexes__catalogue_particle_ids,
                dask_array.searchsorted(
                    dask_array.take(
                        catalogue_particle_ids,
                        sorted_indexes__catalogue_particle_ids
                    ),
                    snapshot_ids
                )
            )
            # The only trustworthy indexes are where the IDs actually match!
            if chunk_index == 0:
                print("        Finding matches.", flush = True)
            snapshot_membership_mask[(snapshot_ids == dask_array.take(catalogue_particle_ids, snap_target_indexes)).compute()] = True

            # Update the data arrays where a match is found
            if chunk_index == 0:
                print("        Updating group numbers.", flush = True)
            snapshot_group_numbers[snapshot_membership_mask] = dask_array.take(catalogue_group_numbers, snap_target_indexes[snapshot_membership_mask])
            if chunk_index == 0:
                print("        Updating subhalo IDs.", flush = True)
            snapshot_subhalo_ids[snapshot_membership_mask] = dask_array.take(catalogue_subhalo_ids, snap_target_indexes[snapshot_membership_mask])

            # Set all other entries to the NULL_INDEX value
            if chunk_index == 0:
                print("        Updating NULL group numbers.", flush = True)
            snapshot_group_numbers[~snapshot_membership_mask] = NULL_INDEX
            if chunk_index == 0:
                print("        Updating NULL subhalo IDs.", flush = True)
            snapshot_subhalo_ids[~snapshot_membership_mask] = NULL_INDEX

            if chunk_index == 0:
                print("        Ensuring all values are computed.", flush = True)

            snapshot_ids, snapshot_group_numbers, snapshot_subhalo_ids = compute(
                snapshot_ids,
                snapshot_group_numbers,
                snapshot_subhalo_ids
            )

            with file_lock:
                if args.verbose or chunk_index == 0:
                    print(f"        Runner {chunk_index + 1} / {n_chunks} writing:", flush = True)

                if args.verbose or chunk_index == 0:
                    print("            Writing PartType0/ParticleIDs.", flush = True)
                save_chunk(
                    filepath      = output_filepath,
                    particle_type = particle_type,
                    field         = "ParticleIDs",
                    offset        = chunk_offsets[chunk_index],
                    length        = chunk_lengths[chunk_index],
                    data          = snapshot_ids
                )

                if args.verbose or chunk_index == 0:
                    print("            Writing PartType0/GroupNumber.", flush = True)
                save_chunk(
                    filepath      = output_filepath,
                    particle_type = particle_type,
                    field         = "GroupNumber",
                    offset        = chunk_offsets[chunk_index],
                    length        = chunk_lengths[chunk_index],
                    data          = snapshot_group_numbers
                )

                if args.verbose or chunk_index == 0:
                    print("            Writing PartType0/SubGroupNumber.", flush = True)
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
        print(f"    {particle_type_name.title()} particles done.", flush = True)

    print("DONE", flush = True)
    return

















#    #--------------------------------------------|
#    # Compute the chunking for the snapshot data |
#    #--------------------------------------------|
#    print("Computing the chunking for the snapshot data.", flush = True)
#
#    # How many chunks should be used?
#    n_chunks: int = args.chunks
#
#    # Given the number of chunks, what is the minimum number of elements each chunk must contain?
#    chunk_size_min_gas = n_total_gas // n_chunks
#    chunk_size_min_stars = n_total_stars // n_chunks
#
#    # Some elements will be left over, how many are there?
#    number_with_extra_element_gas = n_total_gas % n_chunks
#    number_with_extra_element_stars = n_total_stars % n_chunks
#
#    # Set the lengths to the minimum length
#    chunk_lengths_gas = np.full(shape = (n_chunks,), fill_value = chunk_size_min_gas, dtype = np.int64)
#    chunk_lengths_stars = np.full(shape = (n_chunks,), fill_value = chunk_size_min_stars, dtype = np.int64)
#
#    # Assign an extra element to the first few chunks to account for the leftover elements
#    chunk_lengths_gas[:number_with_extra_element_gas] += 1
#    chunk_lengths_stars[:number_with_extra_element_stars] += 1
#
#    # Compute the offsets for each chunk (1 more element than the number of chunks)
#    chunk_offsets_gas = np.array([0, *chunk_lengths_gas.cumsum()], dtype = np.int64)
#    chunk_offsets_stars = np.array([0, *chunk_lengths_stars.cumsum()], dtype = np.int64)
#
#    #-------------------------|
#    # Get the membership data |
#    #-------------------------|
#    print("Getting membership data.", flush = True)
#
#    catalogue_particle_ids_gas  = catalogue_membership.data["PartType0"]["ParticleIDs"]
#    catalogue_group_numbers_gas = catalogue_membership.data["PartType0"]["GroupNumber"]
#    catalogue_subhalo_ids_gas   = catalogue_membership.data["PartType0"]["SubGroupNumber"]
#    print(f"Number of gas particles in FOF groups: {catalogue_membership.data["PartType0"].fieldlength}", flush = True)
#
#    catalogue_particle_ids_stars  = catalogue_membership.data["PartType4"]["ParticleIDs"]
#    catalogue_group_numbers_stars = catalogue_membership.data["PartType4"]["GroupNumber"]
#    catalogue_subhalo_ids_stars   = catalogue_membership.data["PartType4"]["SubGroupNumber"]
#    print(f"Number of star particles in FOF groups: {catalogue_membership.data["PartType4"].fieldlength}", flush = True)
#
#    #-------------------------|
#    # Get the membership data |
#    #-------------------------|
#    print("Computing argsort:", flush = True)
#
#    print("    Gas.", flush = True)
#    sorted_indexes__catalogue_particle_ids_gas = dask_array.from_array(np.argsort(catalogue_particle_ids_gas), chunks = catalogue_particle_ids_gas.chunks)
#
#    print("    Stars.", flush = True)
#    sorted_indexes__catalogue_particle_ids_stars = dask_array.from_array(np.argsort(catalogue_particle_ids_stars), chunks = catalogue_particle_ids_stars.chunks)
#
#    #------------------------------|
#    # Define how to update a chunk |
#    #------------------------------|
#
#    file_lock = SerializableLock()
#
#    @delayed
#    def process_chunk(chunk_index) -> None:
#
#        if chunk_index == 0:
#            print(f"Chunk {chunk_index + 1}/{n_chunks}:", flush = True)
#            print(f"    Start index: {chunk_offsets_gas[chunk_index]}, Length: {chunk_lengths_gas[chunk_index]}", flush = True)
#
#        #-----------|
#        # Gas first |
#        #-----------|
#        if chunk_index == 0:
#            print("    Gas:", flush = True)
#
#        snapshot_slice_gas = slice(chunk_offsets_gas[chunk_index], chunk_offsets_gas[chunk_index + 1])
#        snapshot_ids_gas = snapshot.data["PartType0"]["ParticleIDs"][snapshot_slice_gas]
#
#        snapshot_membership_mask_gas = np.full( shape = (chunk_lengths_gas[chunk_index],), fill_value = False, dtype = np.bool_)
#        snapshot_group_numbers_gas   = np.empty(shape = (chunk_lengths_gas[chunk_index],), dtype = catalogue_group_numbers_gas.dtype)
#        snapshot_subhalo_ids_gas     = np.empty(shape = (chunk_lengths_gas[chunk_index],), dtype = catalogue_subhalo_ids_gas.dtype)
#
#        
#        if chunk_index == 0:
#            print("        Determining locations.", flush = True)
#        # Figure out where the data needs to be drawn from.
#        # WARNING: this uses searchsorted, which means any valid ID will have a return value - even if it dosen't appear in the list!
#        snap_target_indexes = dask_array.take(
#            sorted_indexes__catalogue_particle_ids_gas,
#            dask_array.searchsorted(
#                dask_array.take(
#                    catalogue_particle_ids_gas,
#                    sorted_indexes__catalogue_particle_ids_gas
#                ),
#                snapshot_ids_gas
#            )
#        )
#        # The only trustworthy indexes are where the IDs actually match!
#        if chunk_index == 0:
#            print("        Finding matches.", flush = True)
#        snapshot_membership_mask_gas[(snapshot_ids_gas == dask_array.take(catalogue_particle_ids_gas, snap_target_indexes)).compute()] = True
#
#        # Update the data arrays where a match is found
#        if chunk_index == 0:
#            print("        Updating group numbers.", flush = True)
#        snapshot_group_numbers_gas[snapshot_membership_mask_gas] = dask_array.take(catalogue_group_numbers_gas, snap_target_indexes[snapshot_membership_mask_gas])
#        if chunk_index == 0:
#            print("        Updating subhalo IDs.", flush = True)
#        snapshot_subhalo_ids_gas[snapshot_membership_mask_gas] = dask_array.take(catalogue_subhalo_ids_gas, snap_target_indexes[snapshot_membership_mask_gas])
#
#        # Set all other entries to the NULL_INDEX value
#        if chunk_index == 0:
#            print("        Updating NULL group numbers.", flush = True)
#        snapshot_group_numbers_gas[~snapshot_membership_mask_gas] = NULL_INDEX
#        if chunk_index == 0:
#            print("        Updating NULL subhalo IDs.", flush = True)
#        snapshot_subhalo_ids_gas[~snapshot_membership_mask_gas] = NULL_INDEX
#
#        if chunk_index == 0:
#            print("        Ensuring all values are computed.", flush = True)
#
#        snapshot_ids_gas, snapshot_group_numbers_gas, snapshot_subhalo_ids_gas = compute(
#            snapshot_ids_gas,
#            snapshot_group_numbers_gas,
#            snapshot_subhalo_ids_gas
#        )
#
#        with file_lock:
#            if args.verbose or chunk_index == 0:
#                print(f"        Runner {chunk_index + 1} / {n_chunks} writing:", flush = True)
#
#            if args.verbose or chunk_index == 0:
#                print("            Writing PartType0/ParticleIDs.", flush = True)
#            save_chunk(
#                filepath      = output_filepath,
#                particle_type = "0",
#                field         = "ParticleIDs",
#                offset        = chunk_offsets_gas[chunk_index],
#                length        = chunk_lengths_gas[chunk_index],
#                data          = snapshot_ids_gas
#            )
#
#            if args.verbose or chunk_index == 0:
#                print("            Writing PartType0/GroupNumber.", flush = True)
#            save_chunk(
#                filepath      = output_filepath,
#                particle_type = "0",
#                field         = "GroupNumber",
#                offset        = chunk_offsets_gas[chunk_index],
#                length        = chunk_lengths_gas[chunk_index],
#                data          = snapshot_group_numbers_gas
#            )
#
#            if args.verbose or chunk_index == 0:
#                print("            Writing PartType0/SubGroupNumber.", flush = True)
#            save_chunk(
#                filepath      = output_filepath,
#                particle_type = "0",
#                field         = "SubGroupNumber",
#                offset        = chunk_offsets_gas[chunk_index],
#                length        = chunk_lengths_gas[chunk_index],
#                data          = snapshot_subhalo_ids_gas
#            )
#
#        #--------------|
#        # Stars second |
#        #--------------|
#        if chunk_index == 0:
#            print("    Stars:", flush = True)
#
#        snapshot_slice_stars = slice(chunk_offsets_stars[chunk_index], chunk_offsets_stars[chunk_index + 1])
#        snapshot_ids_stars = snapshot.data["PartType4"]["ParticleIDs"][snapshot_slice_stars]
#
#        snapshot_membership_mask_stars = np.full( shape = (chunk_lengths_stars[chunk_index],), fill_value = False, dtype = np.bool_)
#        snapshot_group_numbers_stars   = np.empty(shape = (chunk_lengths_stars[chunk_index],), dtype = catalogue_group_numbers_stars.dtype)
#        snapshot_subhalo_ids_stars     = np.empty(shape = (chunk_lengths_stars[chunk_index],), dtype = catalogue_subhalo_ids_stars.dtype)
#
#        # Figure out where the data needs to be drawn from.
#        # WARNING: this uses searchsorted, which means any valid ID will have a return value - even if it dosen't appear in the list!
#        if chunk_index == 0:
#            print("        Determining locations.", flush = True)
#        snap_target_indexes = dask_array.take(
#            sorted_indexes__catalogue_particle_ids_stars,
#            dask_array.searchsorted(
#                dask_array.take(
#                    catalogue_particle_ids_stars,
#                    sorted_indexes__catalogue_particle_ids_stars
#                ),
#                snapshot_ids_stars
#            )
#        )
#        # The only trustworthy indexes are where the IDs actually match!
#        if chunk_index == 0:
#            print("        Finding matches.", flush = True)
#        snapshot_membership_mask_stars[snapshot_ids_stars == catalogue_particle_ids_stars[snap_target_indexes]] = True
#
#        # Update the data arrays where a match is found
#        if chunk_index == 0:
#            print("        Updating NULL group numbers.", flush = True)
#        snapshot_group_numbers_stars[snapshot_membership_mask_stars] = dask_array.take(catalogue_group_numbers_stars, snap_target_indexes[snapshot_membership_mask_stars])
#        if chunk_index == 0:
#            print("        Updating NULL subhalo IDs.", flush = True)
#        snapshot_subhalo_ids_stars[snapshot_membership_mask_stars] = dask_array.take(catalogue_subhalo_ids_stars, snap_target_indexes[snapshot_membership_mask_stars])
#
#        # Set all other entries to the NULL_INDEX value
#        if chunk_index == 0:
#            print("        Updating group numbers.", flush = True)
#        snapshot_group_numbers_stars[~snapshot_membership_mask_stars] = NULL_INDEX
#        if chunk_index == 0:
#            print("        Updating subhalo IDs.", flush = True)
#        snapshot_subhalo_ids_stars[~snapshot_membership_mask_stars] = NULL_INDEX
#
#        snapshot_ids_stars, snapshot_group_numbers_stars, snapshot_subhalo_ids_stars = compute(
#            snapshot_ids_stars,
#            snapshot_group_numbers_stars,
#            snapshot_subhalo_ids_stars
#        )
#
#        with file_lock:
#            if args.verbose or chunk_index == 0:
#                print(f"        Runner {chunk_index + 1} / {n_chunks} writing:", flush = True)
#
#            if args.verbose or chunk_index == 0:
#                print("            Writing PartType4/ParticleIDs.", flush = True)
#            save_chunk(
#                filepath      = output_filepath,
#                particle_type = "4",
#                field         = "ParticleIDs",
#                offset        = chunk_offsets_stars[chunk_index],
#                length        = chunk_lengths_stars[chunk_index],
#                data          = snapshot_ids_stars
#            )
#
#            if args.verbose or chunk_index == 0:
#                print("            Writing PartType4/GroupNumber.", flush = True)
#            save_chunk(
#                filepath      = output_filepath,
#                particle_type = "4",
#                field         = "GroupNumber",
#                offset        = chunk_offsets_stars[chunk_index],
#                length        = chunk_lengths_stars[chunk_index],
#                data          = snapshot_group_numbers_stars
#            )
#
#            if args.verbose or chunk_index == 0:
#                print("            Writing PartType4/SubGroupNumber.", flush = True)
#            save_chunk(
#                filepath      = output_filepath,
#                particle_type = "4",
#                field         = "SubGroupNumber",
#                offset        = chunk_offsets_stars[chunk_index],
#                length        = chunk_lengths_stars[chunk_index],
#                data          = snapshot_subhalo_ids_stars
#            )
#
#    #------------------|
#    # Loop over chunks |
#    #------------------|
#
#    delayed_calls = [process_chunk(chunk_index) for chunk_index in range(n_chunks)]
#    compute(*delayed_calls)
#
#    print("DONE", flush = True)



if __name__ == "__main__":
    main()
