# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later


import argparse
import os

import numpy as np

from eagle_tag import load_snapshot, load_catalogue_membership, load_catalogue_data, make_aux_file, save_chunk



def main():

    #------------------------------|
    # Parse command line arguments |
    #------------------------------|

    parser = argparse.ArgumentParser(description = "Run EAGLE halo membership tagging.")

    #TODO: check that this works (it probably doesn't!)
    parser.add_argument("simulation_directory",                type = str, help = "Directory containing the EAGLE simulation data.")
    parser.add_argument("snapshot_number",                     type = str, help = "Snapshot number (e.g. \"012\").")
    parser.add_argument("snapshot_tag",                        type = str, help = "Snapshot redshift tag (e.g. \"z012p345\").")
    parser.add_argument("--snipshot",                                      help = "Target a snipshot.")
    parser.add_argument("output_directory",     default = ".", type = str, help = "Alternate directory in which to create the output file.")
    parser.add_argument("--overwrite",                                     help = "Overwrite existing output files.")
    parser.add_argument("chunks",                              type = int, help = "Number of chunks to divide the data into.")
    parser.add_argument("--help",                                          help = "Number of chunks to divide the data into.")

    args = parser.parse_args()

    if args.help:
        print(args)
        return

    #------------------------|
    # Create directory paths |
    #------------------------|

    snapshot_directory = os.path.join(args.simulation_directory, f"sn{'i' if args.snipshot else 'a'}pshot_{args.snapshot_number}_{args.snapshot_tag}")
    catalogue_data_directory = os.path.join(args.simulation_directory, f"groups_{'snip_' if args.snipshot else ''}{args.snapshot_number}_{args.snapshot_tag}")
    catalogue_data_directory = os.path.join(args.simulation_directory, f"particledata_{'snip_' if args.snipshot else ''}{args.snapshot_number}_{args.snapshot_tag}")

    #----------------------------------------|
    # Load snapshot and catalogue membership |
    #----------------------------------------|

    # scida is used to load EAGLE data in a delayed fashion using dask.
    # This will not actually 'load' the data - just the structure and some metadata.
    # Data will be loaded from disk only when it is actually needed.

    snapshot = load_snapshot(snapshot_directory)
    catalogue_data = load_catalogue_data(catalogue_data_directory, number = args.snapshot_number, redshift_tag = args.snapshot_tag)
    catalogue_membership = load_catalogue_membership(catalogue_data_directory, number = args.snapshot_number, redshift_tag = args.snapshot_tag)

    n_total_gas   = int(snapshot.data["PartType0"].fieldlength)#TODO: check this!!!
    n_total_stars = int(snapshot.data["PartType4"].fieldlength)#TODO: check this!!!

    #-----------------------|
    # Create auxiliary file |
    #-----------------------|

    output_filepath = make_aux_file(
        directory                = args.output_directory,
        number                   = args.snapshot_number,
        redshift_tag             = args.snapshot_tag,
        number_of_gas_particles  = n_total_gas,
        number_of_star_particles = n_total_stars,
        allow_overwrite          = args.overwrite
    )

    #--------------------------------------------|
    # Compute the chunking for the snapshot data |
    #--------------------------------------------|

    # How many chunks should be used?
    n_chunks: int = args.chunks

    # Given the number of chunks, what is the minimum number of elements each chunk must contain?
    chunk_size_min_gas = n_total_gas // n_chunks
    chunk_size_min_stars = n_total_stars // n_chunks

    # Some elements will be left over, how many are there?
    number_with_extra_element_gas = n_total_gas % n_chunks
    number_with_extra_element_stars = n_total_stars % n_chunks

    # Set the lengths to the minimum length
    chunk_lengths_gas = np.full(shape = (n_chunks,), fill_value = chunk_size_min_gas, dtype = np.int64)
    chunk_lengths_stars = np.full(shape = (n_chunks,), fill_value = chunk_size_min_stars, dtype = np.int64)

    # Assign an extra element to the first few chunks to account for the leftover elements
    chunk_lengths_gas[:number_with_extra_element_gas] += 1
    chunk_lengths_stars[:number_with_extra_element_stars] += 1

    # Compute the offsets for each chunk (1 more element than the number of chunks)
    chunk_offsets_gas = np.array([0, *chunk_lengths_gas.cumsum()], dtype = np.int64)
    chunk_offsets_stars = np.array([0, *chunk_lengths_stars.cumsum()], dtype = np.int64)

    #-------------------------|
    # Get the membership data |
    #-------------------------|

    catalogue_particle_ids_gas  = catalogue_membership.data["PartType0"]["ParticleIDs"]
    catalogue_group_numbers_gas = catalogue_membership.data["PartType0"]["GroupNumber"]
    catalogue_subhalo_ids_gas   = catalogue_membership.data["PartType0"]["SubGroupNumber"]

    catalogue_particle_ids_stars  = catalogue_membership.data["PartType4"]["ParticleIDs"]
    catalogue_group_numbers_stars = catalogue_membership.data["PartType4"]["GroupNumber"]
    catalogue_subhalo_ids_stars   = catalogue_membership.data["PartType4"]["SubGroupNumber"]

    #------------------|
    # Loop over chunks |
    #------------------|

    for chunk_index in range(n_chunks):

        #-----------|
        # Gas first |
        #-----------|

        snapshot_slice_gas = slice(chunk_offsets_gas[chunk_index], chunk_offsets_gas[chunk_index + 1])
        snapshot_ids_gas = snapshot.data["PartType0"]["ParticleIDs"][snapshot_slice_gas]

        snapshot_group_numbers_gas = np.empty(shape = snapshot_ids_gas.shape, dtype = catalogue_group_numbers_gas.dtype)
        snapshot_subhalo_ids_gas   = np.empty(shape = snapshot_ids_gas.shape, dtype = catalogue_subhalo_ids_gas.dtype)

        #TODO: do the search here!

        save_chunk(
            filepath      = output_filepath,
            particle_type = "PartType0",
            field         = "ParticleIDs",
            offset        = chunk_offsets_gas[chunk_index],
            length        = chunk_lengths_gas[chunk_index],
            data          = snapshot_ids_gas
        )

        save_chunk(
            filepath      = output_filepath,
            particle_type = "PartType0",
            field         = "GroupNumber",
            offset        = chunk_offsets_gas[chunk_index],
            length        = chunk_lengths_gas[chunk_index],
            data          = snapshot_group_numbers_gas
        )

        save_chunk(
            filepath      = output_filepath,
            particle_type = "PartType0",
            field         = "SubGroupNumber",
            offset        = chunk_offsets_gas[chunk_index],
            length        = chunk_lengths_gas[chunk_index],
            data          = snapshot_subhalo_ids_gas
        )

        #--------------|
        # Stars second |
        #--------------|

        snapshot_slice_stars = slice(chunk_offsets_stars[chunk_index], chunk_offsets_stars[chunk_index + 1])
        snapshot_ids_stars = snapshot.data["PartType4"]["ParticleIDs"][snapshot_slice_stars]

        snapshot_group_numbers_stars = np.empty(shape = snapshot_ids_stars.shape, dtype = catalogue_group_numbers_stars.dtype)
        snapshot_subhalo_ids_stars   = np.empty(shape = snapshot_ids_stars.shape, dtype = catalogue_subhalo_ids_stars.dtype)

        #TODO: do the search here!

        save_chunk(
            filepath      = output_filepath,
            particle_type = "PartType4",
            field         = "ParticleIDs",
            offset        = chunk_offsets_stars[chunk_index],
            length        = chunk_lengths_stars[chunk_index],
            data          = snapshot_ids_stars
        )

        save_chunk(
            filepath      = output_filepath,
            particle_type = "PartType4",
            field         = "GroupNumber",
            offset        = chunk_offsets_stars[chunk_index],
            length        = chunk_lengths_stars[chunk_index],
            data          = snapshot_group_numbers_stars
        )

        save_chunk(
            filepath      = output_filepath,
            particle_type = "PartType4",
            field         = "SubGroupNumber",
            offset        = chunk_offsets_stars[chunk_index],
            length        = chunk_lengths_stars[chunk_index],
            data          = snapshot_subhalo_ids_stars
        )



if __name__ == "__main__":
    main()
