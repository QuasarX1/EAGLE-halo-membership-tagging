#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "headers/constants.h"
#include "headers/chunking.h"
#include "headers/read_eagle_snapshot.h"
#include "headers/read_eagle_membership.h"
#include "headers/write_data.h"

void find_matching_particles(
    int snapshot_chunk_length,
    int catalogue_chunk_length,
    int *snapshot_particle_ids,
    int *catalogue_particle_ids,
    int *catalogue_argsort_indexes,
    int *target_indexes
)
{
    //TODO: find matches
    //TODO: record true index (in chunk) of match
}

void update_matching_data(
    int snapshot_chunk_length,
    int *snapshot_group_numbers,
    int *snapshot_subgroup_numbers,
    int *catalogue_group_numbers,
    int *catalogue_subgroup_numbers,
    int *target_indexes
)
{
    int target_index;
    for (int i = 0; i < snapshot_chunk_length; ++i)
    {
        target_index = target_indexes[i];
        if (target_index >= 0)
        {
            snapshot_group_numbers[i] = catalogue_group_numbers[target_index];
            snapshot_subgroup_numbers[i] = catalogue_subgroup_numbers[target_index];
        }
    }
}

int main(int argc, char *argv[])
{
    int verbose = 0;
    char *input_file = NULL;

    // Output file path
    char output_file_path[FILEPATH_BUFFER_LENGTH];

    // Metadata structures
    Constants constants;
    Header header;
    ParticleMembershipMetadata gas_metadata, dark_matter_metadata, stars_metadata, black_holes_metadata;

    // Chunking information
    int number_of_snapshot_chunks, number_of_catalogue_chunks;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
            verbose = 1;
        } else if ((strcmp(argv[i], "-i") == 0 || strcmp(argv[i], "--input") == 0) && i + 1 < argc) {
            input_file = argv[++i];
        } else {
            printf("Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    printf("Verbose mode: %s\n", verbose ? "ON" : "OFF");
    if (input_file) {
        printf("Input file: %s\n", input_file);
    } else {
        printf("No input file specified.\n");
    }

    //TODO: Parse arguments

    //TODO: check arguments are valid combination

    //TODO: create EAGLE data filepaths

    //TODO: load metadata

    //TODO: create output file
    make_aux_file_path(
        ,
        ,
        ,
        ,
        output_file_path
    );
    make_aux_file(
        output_file_path,
        &header,
        &constants,
        &gas_metadata,         // This should be a NULL pointer if gas data is not requested
        &dark_matter_metadata, // This should be a NULL pointer if dark matter data is not requested
        &stars_metadata,       // This should be a NULL pointer if stars data is not requested
        &black_holes_metadata  // This should be a NULL pointer if black holes data is not requested
    );

    for (int particle_type_index = 0; particle_type_index < 6; ++particle_type_index)
    {
        if (particle_type_index == 2 || particle_type_index == 3)
        {
            continue; // Skip unused particle types 2 & 3
        }

        //TODO: calculate chunking of snapshot
        int snapshot_chunk_offsets[number_of_snapshot_chunks + 1];
        int snapshot_chunk_lengths[number_of_snapshot_chunks];
        calculate_even_chunks(, number_of_snapshot_chunks, snapshot_chunk_offsets, snapshot_chunk_lengths);

        //TODO: calculate chunking of catalogue
        int catalogue_chunk_offsets[number_of_catalogue_chunks + 1];
        int catalogue_chunk_lengths[number_of_catalogue_chunks];
        calculate_even_chunks(, number_of_catalogue_chunks, catalogue_chunk_offsets, catalogue_chunk_lengths);

        int *membership_particle_ids = malloc( * sizeof(int));

        //TODO: read ALL catalogue membership IDs

        //TODO: calculate argsorting of catalogue IDs per chunk

        free(membership_particle_ids);

        //TODO: threading - make file access mutex
        for (int snapshot_chunk_index = 0; snapshot_chunk_index < number_of_snapshot_chunks; ++snapshot_chunk_index)
        {
            int snapshot_chunk_offset = snapshot_chunk_offsets[snapshot_chunk_index];
            int snapshot_chunk_length = snapshot_chunk_lengths[snapshot_chunk_index];

            int *particle_ids = malloc(snapshot_chunk_length * sizeof(int));
            int *group_numbers = malloc(snapshot_chunk_length * sizeof(int));
            int *subgroup_numbers = malloc(snapshot_chunk_length * sizeof(int));
            int *target_indexes = malloc(snapshot_chunk_length * sizeof(int));

            for (int i = 0; i < snapshot_chunk_length; ++i) {
                group_numbers[i] = SUBFIND_NULL_INDEX;
                subgroup_numbers[i] = SUBFIND_NULL_INDEX;
            }

            //TODO: read chunk of snapshot IDs

            for (int catalogue_chunk_index = 0; catalogue_chunk_index < number_of_catalogue_chunks; ++catalogue_chunk_index)
            {
                int catalogue_chunk_offset = catalogue_chunk_offsets[catalogue_chunk_index];
                int catalogue_chunk_length = catalogue_chunk_lengths[catalogue_chunk_index];

                for (int i = 0; i < snapshot_chunk_length; ++i) {
                    target_indexes[i] = -1; // Initialize to -1 (no match)
                }

                int *membership_particle_ids = malloc(catalogue_chunk_length * sizeof(int));
                int *membership_group_numbers = malloc(catalogue_chunk_length * sizeof(int));
                int *membership_subgroup_numbers = malloc(catalogue_chunk_length * sizeof(int));

                //TODO: read chunk of catalogue data

                find_matching_particles(
                    snapshot_chunk_length,
                    catalogue_chunk_length,
                    particle_ids,
                    membership_particle_ids,
                    ,
                    target_indexes
                );

                update_matching_data(
                    snapshot_chunk_length,
                    group_numbers,
                    subgroup_numbers,
                    membership_group_numbers,
                    membership_subgroup_numbers,
                    target_indexes
                );

                free(membership_particle_ids);
                free(membership_group_numbers);
                free(membership_subgroup_numbers);
            }

            free(target_indexes);

            save_chunk(
                output_file_path,
                PARTICLE_TYPE_FIELDS[particle_type_index],
                "ParticleIDs",
                snapshot_chunk_offset,
                snapshot_chunk_length,
                particle_ids
            );
            free(particle_ids);

            save_chunk(
                output_file_path,
                PARTICLE_TYPE_FIELDS[particle_type_index],
                "GroupNumber",
                snapshot_chunk_offset,
                snapshot_chunk_length,
                group_numbers
            );
            free(group_numbers);

            save_chunk(
                output_file_path,
                PARTICLE_TYPE_FIELDS[particle_type_index],
                "SubGroupNumber",
                snapshot_chunk_offset,
                snapshot_chunk_length,
                subgroup_numbers
            );
            free(subgroup_numbers);
        }
    }

    return 0;
}