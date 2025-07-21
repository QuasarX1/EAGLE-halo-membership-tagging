#include <stdbool.h>

#include <hdf5.h>

#include "headers/constants.h"
#include "headers/read_eagle_snapshot.h"

struct SnapshotMetadata
{
    int number_of_files_per_snapshot;
    char **files;
    double redshift;
    double expansion_factor;
    double hubble_param;
    double mass_table[6];
    long number_of_particles_total[6];
    long *number_of_particles_per_file[6];
};

bool load_snapshot_metadata(
    char *snapshot_directory,
    char *number,
    char *redshift_tag,
    SnapshotMetadata *metadata
)
{
    char first_filepath[FILEPATH_BUFFER_LENGTH];
    hid_t file_id, header_group_id, attribute_id;
    herr_t status;

    sprintf(first_filepath, "%s/snapshot_%s_%s.%d.hdf5", snapshot_directory, number, redshift_tag, 0);

    file_id = H5Fopen(first_filepath, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (file_id < 0) {
        fprintf(stderr, "Failed to open file: %s\n", first_filepath);
        return false;
    }

    hid_t header_group_id = H5Oopen(file_id, "Header", H5P_DEFAULT);
    if (header_group_id < 0) {
        fprintf(stderr, "Failed to open header\n");
        H5Fclose(file_id);
        return false;
    }

    // NumFilesPerSnapshot
    attribute_id = H5Aopen(header_group_id, "NumFilesPerSnapshot", H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Failed to open attribute NumFilesPerSnapshot\n");
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    status = H5Aread(attribute_id, H5T_NATIVE_INT, &metadata->number_of_files_per_snapshot);
    if (status < 0) {
        fprintf(stderr, "Failed to read attribute NumFilesPerSnapshot\n");
        H5Aclose(attribute_id);
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    H5Aclose(attribute_id);

    // Redshift
    attribute_id = H5Aopen(header_group_id, "Redshift", H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Failed to open attribute Redshift\n");
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    status = H5Aread(attribute_id, H5T_NATIVE_DOUBLE, &metadata->redshift);
    if (status < 0) {
        fprintf(stderr, "Failed to read attribute Redshift\n");
        H5Aclose(attribute_id);
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    H5Aclose(attribute_id);

    // ExpansionFactor
    attribute_id = H5Aopen(header_group_id, "ExpansionFactor", H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Failed to open attribute ExpansionFactor\n");
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    status = H5Aread(attribute_id, H5T_NATIVE_DOUBLE, &metadata->expansion_factor);
    if (status < 0) {
        fprintf(stderr, "Failed to read attribute ExpansionFactor\n");
        H5Aclose(attribute_id);
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    H5Aclose(attribute_id);

    // HubbleParam
    attribute_id = H5Aopen(header_group_id, "HubbleParam", H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Failed to open attribute HubbleParam\n");
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    status = H5Aread(attribute_id, H5T_NATIVE_DOUBLE, &metadata->hubble_param);
    if (status < 0) {
        fprintf(stderr, "Failed to read attribute HubbleParam\n");
        H5Aclose(attribute_id);
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    H5Aclose(attribute_id);

    // MassTable
    attribute_id = H5Aopen(header_group_id, "MassTable", H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Failed to open attribute MassTable\n");
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    status = H5Aread(attribute_id, H5T_NATIVE_DOUBLE, metadata->mass_table);
    if (status < 0) {
        fprintf(stderr, "Failed to read attribute MassTable\n");
        H5Aclose(attribute_id);
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    H5Aclose(attribute_id);

    // NumPart_Total
    attribute_id = H5Aopen(header_group_id, "NumPart_Total", H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Failed to open attribute NumPart_Total\n");
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    status = H5Aread(attribute_id, H5T_NATIVE_INT, metadata->number_of_particles_total);
    if (status < 0) {
        fprintf(stderr, "Failed to read attribute NumPart_Total\n");
        H5Aclose(attribute_id);
        H5Gclose(header_group_id);
        H5Fclose(file_id);
        return false;
    }
    H5Aclose(attribute_id);

    H5Gclose(header_group_id);
    H5Fclose(file_id);

    char filepaths[metadata->number_of_files_per_snapshot][FILEPATH_BUFFER_LENGTH];

    for (int i = 0; i < metadata->number_of_files_per_snapshot; ++i) {
        sprintf(filepaths[i], "%s/snapshot_%s_%s.%d.hdf5", snapshot_directory, number, redshift_tag, i);
    }

    metadata->files = filepaths;

    long particle_lengths_per_file[metadata->number_of_files_per_snapshot][6];

    for (int i = 0; i < metadata->number_of_files_per_snapshot; ++i) {
        char *filepath = metadata->files[i];

        file_id = H5Fopen(first_filepath, H5F_ACC_RDONLY, H5P_DEFAULT);
        if (file_id < 0) {
            fprintf(stderr, "Failed to open file: %s\n", first_filepath);
            return false;
        }

        hid_t header_group_id = H5Oopen(file_id, "Header", H5P_DEFAULT);
        if (header_group_id < 0) {
            fprintf(stderr, "Failed to open header\n");
            H5Fclose(file_id);
            return false;
        }

        // NumPart_ThisFile
        attribute_id = H5Aopen(header_group_id, "NumPart_ThisFile", H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Failed to open attribute NumPart_ThisFile\n");
            H5Gclose(header_group_id);
            H5Fclose(file_id);
            return false;
        }
        status = H5Aread(attribute_id, H5T_NATIVE_INT, particle_lengths_per_file[i]);
        if (status < 0) {
            fprintf(stderr, "Failed to read attribute NumPart_ThisFile\n");
            H5Aclose(attribute_id);
            H5Gclose(header_group_id);
            H5Fclose(file_id);
            return false;
        }
        H5Aclose(attribute_id);

        H5Gclose(header_group_id);
        H5Fclose(file_id);
    }

    metadata->number_of_particles_per_file = particle_lengths_per_file;
}

void read_snapshot_chunk(
    int particle_type,
    char *field,
    SnapshotMetadata *snapshot_metadata,
    int chunk_offset,
    int chunk_length   
)
{
    PARTICLE_TYPE_FIELDS[particle_type];

    int start_file_index = 0;
    int stop_file_index = 0;
    int min_file_offset = 0;
    int max_file_offset = 0;
    while (chunk_offset + chunk_length > max_file_offset)
    {
        if (start_file_index != 0)
        {
            min_file_offset += snapshot_metadata->number_of_particles_per_file[start_file_index - 1][particle_type];
        }
        stop_file_index += snapshot_metadata->number_of_particles_per_file[stop_file_index][particle_type];
        if (min_file_offset < snapshot_metadata->number_of_particles_per_file[start_file_index - 1][particle_type])
        {
            start_file_index++;
        }
        stop_file_index++;
    }
    start_file_index--;

    hid_t file_id, group_id, dataset_id, filespace, memspace;
    hsize_t start[1] = { offset };
    hsize_t count[1] = { length };

    file_id = H5Fopen(filepath, H5F_ACC_RDWR, H5P_DEFAULT);
    dataset_id = H5Dopen(file_id, field, H5P_DEFAULT);

    filespace = H5Dget_space(dataset_id);
    H5Sselect_hyperslab(filespace, H5S_SELECT_SET, start, NULL, count, NULL);

    memspace = H5Screate_simple(1, count, NULL);

    H5Dwrite(dataset_id, H5T_NATIVE_LONG, memspace, filespace, H5P_DEFAULT, data);

    H5Sclose(memspace);
    H5Sclose(filespace);
    H5Dclose(dataset_id);
    H5Gclose(group_id);
    H5Fclose(file_id);
}
