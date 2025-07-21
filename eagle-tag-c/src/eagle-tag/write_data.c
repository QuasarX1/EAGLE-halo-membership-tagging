#include <stdbool.h>
#include <string.h>

#include <hdf5.h>

#include "headers/write_data.h"

struct Constants
{
    double boltzmann;
    double gamma;
    double protonmass;
    double sec_per_year;
    double solar_mass;
};

struct Header
{
    double expansion_factor;
    double hubble_param;
    double mass_table[6];
    int number_of_files_per_snapshot;
    int number_of_particles_subfind[6];
    int number_of_particles_this_file[6];
    int number_of_particles_total[6];
};

struct ParticleMembershipMetadata
{
    double cgs_conversion_factor;
    double aexp_scale_exponent;
    double h_scale_exponent;
};

//struct ParticleMembershipData
//{
//    long *group_number;
//    long *particle_ids;
//    long *subgroup_number;
//};

void make_aux_file_path(
    char *directory,
    char *number,
    char *redshift_tag,
    bool is_snipshot,
    char *filepath_out
)
{
    const char *prefix = is_snipshot ? "snipshot" : "snapshot";

    sprintf(
        filepath_out,
        "%s/%s_grp_info_%s_%s.hdf5",
        directory,
        prefix,
        number,
        redshift_tag
    );
}

bool make_aux_file(
    char *filepath,
    Header *header,
    Constants *constants,
    ParticleMembershipMetadata *gas,
    ParticleMembershipMetadata *dark_matter,
    ParticleMembershipMetadata *stars,
    ParticleMembershipMetadata *black_holes
)
{
    //char filepath[256];
    hid_t attribute_space_scalar, attribute_space_particle_types;
    hid_t file_id;
    hid_t group_id_header, group_id_constants, group_id_gas, group_id_dark_matter, group_id_stars, group_id_black_holes;
    hid_t dataset_id_gas_IDs, dataset_id_gas_group_numbers, dataset_id_gas_subgroup_numbers;
    hid_t dataset_id_dark_matter_IDs, dataset_id_dark_matter_group_numbers, dataset_id_dark_matter_subgroup_numbers;
    hid_t dataset_id_stars_IDs, dataset_id_stars_group_numbers, dataset_id_stars_subgroup_numbers;
    hid_t dataset_id_black_holes_IDs, dataset_id_black_holes_group_numbers, dataset_id_black_holes_subgroup_numbers;
    hsize_t dataset_space_dimensions_gas[1], dataset_space_dimensions_dark_matter[1], dataset_space_dimensions_stars[1], dataset_space_dimensions_black_holes[1];
    hid_t dataset_space_gas, dataset_space_dark_matter, dataset_space_stars, dataset_space_black_holes;
    hid_t attribute_id;
    herr_t status;

    // Create a new file using default properties, overwrite if it exists
    file_id = H5Fcreate(filepath, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    if (file_id < 0) {
        fprintf(stderr, "Error: Could not create HDF5 file at %s.\n", filepath);
        return false;
    }
    printf("Successfully created HDF5 file at %s.\n", filepath);

    attribute_space_scalar = H5Screate(H5S_SCALAR);

    hsize_t particle_type_array_dims[1] = { 6 }; // 1D array with 6 elements
    attribute_space_particle_types = H5Screate_simple(1, particle_type_array_dims, NULL);

    group_id_constants = H5Gcreate(file_id, "/Constants", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (group_id_constants < 0) {
        fprintf(stderr, "Could not create group /Constants\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Fclose(file_id);
        return false;
    }

    /* /Constants/BOLTZMANN */

    attribute_id = H5Acreate(group_id_constants, "BOLTZMANN", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Constants/BOLTZMANN\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &constants->boltzmann);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Constants/BOLTZMANN\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Constants/GAMMA */

    attribute_id = H5Acreate(group_id_constants, "GAMMA", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Constants/GAMMA\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &constants->gamma);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Constants/GAMMA\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Constants/PROTONMASS */

    attribute_id = H5Acreate(group_id_constants, "PROTONMASS", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Constants/PROTONMASS\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &constants->protonmass);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Constants/PROTONMASS\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Constants/SEC_PER_YEAR */

    attribute_id = H5Acreate(group_id_constants, "SEC_PER_YEAR", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Constants/SEC_PER_YEAR\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &constants->sec_per_year);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Constants/SEC_PER_YEAR\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Constants/SOLAR_MASS */

    attribute_id = H5Acreate(group_id_constants, "SOLAR_MASS", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Constants/SOLAR_MASS\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &constants->solar_mass);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Constants/SOLAR_MASS\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_constants);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    H5Gclose(group_id_constants);

    group_id_header = H5Gcreate(file_id, "/Header", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (group_id_header < 0) {
        fprintf(stderr, "Could not create group /Header\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Fclose(file_id);
        return false;
    }

    /* /Header/ExpansionFactor */

    attribute_id = H5Acreate(group_id_header, "ExpansionFactor", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Header/ExpansionFactor\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &header->expansion_factor);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Header/ExpansionFactor\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Header/HubbleParam */

    attribute_id = H5Acreate(group_id_header, "HubbleParam", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Header/HubbleParam\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &header->hubble_param);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Header/HubbleParam\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Header/MassTable */

    attribute_id = H5Acreate(group_id_header, "MassTable", H5T_NATIVE_DOUBLE, attribute_space_particle_types, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Header/MassTable\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &header->mass_table);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Header/MassTable\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Header/NumFilesPerSnapshot */

    attribute_id = H5Acreate(group_id_header, "NumFilesPerSnapshot", H5T_NATIVE_INT, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Header/NumFilesPerSnapshot\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_INT, &header->number_of_files_per_snapshot);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Header/NumFilesPerSnapshot\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Header/NumPart_Sub */

    attribute_id = H5Acreate(group_id_header, "NumPart_Sub", H5T_NATIVE_LONG, attribute_space_particle_types, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Header/NumPart_Sub\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_LONG, &header->number_of_particles_subfind);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Header/NumPart_Sub\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Header/NumPart_ThisFile */

    attribute_id = H5Acreate(group_id_header, "NumPart_ThisFile", H5T_NATIVE_LONG, attribute_space_particle_types, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Header/NumPart_ThisFile\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_LONG, &header->number_of_particles_this_file);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Header/NumPart_ThisFile\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    /* /Header/NumPart_Total */

    attribute_id = H5Acreate(group_id_header, "NumPart_Total", H5T_NATIVE_LONG, attribute_space_particle_types, H5P_DEFAULT, H5P_DEFAULT);
    if (attribute_id < 0) {
        fprintf(stderr, "Could not create attribute /Header/NumPart_Total\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    status = H5Awrite(attribute_id, H5T_NATIVE_LONG, &header->number_of_particles_total);
    if (status < 0) {
        fprintf(stderr, "Could not write attribute /Header/NumPart_Total\n");
        H5Sclose(attribute_space_scalar);
        H5Sclose(attribute_space_particle_types);
        H5Aclose(attribute_id);
        H5Gclose(group_id_header);
        H5Fclose(file_id);
        return false;
    }

    H5Aclose(attribute_id);

    H5Gclose(group_id_header);

    H5Sclose(attribute_space_particle_types);

    if (gas != NULL)
    {
        group_id_gas = H5Gcreate(file_id, "/PartType0", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (group_id_gas < 0) {
            fprintf(stderr, "Could not create group /PartType0\n");
            H5Sclose(attribute_space_scalar);
            H5Fclose(file_id);
            return false;
        }

        /* /PartType0/CGSConversionFactor */

        attribute_id = H5Acreate(group_id_gas, "CGSConversionFactor", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType0/CGSConversionFactor\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_gas);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &gas->cgs_conversion_factor);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType0/CGSConversionFactor\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_gas);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        /* /PartType0/aexp-scale-exponent */

        attribute_id = H5Acreate(group_id_gas, "aexp-scale-exponent", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType0/aexp-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_gas);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &gas->aexp_scale_exponent);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType0/aexp-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_gas);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        /* /PartType0/h-scale-exponent */

        attribute_id = H5Acreate(group_id_gas, "h-scale-exponent", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType0/h-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_gas);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &gas->h_scale_exponent);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType0/h-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_gas);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        dataset_space_gas = H5Screate_simple(1, dataset_space_dimensions_gas, NULL);

        dataset_id_gas_group_numbers = H5Dcreate(group_id_gas, "GroupNumber", H5T_NATIVE_LONG, dataset_space_gas, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_gas_group_numbers);

        dataset_id_gas_IDs = H5Dcreate(group_id_gas, "ParticleIDs", H5T_NATIVE_LONG, dataset_space_gas, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_gas_IDs);

        dataset_id_gas_subgroup_numbers = H5Dcreate(group_id_gas, "SubGroupNumber", H5T_NATIVE_LONG, dataset_space_gas, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_gas_subgroup_numbers);

        H5Sclose(dataset_space_gas);

        H5Gclose(group_id_gas);
    }

    if (dark_matter != NULL)
    {
        group_id_dark_matter = H5Gcreate(file_id, "/PartType1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (group_id_dark_matter < 0) {
            fprintf(stderr, "Could not create group /PartType1\n");
            H5Sclose(attribute_space_scalar);
            H5Fclose(file_id);
            return false;
        }

        /* /PartType1/CGSConversionFactor */

        attribute_id = H5Acreate(group_id_dark_matter, "CGSConversionFactor", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType1/CGSConversionFactor\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_dark_matter);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &dark_matter->cgs_conversion_factor);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType1/CGSConversionFactor\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_dark_matter);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        /* /PartType1/aexp-scale-exponent */

        attribute_id = H5Acreate(group_id_dark_matter, "aexp-scale-exponent", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType1/aexp-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_dark_matter);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &dark_matter->aexp_scale_exponent);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType1/aexp-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_dark_matter);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        /* /PartType1/h-scale-exponent */

        attribute_id = H5Acreate(group_id_dark_matter, "h-scale-exponent", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType1/h-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_dark_matter);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &dark_matter->h_scale_exponent);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType1/h-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_dark_matter);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        dataset_space_dark_matter = H5Screate_simple(1, dataset_space_dimensions_dark_matter, NULL);

        dataset_id_dark_matter_group_numbers = H5Dcreate(group_id_dark_matter, "GroupNumber", H5T_NATIVE_LONG, dataset_space_dark_matter, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_dark_matter_group_numbers);

        dataset_id_dark_matter_IDs = H5Dcreate(group_id_dark_matter, "ParticleIDs", H5T_NATIVE_LONG, dataset_space_dark_matter, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_dark_matter_IDs);

        dataset_id_dark_matter_subgroup_numbers = H5Dcreate(group_id_dark_matter, "SubGroupNumber", H5T_NATIVE_LONG, dataset_space_dark_matter, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_dark_matter_subgroup_numbers);

        H5Sclose(dataset_space_dark_matter);

        H5Gclose(group_id_dark_matter);
    }

    if (stars != NULL)
    {
        group_id_stars = H5Gcreate(file_id, "/PartType4", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (group_id_stars < 0) {
            fprintf(stderr, "Could not create group /PartType4\n");
            H5Sclose(attribute_space_scalar);
            H5Fclose(file_id);
            return false;
        }

        /* /PartType4/CGSConversionFactor */

        attribute_id = H5Acreate(group_id_stars, "CGSConversionFactor", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType4/CGSConversionFactor\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_stars);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &stars->cgs_conversion_factor);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType4/CGSConversionFactor\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_stars);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        /* /PartType4/aexp-scale-exponent */

        attribute_id = H5Acreate(group_id_stars, "aexp-scale-exponent", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType4/aexp-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_stars);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &stars->aexp_scale_exponent);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType4/aexp-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_stars);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        /* /PartType4/h-scale-exponent */

        attribute_id = H5Acreate(group_id_stars, "h-scale-exponent", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType4/h-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_stars);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &stars->h_scale_exponent);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType4/h-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_stars);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        dataset_space_stars = H5Screate_simple(1, dataset_space_dimensions_stars, NULL);

        dataset_id_stars_group_numbers = H5Dcreate(group_id_stars, "GroupNumber", H5T_NATIVE_LONG, dataset_space_stars, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_stars_group_numbers);

        dataset_id_stars_IDs = H5Dcreate(group_id_stars, "ParticleIDs", H5T_NATIVE_LONG, dataset_space_stars, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_stars_IDs);

        dataset_id_stars_subgroup_numbers = H5Dcreate(group_id_stars, "SubGroupNumber", H5T_NATIVE_LONG, dataset_space_stars, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_stars_subgroup_numbers);

        H5Sclose(dataset_space_stars);

        H5Gclose(group_id_stars);
    }

    if (black_holes != NULL)
    {
        group_id_black_holes = H5Gcreate(file_id, "/PartType5", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (group_id_black_holes < 0) {
            fprintf(stderr, "Could not create group /PartType5\n");
            H5Sclose(attribute_space_scalar);
            H5Fclose(file_id);
            return false;
        }

        /* /PartType5/CGSConversionFactor */

        attribute_id = H5Acreate(group_id_black_holes, "CGSConversionFactor", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType5/CGSConversionFactor\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_black_holes);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &black_holes->cgs_conversion_factor);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType5/CGSConversionFactor\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_black_holes);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        /* /PartType5/aexp-scale-exponent */

        attribute_id = H5Acreate(group_id_black_holes, "aexp-scale-exponent", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType5/aexp-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_black_holes);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &black_holes->aexp_scale_exponent);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType5/aexp-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_black_holes);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        /* /PartType5/h-scale-exponent */

        attribute_id = H5Acreate(group_id_black_holes, "h-scale-exponent", H5T_NATIVE_DOUBLE, attribute_space_scalar, H5P_DEFAULT, H5P_DEFAULT);
        if (attribute_id < 0) {
            fprintf(stderr, "Could not create attribute /PartType5/h-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Gclose(group_id_black_holes);
            H5Fclose(file_id);
            return false;
        }

        status = H5Awrite(attribute_id, H5T_NATIVE_DOUBLE, &black_holes->h_scale_exponent);
        if (status < 0) {
            fprintf(stderr, "Could not write attribute /PartType5/h-scale-exponent\n");
            H5Sclose(attribute_space_scalar);
            H5Aclose(attribute_id);
            H5Gclose(group_id_black_holes);
            H5Fclose(file_id);
            return false;
        }

        H5Aclose(attribute_id);

        dataset_space_black_holes = H5Screate_simple(1, dataset_space_dimensions_dark_matter, NULL);

        dataset_id_black_holes_group_numbers = H5Dcreate(group_id_black_holes, "GroupNumber", H5T_NATIVE_LONG, dataset_space_black_holes, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_black_holes_group_numbers);

        dataset_id_black_holes_IDs = H5Dcreate(group_id_black_holes, "ParticleIDs", H5T_NATIVE_LONG, dataset_space_black_holes, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_black_holes_IDs);

        dataset_id_black_holes_subgroup_numbers = H5Dcreate(group_id_black_holes, "SubGroupNumber", H5T_NATIVE_LONG, dataset_space_black_holes, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dclose(dataset_id_black_holes_subgroup_numbers);

        H5Sclose(dataset_space_black_holes);

        H5Gclose(group_id_black_holes);
    }

    // Always close the file when done
    status = H5Fclose(file_id);
    if (status < 0) {
        fprintf(stderr, "Error: Could not close HDF5 file.\n");
        return false;
    }

    return true;
}

void save_chunk_int(
    char *filepath,
    char *particle_type,
    char *field,
    int offset,
    int length,
    int *data
)
{
    hid_t file_id, group_id, dataset_id, filespace, memspace;
    hsize_t start[1] = { offset };
    hsize_t count[1] = { length };

    file_id = H5Fopen(filepath, H5F_ACC_RDWR, H5P_DEFAULT);
    dataset_id = H5Dopen(file_id, field, H5P_DEFAULT);

    filespace = H5Dget_space(dataset_id);
    H5Sselect_hyperslab(filespace, H5S_SELECT_SET, start, NULL, count, NULL);

    memspace = H5Screate_simple(1, count, NULL);

    H5Dwrite(dataset_id, H5T_NATIVE_INT, memspace, filespace, H5P_DEFAULT, data);

    H5Sclose(memspace);
    H5Sclose(filespace);
    H5Dclose(dataset_id);
    H5Gclose(group_id);
    H5Fclose(file_id);
}

void save_chunk_long(
    char *filepath,
    char *particle_type,
    char *field,
    int offset,
    int length,
    long *data
)
{
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
