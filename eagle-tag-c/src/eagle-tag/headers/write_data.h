#include <stdbool.h>

#ifndef WRITE_DATA_H
#define WRITE_DATA_H

typedef struct Constants
{
    double boltzmann;
    double gamma;
    double protonmass;
    double sec_per_year;
    double solar_mass;
} Constants;

typedef struct Header
{
    double expansion_factor;
    double hubble_param;
    double mass_table[6];
    int number_of_files_per_snapshot;
    int number_of_particles_subfind[6];
    int number_of_particles_this_file[6];
    int number_of_particles_total[6];
} Header;

typedef struct ParticleMembershipMetadata
{
    double cgs_conversion_factor;
    double aexp_scale_exponent;
    double h_scale_exponent;
} ParticleMembershipMetadata;

//typedef struct ParticleMembershipData
//{
//    long *group_number;
//    long *particle_ids;
//    long *subgroup_number;
//} ParticleMembershipData;

void make_aux_file_path(
    char *directory,
    char *number,
    char *redshift_tag,
    bool is_snipshot,
    char *filepath_out
);

bool make_aux_file(
    char *filepath,
    Header *header,
    Constants *constants,
    ParticleMembershipMetadata *gas,
    ParticleMembershipMetadata *dark_matter,
    ParticleMembershipMetadata *stars,
    ParticleMembershipMetadata *black_holes
);

void save_chunk_int(
    char *filepath,
    char *particle_type,
    char *field,
    int offset,
    int length,
    int *data
);

void save_chunk_long(
    char *filepath,
    char *particle_type,
    char *field,
    int offset,
    int length,
    long *data
);

#endif // WRITE_DATA_H
