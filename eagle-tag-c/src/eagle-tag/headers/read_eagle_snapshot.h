#include <stdbool.h>

#ifndef READ_EAGLE_SNAPSHOT_H
#define READ_EAGLE_SNAPSHOT_H

typedef struct SnapshotMetadata
{
    int number_of_files_per_snapshot;
    char **files;
    double redshift;
    double expansion_factor;
    double hubble_param;
    double mass_table[6];
    long number_of_particles_total[6];
    long *number_of_particles_per_file[6];
} SnapshotMetadata;

bool load_snapshot_metadata(
    char *snapshot_directory,
    char *number,
    char *redshift_tag,
    SnapshotMetadata *metadata
);

void read_snapshot_chunk(
    SnapshotMetadata *snapshot_metadata,
    int chunk_offset,
    int chunk_length   
);

#endif // READ_EAGLE_SNAPSHOT_H
