#ifndef CHUNKING_H
#define CHUNKING_H

void calculate_even_chunks(
    long length,
    int number_of_chunks,
    long *chunk_offsets_out,
    long *chunk_lengths_out
);

#endif // CHUNKING_H