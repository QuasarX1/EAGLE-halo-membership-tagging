#include "headers/chunking.h"

void calculate_even_chunks(
    long length,
    int number_of_chunks,
    long *chunk_offsets_out,
    long *chunk_lengths_out
)
{
    long chunk_size_min, chunk_size;
    int number_with_extra_element;

    chunk_size_min = floor(length / number_of_chunks);
    number_with_extra_element = length % number_of_chunks;

    for (int i = 0; i < number_of_chunks; ++i) {
        chunk_size = (i < number_with_extra_element) ? chunk_size_min + 1 : chunk_size_min;
        chunk_offsets_out[i] = i * chunk_size;
        chunk_lengths_out[i] = chunk_size;
    }
}
