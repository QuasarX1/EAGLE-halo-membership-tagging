# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import numpy as np

def calculate_reorder(source_ids: np.ndarray[tuple[int], np.dtype[np.int64]], target_ids: np.ndarray[tuple[int], np.dtype[np.int64]]):
    """
    Calculate the reorder indexes between two sets of particle IDs.
    
    Args:
        np.ndarray[(N0,), np.int64] source_ids:
            Particle IDs from the source snapshot.
        np.ndarray[(N1,), np.int64] target_ids:
            Particle IDs from the target snapshot.

    Returns:
        (np.ndarray[(N1,), np.int64], np.ndarray[(N0,), np.int64]) -> Two arrays containing the forward and backward reorder indexes.
    """

    sorted_indexes_source = np.argsort(source_ids)
    sorted_indexes_target = np.argsort(target_ids)

    sorted_source = source_ids[sorted_indexes_source]
    sorted_target = target_ids[sorted_indexes_target]

    possible_match_locations_source = np.searchsorted(sorted_source, sorted_target)
    possible_match_locations_target = np.searchsorted(sorted_target, sorted_source)

    source_matches_mask = sorted_source[possible_match_locations_source] == sorted_target
    target_matches_mask = sorted_target[possible_match_locations_target] == sorted_source

    reverse_sort_indexes_source = np.argsort(sorted_indexes_source)
    reverse_sort_indexes_target = np.argsort(sorted_indexes_target)

    sorted_forward_indexes = np.where(target_matches_mask, , -1)






    sorted_forward_indexes  = np.full_like(target_ids, -1, dtype=np.int64)
    sorted_backward_indexes = np.full_like(source_ids, -1, dtype=np.int64)

    sorted_forward_indexes [target_matches_mask] = 
    sorted_backward_indexes[source_matches_mask] = 







    return sorted_forward_indexes[reverse_sort_indexes_target], sorted_backward_indexes[reverse_sort_indexes_source]