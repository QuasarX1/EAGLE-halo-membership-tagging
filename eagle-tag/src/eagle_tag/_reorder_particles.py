# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import numpy as np

def calculate_reorder(source_ids: np.ndarray[tuple[int], np.dtype[np.int64]], target_ids: np.ndarray[tuple[int], np.dtype[np.int64]]) -> np.ndarray[tuple[int], np.dtype[np.int64]]:
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

    forwards_indexes: np.ndarray[tuple[int], np.dtype[np.int64]]

    sorted_indexes_source = np.argsort(source_ids)
    sorted_source = source_ids[sorted_indexes_source]

    possible_sorted_match_locations_source = np.searchsorted(sorted_source, target_ids)
    possible_sorted_match_locations_source[possible_sorted_match_locations_source >= source_ids.shape[0]] = -1

    target_matches_mask = sorted_source[possible_sorted_match_locations_source] == target_ids
    del sorted_source

    forwards_indexes = np.where(target_matches_mask, sorted_indexes_source[possible_sorted_match_locations_source], -1)
    del sorted_indexes_source
    del possible_sorted_match_locations_source
    del target_matches_mask

    return forwards_indexes
