# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import os

import xarray as xr

from ._load_data_with_xarray import load_hdf5_pattern_with_xarray

def load_snapshot(snapshot_directory: str, number: str, redshift_tag: str, is_snipshot: bool) -> dict[str, xr.Dataset]:
    snapshot_prefix = "snip" if is_snipshot else "snap"
    filepath_template = os.path.join(snapshot_directory, f"{snapshot_prefix}_{number}_{redshift_tag}.*.hdf5")
    return {
        "PartType0" : load_hdf5_pattern_with_xarray(filepath_template, "PartType0", ["ParticleIDs"]),
        "PartType1" : load_hdf5_pattern_with_xarray(filepath_template, "PartType1", ["ParticleIDs"]),
        "PartType4" : load_hdf5_pattern_with_xarray(filepath_template, "PartType4", ["ParticleIDs"]),
        "PartType5" : load_hdf5_pattern_with_xarray(filepath_template, "PartType5", ["ParticleIDs"]),
    }
