# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import os

import xarray as xr

from ._load_data_with_xarray import load_hdf5_pattern_with_xarray

def load_catalogue_membership(catalogue_directory: str, number: str, redshift_tag: str, is_snipshot: bool) -> dict[str, xr.Dataset]:
    snapshot_prefix = "snip_" if is_snipshot else ""
    filepath_template = os.path.join(catalogue_directory, f"eagle_subfind_{snapshot_prefix}particles_{number}_{redshift_tag}.*.hdf5")
    return {
        "PartType0" : load_hdf5_pattern_with_xarray(filepath_template, "PartType0", ["ParticleIDs", "GroupNumber", "SubGroupNumber"]),
        "PartType1" : load_hdf5_pattern_with_xarray(filepath_template, "PartType1", ["ParticleIDs", "GroupNumber", "SubGroupNumber"]),
        "PartType4" : load_hdf5_pattern_with_xarray(filepath_template, "PartType4", ["ParticleIDs", "GroupNumber", "SubGroupNumber"]),
        "PartType5" : load_hdf5_pattern_with_xarray(filepath_template, "PartType5", ["ParticleIDs", "GroupNumber", "SubGroupNumber"]),
    }
