# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import os

import h5py as h5
import xarray as xr
import numpy as np

from ._load_data_with_xarray import load_hdf5_pattern_with_xarray
from ._eagle_filepaths import EAGLE_Snapshot

def load_snapshot(
        snapshot: EAGLE_Snapshot,
        gas_fields:         list[str]|None = None,
        dark_matter_fields: list[str]|None = None,
        star_fields:        list[str]|None = None,
        black_hole_fields:  list[str]|None = None
) -> dict[str, xr.Dataset|None]:

    if not snapshot.has_snapshot:
        raise FileNotFoundError(f"Unable to locate snapshot files for {snapshot}.")

    filepath_template = snapshot.snapshot_file_template
    filepath_pattern = filepath_template.format("*")

    number_of_files: int
    with h5.File(filepath_template.format(0), "r") as file:
        number_of_files = int(file["Header"].attrs["NumFilesPerSnapshot"])

    data_in_files = np.full(shape = (number_of_files, 6), fill_value = False, dtype = np.bool_)
    for i in range(number_of_files):
        with h5.File(filepath_template.format(i), "r") as file:
            data_in_files[i][:] = file["Header"].attrs["NumPart_ThisFile"] > 0
    any_data_present = np.any(data_in_files, axis = 0)

    if gas_fields is None:
        gas_fields = ["ParticleIDs"]
    elif "ParticleIDs" not in gas_fields:
        gas_fields = ["ParticleIDs", *gas_fields]

    if dark_matter_fields is None:
        dark_matter_fields = ["ParticleIDs"]
    elif "ParticleIDs" not in dark_matter_fields:
        dark_matter_fields = ["ParticleIDs", *dark_matter_fields]

    if star_fields is None:
        star_fields = ["ParticleIDs"]
    elif "ParticleIDs" not in star_fields:
        star_fields = ["ParticleIDs", *star_fields]

    if black_hole_fields is None:
        black_hole_fields = ["ParticleIDs"]
    elif "ParticleIDs" not in black_hole_fields:
        black_hole_fields = ["ParticleIDs", *black_hole_fields]

    return {
        "PartType0" : load_hdf5_pattern_with_xarray(filepath_pattern, "PartType0", gas_fields,         skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][0]]) if any_data_present[0] else None,
        "PartType1" : load_hdf5_pattern_with_xarray(filepath_pattern, "PartType1", dark_matter_fields, skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][1]]) if any_data_present[1] else None,
        "PartType4" : load_hdf5_pattern_with_xarray(filepath_pattern, "PartType4", star_fields,        skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][4]]) if any_data_present[4] else None,
        "PartType5" : load_hdf5_pattern_with_xarray(filepath_pattern, "PartType5", black_hole_fields,  skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][5]]) if any_data_present[5] else None,
    }
