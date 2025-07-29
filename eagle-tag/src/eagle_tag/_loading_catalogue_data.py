# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import h5py as h5
import xarray as xr
import numpy as np

from ._load_data_with_xarray import load_hdf5_pattern_with_xarray
from ._eagle_filepaths import EAGLE_Snapshot

def load_catalogue(
        snapshot: EAGLE_Snapshot,
        group_fields:   list[str]|None = None,
        subfind_fields: list[str]|None = None
) -> dict[str, xr.Dataset|None]:

    if not snapshot.has_catalogue:
        raise FileNotFoundError(f"Unable to locate catalogue files for {snapshot}.")

    filepath_template = snapshot.catalogue_file_template
    filepath_pattern = filepath_template.format("*")

    number_of_files: int
    with h5.File(filepath_template.format(0), "r") as file:
        number_of_files = int(file["Header"].attrs["NTask"])

    data_in_files = np.full(shape = (number_of_files, 2), fill_value = False, dtype = np.bool_)
    for i in range(number_of_files):
        with h5.File(filepath_template.format(i), "r") as file:
            data_in_files[i][0] = file["Header"].attrs["Ngroups"] > 0
            data_in_files[i][1] = file["Header"].attrs["Nsubgroups"] > 0
    any_data_present = np.any(data_in_files, axis = 0)

    if group_fields is None:
        group_fields = ["FirstSubhaloID", "NumOfSubhalos"]
    if "FirstSubhaloID" not in group_fields:
        group_fields = ["FirstSubhaloID", *group_fields]
    if "NumOfSubhalos" not in group_fields:
        group_fields = ["NumOfSubhalos", *group_fields]

    if subfind_fields is None:
        subfind_fields = ["Mass"]
    elif "Mass" not in subfind_fields:
        subfind_fields = ["Mass", *subfind_fields]

    return {
        "FOF"     : load_hdf5_pattern_with_xarray(filepath_pattern, "FOF",     group_fields,   skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][0]]) if any_data_present[0] else None,
        "Subhalo" : load_hdf5_pattern_with_xarray(filepath_pattern, "Subhalo", subfind_fields, skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][1]]) if any_data_present[1] else None,
    }
