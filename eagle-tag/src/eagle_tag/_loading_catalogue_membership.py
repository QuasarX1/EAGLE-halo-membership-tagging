# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import h5py as h5
import xarray as xr
import numpy as np

from ._load_data_with_xarray import load_hdf5_pattern_with_xarray
from ._eagle_filepaths import EAGLE_Snapshot

def load_catalogue_membership(snapshot: EAGLE_Snapshot, do_gas: bool = True, do_dark_matter: bool = True, do_stars: bool = True, do_black_holes: bool = True) -> dict[str, xr.Dataset|None]:

    if not snapshot.has_catalogue_membership:
        raise FileNotFoundError(f"Unable to locate catalogue membership files for {snapshot}.")

    filepath_template = snapshot.catalogue_membership_file_template
    filepath_pattern = filepath_template.format("*")

    number_of_files: int
    #numbers_of_particles: tuple[int, int, int, int, int, int]
    with h5.File(filepath_template.format(0), "r") as file:
        number_of_files = int(file["Header"].attrs["NumFilesPerSnapshot"])
        #numbers_of_particles = tuple(file["Header"].attrs["NumPart_ThisFile"])

    data_in_files = np.full(shape = (number_of_files, 6), fill_value = False, dtype = np.bool_)
    for i in range(number_of_files):
        with h5.File(filepath_template.format(i), "r") as file:
            data_in_files[i][:] = file["Header"].attrs["NumPart_ThisFile"] > 0
    any_data_present = np.any(data_in_files, axis = 0)

    return {
        "PartType0" : load_hdf5_pattern_with_xarray(filepath_pattern, "PartType0", ["ParticleIDs", "GroupNumber", "SubGroupNumber"], skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][0]], dimension_sizes = { "catalogue_membership_particle_index" : None, "box_axis_index" : 3 }, concatenation_dimension_index = 0) if do_gas         and any_data_present[0] else None,
        "PartType1" : load_hdf5_pattern_with_xarray(filepath_pattern, "PartType1", ["ParticleIDs", "GroupNumber", "SubGroupNumber"], skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][1]], dimension_sizes = { "catalogue_membership_particle_index" : None, "box_axis_index" : 3 }, concatenation_dimension_index = 0) if do_dark_matter and any_data_present[1] else None,
        "PartType4" : load_hdf5_pattern_with_xarray(filepath_pattern, "PartType4", ["ParticleIDs", "GroupNumber", "SubGroupNumber"], skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][4]], dimension_sizes = { "catalogue_membership_particle_index" : None, "box_axis_index" : 3 }, concatenation_dimension_index = 0) if do_stars       and any_data_present[4] else None,
        "PartType5" : load_hdf5_pattern_with_xarray(filepath_pattern, "PartType5", ["ParticleIDs", "GroupNumber", "SubGroupNumber"], skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][5]], dimension_sizes = { "catalogue_membership_particle_index" : None, "box_axis_index" : 3 }, concatenation_dimension_index = 0) if do_black_holes and any_data_present[5] else None,
    }
