# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import os

import h5py as h5
import xarray as xr
import numpy as np

from ._load_data_with_xarray import load_hdf5_pattern_with_xarray
from ._eagle_filepaths import EAGLE_Snapshot

def load_catalogue(
    snapshot: EAGLE_Snapshot,
    group_fields:   list[str]|None = None,
    subfind_fields: list[str]|None = None,
    subfind_alternate_group_path: str|None = None,
    override_chunks_in_all_dimensions: int|str|None = None
) -> dict[str, xr.Dataset|None]:

    if not snapshot.has_catalogue:
        raise FileNotFoundError(f"Unable to locate catalogue files for {snapshot}.")

    filepath_template = snapshot.catalogue_file_template
    filepath_pattern = filepath_template.format("*")

    number_of_files: int
    #number_of_fof_entries: int
    #number_of_subhalo_entries: int
    with h5.File(filepath_template.format(0), "r") as file:
        number_of_files = int(file["Header"].attrs["NTask"])
        #number_of_fof_entries = int(file["TotNgroups"].attrs["NTask"])
        #number_of_subhalo_entries = int(file["TotNsubgroups"].attrs["NTask"])

    data_in_files = np.full(shape = (number_of_files, 2), fill_value = False, dtype = np.bool_)
    for i in range(number_of_files):
        with h5.File(filepath_template.format(i), "r") as file:
            data_in_files[i][0] = file["Header"].attrs["Ngroups"] > 0
            data_in_files[i][1] = file["Header"].attrs["Nsubgroups"] > 0
    any_data_present = np.any(data_in_files, axis = 0)

    results: dict[str, xr.Dataset|None] = { "FOF" : None, "Subhalo" : None }

    if group_fields is not None and any_data_present[0]:

        results["FOF"] = load_hdf5_pattern_with_xarray(
            filepath_pattern,
            "FOF",
            group_fields,
            skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][0]],
            override_chunks_in_all_dimensions = override_chunks_in_all_dimensions,
            dimension_sizes = {
                "catalogue_fof_index"  : None,
                "box_axis_index"       : 3,
                "particle_type_number" : 6 # EAGLE data has particle types 0 -> 5
            },
            concatenation_dimension_index = 0
        )

    if subfind_fields is not None and any_data_present[1]:

        results["Subhalo"] = load_hdf5_pattern_with_xarray(
            filepath_pattern,
            f"Subhalo/{subfind_alternate_group_path}" if subfind_alternate_group_path is not None else "Subhalo",
            subfind_fields,
            skip_values = [str(i) for i in range(number_of_files) if not data_in_files[i][1]],
            override_chunks_in_all_dimensions = override_chunks_in_all_dimensions,
            dimension_sizes = {
                "catalogue_subhalo_index" : None,
                "box_axis_index"          : 3,
                "particle_type_number"    : 6 # EAGLE data has particle types 0 -> 5
            },
            concatenation_dimension_index = 0
        )

    return results

def check_cache_exists(cache_directory: str, identifier: str) -> bool:
    return os.path.exists(os.path.join(cache_directory, f"{identifier}-fof-cache.nc")) and os.path.exists(os.path.join(cache_directory, f"{identifier}-subhalo-cache.nc"))

def cache_catalogue_as_single_file(
    cache_directory: str,
    identifier: str,
    fof_dataset: xr.Dataset,
    subhalo_dataset: xr.Dataset
) -> None:
    renames = {}
    for key in subhalo_dataset.keys():
        if "/" in str(key):
            renames[str(key)] = str(key).replace("/", "->")
    if len(renames) > 0:
        subhalo_dataset = subhalo_dataset.rename(renames)

    fof_filepath     = os.path.join(cache_directory, f"{identifier}-fof-cache.nc")
    subhalo_filepath = os.path.join(cache_directory, f"{identifier}-subhalo-cache.nc")
    fof_dataset.to_netcdf(fof_filepath, mode = "w", engine = "h5netcdf")
    subhalo_dataset.to_netcdf(subhalo_filepath, mode = "w", engine = "h5netcdf")

def clear_catalogue_cache(cache_directory: str, identifier: str) -> None:

    fof_filepath     = os.path.join(cache_directory, f"{identifier}-fof-cache.nc")
    subhalo_filepath = os.path.join(cache_directory, f"{identifier}-subhalo-cache.nc")
    if os.path.exists(fof_filepath):
        os.remove(fof_filepath)
    if os.path.exists(subhalo_filepath):
        os.remove(subhalo_filepath)

def load_catalogue_cache(
    cache_directory: str,
    identifier: str,
    fof: bool = True,
    subhalo: bool = True
) -> dict[str, xr.Dataset|None]:

    fof_filepath     = os.path.join(cache_directory, f"{identifier}-fof-cache.nc")
    subhalo_filepath = os.path.join(cache_directory, f"{identifier}-subhalo-cache.nc")

    data = {
        "FOF"     : xr.open_dataset(fof_filepath,     engine = "h5netcdf", chunks = "auto") if fof     else None,
        "Subhalo" : xr.open_dataset(subhalo_filepath, engine = "h5netcdf", chunks = "auto") if subhalo else None
    }

    if subhalo:
        renames = {}
        for key in data["Subhalo"].keys():
            if "->" in key:
                renames[str(key)] = str(key).replace("->", "/")
        if len(renames) > 0:
            data["Subhalo"] = data["Subhalo"].rename(renames)

    return data
