# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import os

import h5py as h5
import numpy as np

def make_aux_file_path(directory: str, number: str, redshift_tag: str, is_snipshot: bool = False) -> str:
    return os.path.join(directory, f"sn{'i' if is_snipshot else 'a'}pshot_grp_info_{number}_{redshift_tag}.hdf5")

class Metadata(object):

    def __init__(self) -> None:

        self.constant_boltzmann:    np.float64 = np.float64(1.0)
        self.constant_gamma:        np.float64 = np.float64(1.0)
        self.constant_protonmass:   np.float64 = np.float64(1.0)
        self.constant_sec_per_year: np.float64 = np.float64(1.0)
        self.constant_solar_mass:   np.float64 = np.float64(1.0)

        self.header_expansion_factor:       np.float64                                   = np.float64(1.0)
        self.header_hubble_param:           np.float64                                   = np.float64(1.0)
        self.header_mass_table:             np.ndarray[tuple[int], np.dtype[np.float64]] = np.array([0.0, 0.0, 0.0, 0.0, 0.0, 0.0], dtype = np.float64)
        self.header_num_files_per_snapshot: np.int32                                     = np.int32(1)
        self.header_num_part_sub:           np.ndarray[tuple[int], np.dtype[np.int32]]   = np.array([0, 0, 0, 0, 0, 0], dtype = np.int32)
        self.header_num_part_this_file:     np.ndarray[tuple[int], np.dtype[np.int32]]   = np.array([0, 0, 0, 0, 0, 0], dtype = np.int32)
        self.header_num_part_total:         np.ndarray[tuple[int], np.dtype[np.int32]]   = np.array([0, 0, 0, 0, 0, 0], dtype = np.int32)

def make_aux_file(
    directory: str,
    number: str,
    redshift_tag: str,
    number_of_gas_particles: int,
    number_of_star_particles: int,
    metadata: Metadata,
    allow_overwrite: bool = False,
    is_snipshot: bool = False
) -> str:

    filepath = make_aux_file_path(directory, number, redshift_tag, is_snipshot)

    if os.path.exists(filepath) and not allow_overwrite:
        raise FileExistsError(f"Auxiliary file {filepath} already exists. Use allow_overwrite to overwrite.")

    # Create the file and set attributes
    with h5.File(filepath, "w") as file:

        # Create the nesessary groups
        consts = file.create_group("Constants")
        header = file.create_group("Header")
        gas = file.create_group("PartType0")
        stars = file.create_group("PartType4")

        # Set constants
        consts.attrs["BOLTZMANN"]    = np.float64(metadata.constant_boltzmann)
        consts.attrs["GAMMA"]        = np.float64(metadata.constant_gamma)
        consts.attrs["PROTONMASS"]   = np.float64(metadata.constant_protonmass)
        consts.attrs["SEC_PER_YEAR"] = np.float64(metadata.constant_sec_per_year)
        consts.attrs["SOLAR_MASS"]   = np.float64(metadata.constant_solar_mass)

        # Set header attributes
        header.attrs["ExpansionFactor"]     = np.float64(metadata.header_expansion_factor)
        header.attrs["HubbleParam"]         = np.float64(metadata.header_hubble_param)
        header.attrs["MassTable"]           = np.array(metadata.header_mass_table, dtype = np.float64)
        header.attrs["NumFilesPerSnapshot"] = np.int32(metadata.header_num_files_per_snapshot)
        header.attrs["NumPart_Sub"]         = np.array(metadata.header_num_part_sub, dtype = np.int32)
        header.attrs["NumPart_ThisFile"]    = np.array(metadata.header_num_part_this_file, dtype = np.int32)
        header.attrs["NumPart_Total"]       = np.array(metadata.header_num_part_total, dtype = np.int32)

        # Create gas datasets and assign attributes

        gas_group_number     = gas.create_dataset("GroupNumber",    shape = (number_of_gas_particles,), dtype = np.int32)
        gas_particle_ids     = gas.create_dataset("ParticleIDs",    shape = (number_of_gas_particles,), dtype = np.int64)
        gas_sub_group_number = gas.create_dataset("SubGroupNumber", shape = (number_of_gas_particles,), dtype = np.int32)

        gas_group_number.attrs["CGSConversionFactor"] = np.float64(1.0)
        gas_group_number.attrs["aexp-scale-exponent"] = np.float64(0.0)
        gas_group_number.attrs["h-scale-exponent"]    = np.float64(0.0)

        gas_particle_ids.attrs["CGSConversionFactor"] = np.float64(1.0)
        gas_particle_ids.attrs["aexp-scale-exponent"] = np.float64(0.0)
        gas_particle_ids.attrs["h-scale-exponent"]    = np.float64(0.0)

        gas_sub_group_number.attrs["CGSConversionFactor"] = np.float64(1.0)
        gas_sub_group_number.attrs["aexp-scale-exponent"] = np.float64(0.0)
        gas_sub_group_number.attrs["h-scale-exponent"]    = np.float64(0.0)

        # Create star datasets and assign attributes

        stars_group_number     = stars.create_dataset("GroupNumber",    shape = (number_of_star_particles,), dtype = np.int32)
        stars_particle_ids     = stars.create_dataset("ParticleIDs",    shape = (number_of_star_particles,), dtype = np.int64)
        stars_sub_group_number = stars.create_dataset("SubGroupNumber", shape = (number_of_star_particles,), dtype = np.int32)

        stars_group_number.attrs["CGSConversionFactor"] = np.float64(1.0)
        stars_group_number.attrs["aexp-scale-exponent"] = np.float64(0.0)
        stars_group_number.attrs["h-scale-exponent"]    = np.float64(0.0)

        stars_particle_ids.attrs["CGSConversionFactor"] = np.float64(1.0)
        stars_particle_ids.attrs["aexp-scale-exponent"] = np.float64(0.0)
        stars_particle_ids.attrs["h-scale-exponent"]    = np.float64(0.0)

        stars_sub_group_number.attrs["CGSConversionFactor"] = np.float64(1.0)
        stars_sub_group_number.attrs["aexp-scale-exponent"] = np.float64(0.0)
        stars_sub_group_number.attrs["h-scale-exponent"]    = np.float64(0.0)

    return filepath

def save_chunk(
    filepath: str,
    particle_type: str,
    field: str,
    offset: int,
    length: int,
    data
) -> None:
    with h5.File(filepath, "a") as file:
        file[f"PartType{particle_type}/{field}"][offset:offset + length] = data[:]
