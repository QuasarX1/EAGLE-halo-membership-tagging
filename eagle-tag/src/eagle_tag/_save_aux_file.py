# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import os

import h5py as h5
import numpy as np

def make_aux_file_path(directory: str, number: str, redshift_tag: str) -> str:
    return os.path.join(directory, f"snapshot_grp_info_{number}_{redshift_tag}.hdf5")

def make_aux_file(
    directory: str,
    number: str,
    redshift_tag: str,
    number_of_gas_particles: int,
    number_of_star_particles: int,
    allow_overwrite: bool = False
) -> str:

    filepath = make_aux_file_path(directory, number, redshift_tag)

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
        consts.attrs["BOLTZMANN"]    = np.float64(1.0)#TODO: get values from snapshot???
        consts.attrs["GAMMA"]        = np.float64(1.0)#TODO: get values from snapshot???
        consts.attrs["PROTONMASS"]   = np.float64(1.0)#TODO: get values from snapshot???
        consts.attrs["SEC_PER_YEAR"] = np.float64(1.0)#TODO: get values from snapshot???
        consts.attrs["SOLAR_MASS"]   = np.float64(1.0)#TODO: get values from snapshot???

        # Set header attributes
        header.attrs["ExpansionFactor"]     = np.float64(1.0)#TODO: get values from snapshot
        header.attrs["HubbleParam"]         = np.float64(1.0)#TODO: get values from snapshot
        header.attrs["MassTable"]           = np.array([0.0, 0.0, 0.0, 0.0, 0.0, 0.0], dtype = np.float64)#TODO: get values from snapshot
        header.attrs["NumFilesPerSnapshot"] = np.int32(1)
        header.attrs["NumPart_Sub"]         = np.array([0, 0, 0, 0, 0, 0], dtype = np.int32)#TODO: get values from SUBFIND???
        header.attrs["NumPart_ThisFile"]    = np.array([0, 0, 0, 0, 0, 0], dtype = np.int32)#TODO: get values from snapshot
        header.attrs["NumPart_Total"]       = np.array([0, 0, 0, 0, 0, 0], dtype = np.int32)#TODO: get values from snapshot

        # Create gas datasets and assign attributes

        gas_group_number     = gas.create_dataset("GroupNumber",    shape = (number_of_gas_particles,), dtype = np.int32)
        gas_particle_ids     = gas.create_dataset("ParticleIDs",    shape = (number_of_gas_particles,), dtype = np.int32)
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
        stars_particle_ids     = stars.create_dataset("ParticleIDs",    shape = (number_of_star_particles,), dtype = np.int32)
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
