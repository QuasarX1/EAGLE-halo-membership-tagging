# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

#from scida import load, GadgetStyleSnapshot
#from scida.interface import Dataset
#
#def load_catalogue_data(catalogue_directory: str, number: str, redshift_tag: str) -> Dataset:
#    """
#    PartType0:
#        Coordinates
#        Density
#        Entropy
#        GroupNumber
#        InternalEnergy
#        Mass
#        Metallicity
#        ParticleIDs
#        SubGroupNumber
#        Temperature
#        Velocity
#
#    PartType4:
#        Coordinates
#        GroupNumber
#        InitialMass
#        Mass
#        Metallicity
#        ParticleIDs
#        SmoothedMetalAbundance_Fe
#        SmoothedMetalAbundance_O
#        SmoothedMetallicity
#        StellarFormationTime
#        SubGroupNumber
#        Velocity
#    """
#    snap_object = load(catalogue_directory, fileprefix = f"eagle_subfind_snip_tab_{number}_{redshift_tag}", units = False)
#    if "FOF" not in snap_object.data.keys():
#        raise ValueError("Snapshot does not contain PartType0 (gas) data.")
#    if "Subfind" not in snap_object.data.keys():
#        raise ValueError("Snapshot does not contain PartType4 (star) data.")
#    return snap_object
