# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

from scida import load
from scida.interface import Dataset

def load_catalogue_membership(catalogue_directory: str, number: str, redshift_tag: str):
    snap_object = load(catalogue_directory, fileprefix = f"eagle_subfind_snip_particles_{number}_{redshift_tag}", units = False)
    if "PartType0" not in snap_object.data.keys():
        raise ValueError("Snapshot does not contain PartType0 (gas) data.")
    if "PartType4" not in snap_object.data.keys():
        raise ValueError("Snapshot does not contain PartType4 (star) data.")
    return snap_object
