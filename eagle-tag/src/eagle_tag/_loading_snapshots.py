# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

from scida import load, GadgetStyleSnapshot

def load_snapshot(snapshot_directory: str) -> GadgetStyleSnapshot:
    snap_object: GadgetStyleSnapshot
    snap_object = load(snapshot_directory, units = False)
    if "PartType0" not in snap_object.data.keys():
        raise ValueError("Snapshot does not contain PartType0 (gas) data.")
    if "PartType4" not in snap_object.data.keys():
        raise ValueError("Snapshot does not contain PartType4 (star) data.")
    return snap_object
