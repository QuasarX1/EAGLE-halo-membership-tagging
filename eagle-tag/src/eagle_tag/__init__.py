# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

from ._load_data_with_xarray import load_hdf5_files_with_xarray, load_hdf5_pattern_with_xarray
from ._snapshot_tag import SnapshotTag
from ._eagle_filepaths import EAGLE_Files, EAGLE_Snapshot
from ._loading_snapshots import load_snapshot
from ._loading_catalogue_data import load_catalogue
from ._loading_catalogue_membership import load_catalogue_membership
from ._save_aux_file import make_aux_file_path, Metadata, make_aux_file, save_chunk
