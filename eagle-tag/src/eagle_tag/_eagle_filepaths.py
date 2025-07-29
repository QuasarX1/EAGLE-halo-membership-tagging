# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
import os
from typing import cast

from ._snapshot_tag import SnapshotTag



@dataclass
class EAGLE_Files:

    boxes_directory: str = "."
    box: str = "L0100N1504"
    model: str = "REFERENCE"
    data_folder: str = "data"
    line_of_sight_folder: str = "los"
    directory: str = None # string value will be assigned in __post_init__

    snapshot_folder_template: str = "snapshot_{}"
    snapshot_file_template: str = "snap_{}.{{}}.hdf5"

    snapshot_catalogue_folder_template: str = "groups_{}"
    snapshot_catalogue_file_template: str = "eagle_subfind_tab_{}.{{}}.hdf5"

    snapshot_catalogue_membership_folder_template: str = "particledata_{}"
    snapshot_catalogue_membership_file_template: str = "eagle_subfind_particles_{}.{{}}.hdf5"

    snipshot_folder_template: str = "snipshot_{}"
    snipshot_file_template: str = "snip_{}.{{}}.hdf5"

    snipshot_catalogue_folder_template: str = "groups_snip_{}"
    snipshot_catalogue_file_template: str = "eagle_subfind_snip_tab_{}.{{}}.hdf5"

    snipshot_catalogue_membership_folder_template: str = "particledata_snip_{}"
    snipshot_catalogue_membership_file_template: str = "eagle_subfind_snip_particles_{}.{{}}.hdf5"

    def __post_init__(self) -> None:
        if self.directory is None:
            self.make_directory()

    def make_directory(self) -> None:
        self.directory = os.path.join(self.boxes_directory, self.box, self.model, self.data_folder)

    def __str__(self) -> str:
        return f"EAGLE Data: \"{self.directory}\""

    def __repr__(self) -> str:
        return str(self)

    def snapshot(self, tag: SnapshotTag, snipshot: bool = False) -> "EAGLE_Snapshot":
        if self.directory is None:
            self.make_directory()
        return EAGLE_Snapshot(box = self, tag = tag, is_snipshot = snipshot)



@dataclass
class EAGLE_Snapshot:

    box: EAGLE_Files
    tag: SnapshotTag
    is_snipshot: bool = False

    def __str__(self) -> str:
        return f"EAGLE Sn{"i" if self.is_snipshot else "a"}pshot: {self.tag}"

    def __repr__(self) -> str:
        return str(self)

    @property
    def snapshot_directory(self) -> str:
        return os.path.join(self.box.directory, self.box.snipshot_folder_template.format(self.tag.tag) if self.is_snipshot else self.box.snapshot_folder_template.format(self.tag.tag))

    @property
    def snapshot_file_template(self) -> str:
        return os.path.join(self.snapshot_directory, self.box.snipshot_file_template.format(self.tag.tag) if self.is_snipshot else self.box.snapshot_file_template.format(self.tag.tag))

    @property
    def snapshot_file_pattern(self) -> str:
        return self.snapshot_file_template.format("*")
    
    @property
    def has_snapshot(self) -> bool:
        return os.path.exists(self.snapshot_file_template.format("0"))

    @property
    def catalogue_directory(self) -> str:
        return os.path.join(self.box.directory, self.box.snipshot_catalogue_folder_template.format(self.tag.tag) if self.is_snipshot else self.box.snapshot_catalogue_folder_template.format(self.tag.tag))

    @property
    def catalogue_file_template(self) -> str:
        return os.path.join(self.catalogue_directory, self.box.snipshot_catalogue_file_template.format(self.tag.tag) if self.is_snipshot else self.box.snapshot_catalogue_file_template.format(self.tag.tag))

    @property
    def catalogue_file_pattern(self) -> str:
        return self.catalogue_file_template.format("*")
    
    @property
    def has_catalogue(self) -> bool:
        return os.path.exists(self.snapshot_file_template.format("0"))

    @property
    def catalogue_membership_directory(self) -> str:
        return os.path.join(self.box.directory, self.box.snipshot_catalogue_membership_folder_template.format(self.tag.tag) if self.is_snipshot else self.box.snapshot_catalogue_membership_folder_template.format(self.tag.tag))

    @property
    def catalogue_membership_file_template(self) -> str:
        return os.path.join(self.catalogue_membership_directory, self.box.snipshot_catalogue_membership_file_template.format(self.tag.tag) if self.is_snipshot else self.box.snapshot_catalogue_membership_file_template.format(self.tag.tag))

    @property
    def catalogue_membership_file_pattern(self) -> str:
        return self.catalogue_membership_file_template.format("*")
    
    @property
    def has_catalogue_membership(self) -> bool:
        return os.path.exists(self.snapshot_file_template.format("0"))
