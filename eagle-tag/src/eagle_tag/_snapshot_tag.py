# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass



@dataclass
class SnapshotTag:
    """
    Snapshot number and redshift string used to label EAGLE snapshots.
    Format: "999_z999p999"

    Attributes:
        str number:
            Default "028"
        str redshift_tag:
            Default "z000p000" -> 999.999
    """

    number: str = "028"
    redshift_tag: str = "z000p000"

    def __repr__(self) -> str:
        return f"{self.number} (z = {self.redshift_tag})"

    def __str__(self) -> str:
        return f"{self.number}_{self.redshift_tag}"

    @property
    def tag(self) -> str:
        return str(self)

    @property
    def redshift(self) -> float:
        return float(self.redshift_tag[1:].replace("p", "."))
