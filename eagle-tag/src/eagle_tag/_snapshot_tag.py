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
    
    @staticmethod
    def from_string(tag: str) -> "SnapshotTag":
        """
        Create a SnapshotTag from a string in the format "999_z999p999".
        """
        parts = tag.split("_")
        if len(parts) != 2:
            raise ValueError(f"Invalid tag format: {tag}. Expected format '999_z999p999'.")
        
        number = parts[0]
        redshift_tag = parts[1]
        
        return SnapshotTag(number, redshift_tag)
