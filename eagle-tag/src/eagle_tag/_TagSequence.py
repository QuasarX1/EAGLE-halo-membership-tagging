# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from typing import Self
from ._snapshot_tag import SnapshotTag

class TagSequence(tuple[SnapshotTag, ...]):
    """
    Tuple of EAGLE snapshot/snipshot tags with some convenience methods.
    """

    def __new__(cls, *tags: SnapshotTag|str) -> Self:
        parsed_tags = [(t if isinstance(t, SnapshotTag) else SnapshotTag.from_string(t)) for t in tags]
        return super().__new__(cls, parsed_tags)

    def __init__(self, *args, **kwargs) -> None:

        super().__init__()
        self.__ignored_tag_indexes: list[int] = []

        self.__iterable_index: int = 0

    @staticmethod
    def from_file(filepath: str) -> "TagSequence":
        """
        Create a TagSequence from a text file containing one snapshot tag per line.
        Lines starting with '#' are ignored as comments.
        """

        tags = []
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    tags.append(SnapshotTag.from_string(line))
        return TagSequence(*tags)
    
    @property
    def count_skipped(self) -> int:
        """
        Number of tags that are currently marked as being skipped during iteration.
        """

        return len(self.__ignored_tag_indexes)

    @property
    def count_selected(self) -> int:
        """
        Number of tags that are not currently marked as being skipped.
        """

        return len(self) - len(self.__ignored_tag_indexes)

    @property
    def skipped(self) -> tuple[SnapshotTag, ...]:
        """
        Tuple of tags that are currently marked as being skipped during iteration.
        """

        return tuple(self[i] for i in self.__ignored_tag_indexes)

    @property
    def selected(self) -> tuple[SnapshotTag, ...]:
        """
        Tuple of tags that are not currently marked as being skipped.
        """

        return tuple(self[i] for i in range(len(self)) if i not in self.__ignored_tag_indexes)

    def skip_tags(self, *tags_to_skip: SnapshotTag|str) -> Self:
        """
        Mutate the sequence to skip specific tags during iteration.

        This will wipe any existing ignores and ignore ONLY the specified tags!
        """

        ignore_strings = [(t.tag if isinstance(t, SnapshotTag) else t) for t in tags_to_skip]

        self.__ignored_tag_indexes = [i for i in range(len(self)) if self[i].tag in ignore_strings]

        return self

    def __iter__(self) -> Iterator[SnapshotTag]:
        self.__iterable_index = 0
        return self

    def __next__(self) -> SnapshotTag:
        while True:
            self.__iterable_index += 1
            if self.__iterable_index >= len(self):
                raise StopIteration
            if self.__iterable_index not in self.__ignored_tag_indexes:
                return self[self.__iterable_index]

    def __copy__(self) -> "TagSequence":
        copy = TagSequence(*[t.copy() for t in self])
        copy.skip_tags(*self.skipped)
        return copy

    def copy(self) -> "TagSequence":
        return self.__copy__()
