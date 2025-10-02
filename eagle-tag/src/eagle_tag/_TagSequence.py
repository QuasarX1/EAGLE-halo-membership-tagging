# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from typing import Self

from ._snapshot_tag import SnapshotTag



class TagSequence(tuple[SnapshotTag, ...]):
    """
    Tuple of EAGLE snapshot/snipshot tags with some convenience methods.

    Iteration defaults to only tags not marked for skipping.
    """

    def __new__(cls, *tags: SnapshotTag|str) -> Self:
        parsed_tags = [(t if isinstance(t, SnapshotTag) else SnapshotTag.from_string(t)) for t in tags]
        return super().__new__(cls, parsed_tags)

    def __init__(self, *args, **kwargs) -> None:

        super().__init__()
        self.__ignored_tag_indexes: list[int] = []
        self.__start_index: int = 0
        self.__end_index: int = len(self) - 1

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
    def length(self) -> int:
        """
        Number of tags, regardless of skip state.
        """

        return len(self)
    
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

        return self.length - len(self.__ignored_tag_indexes)

    @property
    def all(self) -> tuple[SnapshotTag, ...]:
        """
        Tuple of all tags, regardless of skip state.

        Use this when iterating to capture all tags.
        """

        return tuple(self[i] for i in range(self.length))

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

        return tuple(self[i] for i in range(self.length) if i not in self.__ignored_tag_indexes)

    def skip_tags(self, *tags_to_skip: SnapshotTag|str) -> Self:
        """
        Mutate the sequence to skip specific tags during iteration.

        This will reset the start and end positions, wipe any existing ignores and ignore ONLY the specified tags!
        """

        ignore_strings = [(t.tag if isinstance(t, SnapshotTag) else t) for t in tags_to_skip]

        new_ignore_indexes = [i for i in range(self.length) if self[i].tag in ignore_strings]

        new_start_index = 0
        while new_start_index in new_ignore_indexes:
            new_start_index += 1
            if new_start_index >= self.length:
                raise ValueError("All tags are marked to be skipped - unable to set start or end positions.")

        new_end_index = self.length - 1
        while new_end_index in new_ignore_indexes:
            new_end_index -= 1
            if new_end_index < 0:
                # This should never be reached, as the above copy ought to trigger first, but just in case...
                raise ValueError("All tags are marked to be skipped - unable to set start or end positions.")

        self.__ignored_tag_indexes = new_ignore_indexes
        self.__start_index = new_start_index
        self.__end_index = new_end_index

        return self

    def is_skipped(self, tag: SnapshotTag|str) -> bool:
        """
        Check if a specific tag is currently marked as being skipped during iteration.
        """

        tag_string = tag.tag if isinstance(tag, SnapshotTag) else tag
        for i, tag in enumerate(self.all):
            if tag.tag == tag_string:
                return i in self.__ignored_tag_indexes
        raise ValueError(f"TagSequence object does not contain: {tag}")

    def is_selected(self, tag: SnapshotTag|str) -> bool:
        """
        Check if a specific tag is currently marked as being selected during iteration.
        """

        tag_string = tag.tag if isinstance(tag, SnapshotTag) else tag
        for i, tag in enumerate(self.all):
            if tag.tag == tag_string:
                return i in self.__ignored_tag_indexes
        raise ValueError(f"TagSequence object does not contain: {tag}")

    def __contains__(self, key: object) -> bool:
        try:
            _ = self.is_skipped(key if isinstance(key, SnapshotTag) else str(key))
            return True
        except ValueError:
            return False

    @property
    def start(self) -> SnapshotTag:
        """
        First tag in the sequence.

        Set this to mark all prior tags as skippable.
        """
        return self[self.__start_index]
    @start.setter
    def start(self, value: SnapshotTag|str) -> None:
        tag_string = value.tag if isinstance(value, SnapshotTag) else value
        if tag_string not in self:
            raise ValueError(f"Unable to set tag {value} as sequence start - it is not a part of the sequence.")
        if self.is_skipped(tag_string):
            raise ValueError(f"Unable to set tag {value} as sequence start - it is marked to be skipped.")
        start_index: int
        for start_index in range(self.length - 1, -1, -1):
            if self[start_index].tag == tag_string:
                break
        if start_index > self.__end_index:
            raise ValueError(f"Unable to set tag {value} as sequence start - it comes after the current end tag.")
        if start_index > 0:
            self.__ignored_tag_indexes.extend(range(0, start_index))
            self.__ignored_tag_indexes.sort()
        self.__start_index = start_index

    @property
    def end(self) -> SnapshotTag:
        """
        Last tag in the sequence.

        Set this to mark all subsequent tags as skippable.
        """
        return self[self.__end_index]
    @end.setter
    def end(self, value: SnapshotTag|str) -> None:
        tag_string = value.tag if isinstance(value, SnapshotTag) else value
        if tag_string not in self:
            raise ValueError(f"Unable to set tag {value} as sequence end - it is not a part of the sequence.")
        if self.is_skipped(tag_string):
            raise ValueError(f"Unable to set tag {value} as sequence end - it is marked to be skipped.")
        end_index: int
        for end_index in range(self.length):
            if self[end_index].tag == tag_string:
                break
        if end_index < self.__start_index:
            raise ValueError(f"Unable to set tag {value} as sequence end - it comes before the current start tag.")
        if end_index < self.length - 1:
            self.__ignored_tag_indexes.extend(range(end_index + 1, self.length))
            self.__ignored_tag_indexes.sort()
        self.__end_index = end_index

    def __iter__(self) -> Iterator[SnapshotTag]:
        return self.selected.__iter__()

    @property
    def pairs(self) -> tuple[tuple[SnapshotTag, SnapshotTag], ...]:
        """
        Tuple of pairs of adjacent tags in the sequence, skipping any ignored tags.

        Useful for iterating over pairs of snapshots.
        """

        selected_tags = self.selected
        return tuple((selected_tags[i - 1], selected_tags[i]) for i in range(1, len(selected_tags)))

    def __copy__(self) -> "TagSequence":
        copy = TagSequence(*[t.copy() for t in self.all])
        copy.skip_tags(*self.skipped)
        return copy

    def copy(self) -> "TagSequence":
        return self.__copy__()

    def reverse(self) -> "TagSequence":
        """
        Return a new TagSequence with the tags in reverse order.
        """

        rev = TagSequence(*reversed(self.all))
        rev.skip_tags(*self.skipped)
        rev.start = self.end
        rev.end = self.start
        return rev

    def __str__(self) -> str:
        return f"{self.start} -> {self.end}, length={self.length}, selected={self.count_selected}, skipped={self.count_skipped}"
    
    def __repr__(self) -> str:
        return f"TagSequence({self})"
