#!/usr/bin/env python3
"""Bounded state checks for Cedar's append-only Patricia publication rule."""

from __future__ import annotations

import argparse
import itertools
from dataclasses import dataclass


@dataclass(frozen=True)
class Leaf:
    key: int


@dataclass(frozen=True)
class Branch:
    bit: int
    zero: object
    one: object


@dataclass(frozen=True)
class NibbleBranch:
    nibble: int
    children: tuple[object | None, ...]


@dataclass(frozen=True)
class ByteBlock:
    occupied: int
    children: tuple[int, ...]


def bit(key: int, position: int, width: int) -> int:
    return (key >> (width - 1 - position)) & 1


def first_difference(left: int, right: int, width: int) -> int:
    for position in range(width):
        if bit(left, position, width) != bit(right, position, width):
            return position
    return width


def nibble_at(key: int, position: int, width: int) -> int:
    assert width % 4 == 0
    return (key >> (width - 4 * (position + 1))) & 0xF


def first_differing_nibble(left: int, right: int, width: int) -> int:
    assert width % 4 == 0
    for position in range(width // 4):
        if nibble_at(left, position, width) != nibble_at(right, position, width):
            return position
    return width // 4


def first_differing_byte(left: bytes, right: bytes) -> int:
    assert len(left) == len(right) == 40
    for position, (left_byte, right_byte) in enumerate(zip(left, right)):
        if left_byte != right_byte:
            return position
    return 40


def segment_for_byte(value: int) -> int:
    assert 0 <= value <= 0xff
    return value >> 3


def copy_byte_block_with_inserted_child(block: ByteBlock | None, value: int,
                                        child: int) -> ByteBlock:
    local = value & 0x07
    occupied = 0 if block is None else block.occupied
    children = () if block is None else block.children
    assert occupied & (1 << local) == 0
    rank = (occupied & ((1 << local) - 1)).bit_count()
    return ByteBlock(occupied | (1 << local),
                     children[:rank] + (child,) + children[rank:])


def byte_branch_contract(broken: bool = False) -> None:
    blocks: list[ByteBlock | None] = [None] * 32
    order = tuple((position * 73 + 19) & 0xff for position in range(256))
    for value in order:
        segment = segment_for_byte(value)
        blocks[segment] = copy_byte_block_with_inserted_child(
            blocks[segment], value, value)
    assert [child for block in blocks for child in block.children] == list(range(256))
    for segment, block in enumerate(blocks):
        assert block is not None
        assert block.occupied == 0xff
        assert len(block.children) == 8
        assert all(segment_for_byte(value) == segment
                   for value in block.children)

    left = bytes(40)
    for position in range(40):
        right = bytearray(left)
        right[position] = 1
        assert first_differing_byte(left, bytes(right)) == position
    assert first_differing_byte(left, left) == 40

    # A snapshots two children in segment 8, B publishes a third, and A's
    # stale insertion must copy B's snapshot before it adds its own child.
    stale = copy_byte_block_with_inserted_child(None, 64, 64)
    stale = copy_byte_block_with_inserted_child(stale, 66, 66)
    published = copy_byte_block_with_inserted_child(stale, 69, 69)
    result = copy_byte_block_with_inserted_child(stale if broken else published,
                                                  71, 71)
    assert result.children == (64, 66, 69, 71)


def find_leaf(node: object | None, key: int, width: int) -> Leaf | None:
    while isinstance(node, Branch):
        node = node.one if bit(key, node.bit, width) else node.zero
    return node


def insert(root: object | None, key: int, width: int, broken: bool = False) -> tuple[object, bool]:
    if root is None:
        return Leaf(key), True
    matched = find_leaf(root, key, width)
    assert matched is not None
    if matched.key == key:
        return root, False
    difference = first_difference(matched.key, key, width)

    def publish(node: object, previous_bit: int = -1) -> tuple[object, bool]:
        if isinstance(node, Leaf):
            new_leaf = Leaf(key)
            if bit(key, difference, width):
                return Branch(difference, node, new_leaf), True
            return Branch(difference, new_leaf, node), True
        if node.bit >= difference:
            new_leaf = Leaf(key)
            if broken:
                # Negative control: replacing a branch discards its subtree.
                return Branch(difference, new_leaf, new_leaf), True
            if bit(key, difference, width):
                return Branch(difference, node, new_leaf), True
            return Branch(difference, new_leaf, node), True
        if bit(key, node.bit, width):
            child, inserted = publish(node.one, node.bit)
            return Branch(node.bit, node.zero, child), inserted
        child, inserted = publish(node.zero, node.bit)
        return Branch(node.bit, child, node.one), inserted

    return publish(root)


def leaves(node: object | None) -> list[int]:
    if node is None:
        return []
    if isinstance(node, Leaf):
        return [node.key]
    return leaves(node.zero) + leaves(node.one)


def assert_tree(node: object | None, width: int, prefix_bit: int = -1) -> None:
    if node is None or isinstance(node, Leaf):
        return
    assert prefix_bit < node.bit < width
    left = leaves(node.zero)
    right = leaves(node.one)
    assert left and right
    assert all(bit(key, node.bit, width) == 0 for key in left)
    assert all(bit(key, node.bit, width) == 1 for key in right)
    assert_tree(node.zero, width, node.bit)
    assert_tree(node.one, width, node.bit)


def wrapping_path(node: object, key: int, difference: int,
                  width: int) -> list[Branch]:
    path: list[Branch] = []
    while isinstance(node, Branch) and node.bit < difference:
        path.append(node)
        node = node.one if bit(key, node.bit, width) else node.zero
    return path


def assert_boundary_updates_match_brute_force(node: object, key: int,
                                              width: int) -> None:
    matched = find_leaf(node, key, width)
    assert matched is not None
    difference = first_difference(matched.key, key, width)
    if difference == width:
        return
    path = wrapping_path(node, key, difference, width)
    before = {
        branch.bit: (min(leaves(branch)), max(leaves(branch)))
        for branch in path
    }
    brute_force = {
        branch.bit: (min(key, minimum), max(key, maximum))
        for branch, (minimum, maximum) in
        ((branch, before[branch.bit]) for branch in path)
    }
    optimized = dict(before)

    minimum_start = len(path)
    while minimum_start > 0 and bit(key, path[minimum_start - 1].bit, width) == 0:
        minimum_start -= 1
    for branch in path[minimum_start:]:
        minimum, maximum = optimized[branch.bit]
        optimized[branch.bit] = (min(key, minimum), maximum)

    maximum_start = len(path)
    while maximum_start > 0 and bit(key, path[maximum_start - 1].bit, width) == 1:
        maximum_start -= 1
    for branch in path[maximum_start:]:
        minimum, maximum = optimized[branch.bit]
        optimized[branch.bit] = (minimum, max(key, maximum))

    assert optimized == brute_force


def nibble_leaves(node: object | None) -> list[int]:
    if node is None:
        return []
    if isinstance(node, Leaf):
        return [node.key]
    assert isinstance(node, NibbleBranch)
    return [key for child in node.children for key in nibble_leaves(child)]


def nibble_insert(root: object | None, key: int, width: int,
                  broken: bool = False) -> tuple[object, bool]:
    if root is None:
        return Leaf(key), True

    node = root
    while isinstance(node, NibbleBranch):
        child = node.children[nibble_at(key, node.nibble, width)]
        if child is None:
            break
        node = child
    if isinstance(node, Leaf) and node.key == key:
        return root, False

    matched = node if isinstance(node, Leaf) else Leaf(nibble_leaves(root)[0])
    difference = first_differing_nibble(matched.key, key, width)

    def publish(current: object) -> object:
        if isinstance(current, Leaf):
            old_digit = nibble_at(current.key, difference, width)
            new_digit = nibble_at(key, difference, width)
            children: list[object | None] = [None] * 16
            children[old_digit] = current
            children[new_digit] = Leaf(key)
            return NibbleBranch(difference, tuple(children))
        assert isinstance(current, NibbleBranch)
        if current.nibble == difference:
            digit = nibble_at(key, difference, width)
            children = list(current.children)
            assert children[digit] is None
            if broken:
                # Negative control: stale table publication loses the other
                # direct children instead of copying the observed snapshot.
                children = [None] * 16
            children[digit] = Leaf(key)
            return NibbleBranch(current.nibble, tuple(children))
        if current.nibble > difference:
            old_digit = nibble_at(nibble_leaves(current)[0], difference, width)
            new_digit = nibble_at(key, difference, width)
            children = [None] * 16
            children[old_digit] = current
            children[new_digit] = Leaf(key)
            return NibbleBranch(difference, tuple(children))
        digit = nibble_at(key, current.nibble, width)
        children = list(current.children)
        if children[digit] is None:
            if broken:
                # Negative control: publishing only the new child discards the
                # immutable table snapshot's existing children.
                children = [None] * 16
            children[digit] = Leaf(key)
        else:
            children[digit] = publish(children[digit])
        return NibbleBranch(current.nibble, tuple(children))

    return publish(root), True


def assert_nibble_tree(node: object | None, width: int,
                       previous_nibble: int = -1) -> None:
    if node is None or isinstance(node, Leaf):
        return
    assert isinstance(node, NibbleBranch)
    assert previous_nibble < node.nibble < width // 4
    assert sum(child is not None for child in node.children) >= 2
    for digit, child in enumerate(node.children):
        assert all(nibble_at(key, node.nibble, width) == digit
                   for key in nibble_leaves(child))
        assert_nibble_tree(child, width, node.nibble)


def sparse_final_nibble_contract(broken: bool = False) -> None:
    width = 8
    root: object | None = None
    order = (15, 0, 9, 3, 12, 6, 1, 14, 7, 4, 10, 2, 13, 5, 11, 8)
    for key in order:
        root, inserted = nibble_insert(root, key, width, broken)
        assert inserted
        assert_nibble_tree(root, width)
    assert nibble_leaves(root) == list(range(16))
    assert isinstance(root, NibbleBranch)
    assert root.nibble == 1
    assert all(isinstance(child, Leaf) for child in root.children)
    segmented_nibble_blocks_are_sorted()


def segmented_nibble_blocks_are_sorted() -> None:
    # A four-way block owns a local nibble bitmap. Flattening segment-major,
    # then local-rank order must recover the original 16-way key order.
    blocks: list[list[int | None]] = [[None] * 4 for _ in range(4)]
    for nibble in (13, 0, 7, 3, 15, 4, 10, 1,
                   8, 14, 6, 11, 2, 12, 5, 9):
        segment, local = divmod(nibble, 4)
        blocks[segment][local] = nibble
    packed = [
        tuple(value for value in block if value is not None)
        for block in blocks
    ]
    assert packed == [(0, 1, 2, 3), (4, 5, 6, 7),
                      (8, 9, 10, 11), (12, 13, 14, 15)]
    assert [value for block in packed for value in block] == list(range(16))


def run(width: int, writers: int, broken: bool) -> bool:
    domain = range(1, min(1 << width, writers + 3))
    for keys in itertools.product(domain, repeat=writers):
        root: object | None = None
        expected: set[int] = set()
        for order in itertools.permutations(range(writers)):
            current: object | None = root
            current_expected = set(expected)
            for writer in order:
                if current is not None:
                    assert_boundary_updates_match_brute_force(
                        current, keys[writer], width)
                current, inserted = insert(current, keys[writer], width, broken)
                assert inserted == (keys[writer] not in current_expected)
                current_expected.add(keys[writer])
                assert_tree(current, width)
                assert leaves(current) == sorted(current_expected)
    sparse_final_nibble_contract(broken)
    byte_branch_contract(broken)
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--key-bits", type=int, default=4)
    parser.add_argument("--writers", type=int, default=3)
    parser.add_argument("--check-negative-control", action="store_true")
    args = parser.parse_args()
    run(args.key_bits, args.writers, broken=False)
    if args.check_negative_control:
        try:
            run(args.key_bits, args.writers, broken=True)
        except AssertionError:
            print("positive model passed; negative control found a counterexample")
            return 0
        raise AssertionError("negative control unexpectedly passed")
    print("positive model passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
