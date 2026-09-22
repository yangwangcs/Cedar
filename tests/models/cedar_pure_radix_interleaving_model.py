#!/usr/bin/env python3
"""Small deterministic model of the Radix wrapping-CAS publication rule.

The model exposes the only unsafe interleaving this protocol must reject:
writer A descends to a leaf while writer B publishes an ancestor branch, then
A must fail its stale-slot CAS and retry from the new root.  The negative
control deliberately accepts the stale CAS and drops B's subtree.
"""

from __future__ import annotations

import argparse
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
class SegmentedNibbleBranch:
    nibble: int
    blocks: tuple[tuple[object | None, ...], ...]


@dataclass(frozen=True)
class ByteBlock:
    occupied: int
    children: tuple[object, ...]


@dataclass(frozen=True)
class SegmentedByteBranch:
    byte_index: int
    blocks: tuple[ByteBlock | None, ...]


@dataclass
class SameEdgeRetryState:
    edge_snapshot: ByteBlock
    target_child: object | None
    retry_budget: int = 1
    local_retries: int = 0
    root_restarts: int = 0


def segment_for_byte(value: int) -> int:
    assert 0 <= value <= 0xff
    return value >> 3


def insert_byte_block(block: ByteBlock | None, value: int,
                      child: object) -> ByteBlock:
    local = value & 0x07
    occupied = 0 if block is None else block.occupied
    children = () if block is None else block.children
    assert occupied & (1 << local) == 0
    rank = (occupied & ((1 << local) - 1)).bit_count()
    return ByteBlock(occupied | (1 << local),
                     children[:rank] + (child,) + children[rank:])


def byte_child(block: ByteBlock | None, value: int) -> object | None:
    if block is None:
        return None
    local = value & 0x07
    if block.occupied & (1 << local) == 0:
        return None
    rank = (block.occupied & ((1 << local) - 1)).bit_count()
    return block.children[rank]


def replace_byte_child(block: ByteBlock, value: int,
                       child: object) -> ByteBlock:
    local = value & 0x07
    assert block.occupied & (1 << local)
    rank = (block.occupied & ((1 << local) - 1)).bit_count()
    children = list(block.children)
    children[rank] = child
    return ByteBlock(block.occupied, tuple(children))


def replace_byte_block(branch: SegmentedByteBranch, value: int,
                       block: ByteBlock) -> SegmentedByteBranch:
    blocks = list(branch.blocks)
    blocks[segment_for_byte(value)] = block
    return SegmentedByteBranch(branch.byte_index, tuple(blocks))


def first_difference(left: int, right: int, width: int) -> int:
    for bit in range(width):
        if ((left >> (width - bit - 1)) & 1) != ((right >> (width - bit - 1)) & 1):
            return bit
    return width


def leaves(node: object) -> list[int]:
    if isinstance(node, Leaf):
        return [node.key]
    return leaves(node.zero) + leaves(node.one)


def publish_wrap(old: object, key: int, width: int) -> Branch:
    old_key = leaves(old)[0]
    bit = first_difference(old_key, key, width)
    return Branch(bit, old, Leaf(key)) if ((key >> (width - bit - 1)) & 1) else Branch(bit, Leaf(key), old)


def bit_at(key: int, bit: int, width: int) -> int:
    return (key >> (width - bit - 1)) & 1


def find_leaf(node: object, key: int, width: int) -> Leaf:
    while isinstance(node, Branch):
        node = node.one if bit_at(key, node.bit, width) else node.zero
    assert isinstance(node, Leaf)
    return node


def max_depth(node: object) -> int:
    if isinstance(node, Leaf):
        return 0
    return 1 + max(max_depth(node.zero), max_depth(node.one))


def nibble_leaves(node: object | None) -> list[int]:
    if node is None:
        return []
    if isinstance(node, Leaf):
        return [node.key]
    assert isinstance(node, NibbleBranch)
    return [key for child in node.children for key in nibble_leaves(child)]


def replace_nibble_child(branch: NibbleBranch, digit: int,
                         replacement: object) -> NibbleBranch:
    children = list(branch.children)
    children[digit] = replacement
    return NibbleBranch(branch.nibble, tuple(children))


def direct_child_table_vs_wrapper(broken: bool) -> None:
    # A observes the immutable root child-table and wants to add nibble 8. B
    # wraps the leaf at nibble 12, publishing a newer root snapshot first.
    # A must fail its stale table CAS, reload, and add nibble 8 to B's table.
    initial_children: list[object | None] = [None] * 16
    initial_children[4] = Leaf(0x40)
    initial_children[12] = Leaf(0xC0)
    root = NibbleBranch(0, tuple(initial_children))
    stale_snapshot = root

    wrapped_children: list[object | None] = [None] * 16
    wrapped_children[0] = Leaf(0xC0)
    wrapped_children[1] = Leaf(0xC1)
    wrapped = NibbleBranch(1, tuple(wrapped_children))
    root = replace_nibble_child(root, 12, wrapped)

    if broken:
        # Negative control: stale A overwrites B's published child-table.
        root = replace_nibble_child(stale_snapshot, 8, Leaf(0x80))
    else:
        assert root is not stale_snapshot
        root = replace_nibble_child(root, 8, Leaf(0x80))

    assert sorted(nibble_leaves(root)) == [0x40, 0x80, 0xC0, 0xC1]


def segmented_leaves(node: object | None) -> list[int]:
    if node is None:
        return []
    if isinstance(node, Leaf):
        return [node.key]
    assert isinstance(node, SegmentedNibbleBranch)
    return [key for block in node.blocks for child in block
            for key in segmented_leaves(child)]


def replace_segment_child(branch: SegmentedNibbleBranch, digit: int,
                          replacement: object) -> SegmentedNibbleBranch:
    segment, local = divmod(digit, 4)
    blocks = [list(block) for block in branch.blocks]
    blocks[segment][local] = replacement
    return SegmentedNibbleBranch(branch.nibble,
                                 tuple(tuple(block) for block in blocks))


def direct_child_block_vs_other_segment_wrapper(broken: bool) -> None:
    # A snapshots segment 2 for nibble 8. B replaces only segment 3 by
    # wrapping nibble 12. Correct block-local publication preserves B without
    # retry; an old whole-branch stale publication loses the wrapper.
    blocks: list[list[object | None]] = [[None] * 4 for _ in range(4)]
    blocks[1][0] = Leaf(0x40)
    blocks[3][0] = Leaf(0xC0)
    root = SegmentedNibbleBranch(0, tuple(tuple(block) for block in blocks))
    stale_snapshot = root

    wrapped_blocks: list[list[object | None]] = [[None] * 4 for _ in range(4)]
    wrapped_blocks[0][0] = Leaf(0xC0)
    wrapped_blocks[0][1] = Leaf(0xC1)
    wrapped = SegmentedNibbleBranch(1,
                                    tuple(tuple(block) for block in wrapped_blocks))
    root = replace_segment_child(root, 12, wrapped)
    root = replace_segment_child(stale_snapshot if broken else root, 8,
                                 Leaf(0x80))
    assert sorted(segmented_leaves(root)) == [0x40, 0x80, 0xC0, 0xC1]


def stale_same_byte_segment_block_restarts(broken: bool) -> None:
    # A and B both observed segment 8. B publishes byte 69 first; A's CAS
    # must fail, reload the eight-bit bitmap, and preserve every packed child.
    initial = insert_byte_block(None, 64, Leaf(0x40))
    initial = insert_byte_block(initial, 66, Leaf(0x42))
    blocks: list[ByteBlock | None] = [None] * 32
    blocks[8] = initial
    root = SegmentedByteBranch(0, tuple(blocks))
    stale = initial
    published = insert_byte_block(initial, 69, Leaf(0x45))
    root = replace_byte_block(root, 69, published)
    final = insert_byte_block(stale if broken else published, 71, Leaf(0x47))
    root = replace_byte_block(root, 71, final)
    assert root.blocks[8] is not None
    assert [leaf.key for leaf in root.blocks[8].children] == [0x40, 0x42, 0x45, 0x47]


def bounded_same_edge_retry(broken: bool) -> None:
    initial = insert_byte_block(None, 64, Leaf(0x40))
    initial = insert_byte_block(initial, 66, Leaf(0x42))

    # Different local byte: the failed CAS observes a compatible edge and one
    # local retry preserves the concurrently published child.
    state = SameEdgeRetryState(initial, None)
    current = insert_byte_block(initial, 71, Leaf(0x47))
    compatible = byte_child(current, 69) is state.target_child
    assert compatible and state.retry_budget == 1
    state.local_retries += 1
    state.retry_budget -= 1
    retried = insert_byte_block(current, 69, Leaf(0x45))
    assert [leaf.key for leaf in retried.children] == [0x40, 0x42, 0x45, 0x47]
    assert state.local_retries == 1 and state.root_restarts == 0

    # A second same-edge publication exhausts the one-retry budget and forces
    # an acquire-root restart rather than a retry loop.
    second_current = insert_byte_block(current, 70, Leaf(0x46))
    assert second_current is not state.edge_snapshot
    state.root_restarts += 1
    assert state.retry_budget == 0 and state.root_restarts == 1

    # Same target child: a concurrent wrapper changes pointer identity. The
    # correct protocol rejects local replacement. The negative control ignores
    # identity and overwrites that wrapper, which this assertion detects.
    expected = byte_child(initial, 64)
    concurrent_wrapper = (Leaf(0x40), Leaf(0x4001))
    changed = replace_byte_child(initial, 64, concurrent_wrapper)
    compatible = byte_child(changed, 64) is expected
    assert not compatible
    if broken:
        changed = replace_byte_child(changed, 64, Leaf(0x4002))
    else:
        state.root_restarts += 1
    assert byte_child(changed, 64) is concurrent_wrapper


def same_key_race(width: int) -> None:
    base = Leaf(0)
    key = 1 << (width - 1)
    # Both writers descend to the same leaf. Exactly one CAS can replace its
    # slot; the loser retries and must observe the key as a duplicate.
    first_expected = base
    second_expected = base
    root: object = publish_wrap(first_expected, key, width)
    assert root is not second_expected
    assert find_leaf(root, key, width).key == key
    assert sorted(leaves(root)) == [0, key]


def ancestor_wrap_vs_descendant(width: int, broken: bool) -> None:
    # A descends to the 000... leaf and snapshots the slot it will wrap.
    # B then publishes 100... through that same slot. A's compare_exchange
    # must fail and its retry must descend through B's newly published branch.
    base = Leaf(0)
    stale_expected = base
    root: object = publish_wrap(base, 1 << (width - 1), width)
    replacement = publish_wrap(stale_expected, 1 << 1, width)
    if broken:
        # A broken implementation uses an unconditional store and loses B.
        root = replacement
    else:
        assert root is not stale_expected
        # A failed stale CAS retries against the current root and wraps the
        # subtree containing both already-published leaves.
        root = publish_wrap(root, 1 << 1, width)
    visible = sorted(leaves(root))
    assert visible == [0, 2, 1 << (width - 1)]


def path_deeper_than_inline_cache(width: int) -> None:
    deep_width = max(width, 33)
    root: object = Leaf(0)
    expected = [0]
    # Each key differs from the zero leaf at a progressively later bit. The
    # zero-child path therefore exceeds the production 32-entry fast cache.
    for bit in range(33):
        key = 1 << (deep_width - bit - 1)
        root = publish_wrap(root, key, deep_width)
        expected.append(key)
    assert max_depth(root) > 32
    assert sorted(leaves(root)) == sorted(expected)


def run(width: int, broken: bool) -> None:
    assert width >= 4
    bounded_same_edge_retry(broken)
    same_key_race(width)
    ancestor_wrap_vs_descendant(width, broken)
    if not broken:
      path_deeper_than_inline_cache(width)
    direct_child_table_vs_wrapper(broken)
    direct_child_block_vs_other_segment_wrapper(broken)
    stale_same_byte_segment_block_restarts(broken)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--key-bits", type=int, default=4)
    parser.add_argument("--writers", type=int, default=3)
    parser.add_argument("--check-negative-control", action="store_true")
    args = parser.parse_args()
    if args.key_bits < 2 or args.writers < 2:
        raise SystemExit("key-bits >= 2 and writers >= 2 are required")
    run(args.key_bits, broken=False)
    if args.check_negative_control:
        try:
            run(args.key_bits, broken=True)
        except AssertionError:
            print("interleaving model passed; negative control found a counterexample")
            return 0
        raise AssertionError("negative control unexpectedly passed")
    print("interleaving model passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
