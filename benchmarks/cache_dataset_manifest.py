#!/usr/bin/env python3
"""Describe a dedicated cache experiment dataset without opening the database."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat


def manifest(directory, hash_content=False):
    root = Path(directory).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("dataset must be a directory")
    files = []
    for parent, directories, names in os.walk(root, followlinks=False):
        directories[:] = sorted(name for name in directories
                                if not (Path(parent) / name).is_symlink())
        for name in sorted(names):
            path = Path(parent) / name
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode):
                continue
            entry = {"path": path.relative_to(root).as_posix(),
                     "bytes": before.st_size, "mtime_ns": before.st_mtime_ns}
            if hash_content:
                digest = hashlib.sha256()
                with path.open("rb") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                entry["sha256"] = digest.hexdigest()
            after = path.lstat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (
                    after.st_ino, after.st_size, after.st_mtime_ns):
                raise ValueError("dataset changed during sampling: " + str(path))
            files.append(entry)
    files.sort(key=lambda entry: entry["path"])
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {"schema_version": 1, "directory": str(root), "files": files,
            "file_count": len(files), "total_file_bytes": sum(f["bytes"] for f in files),
            "manifest_sha256": hashlib.sha256(encoded).hexdigest(),
            "content_hashes": hash_content,
            "payload_read_warms_system_cache": hash_content,
            "skips_symlinks": True,
            "consistency": "per-file metadata checked; run against a quiescent dataset"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory")
    parser.add_argument("--hash-content", action="store_true",
                        help="read all payload bytes; this warms the system file cache")
    args = parser.parse_args()
    try:
        result = manifest(args.directory, args.hash_content)
    except (OSError, ValueError) as error:
        parser.exit(1, str(error) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
