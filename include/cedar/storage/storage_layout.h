// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_STORAGE_LAYOUT_H_
#define CEDAR_STORAGE_STORAGE_LAYOUT_H_

namespace cedar::storage_layout {

inline constexpr char kManifestRelativePath[] = "manifest/MANIFEST";
inline constexpr char kOldManifestRelativePath[] = "manifest/MANIFEST-V2";
inline constexpr char kSstExtension[] = ".sst";
inline constexpr char kOldSstExtension[] = ".sst2";
inline constexpr char kIndexExtension[] = ".idx";
inline constexpr char kOldIndexExtension[] = ".idx1";
inline constexpr char kTemporarySuffix[] = ".tmp";

}  // namespace cedar::storage_layout

#endif  // CEDAR_STORAGE_STORAGE_LAYOUT_H_
