// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

#include "cedar/benchmark/artifact_reader.h"

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: cedar_evidence_verify <manifest.json>\n";
    return 2;
  }
  std::ifstream input(argv[1], std::ios::binary);
  if (!input) {
    std::cerr << "release evidence: cannot open manifest\n";
    return 1;
  }
  const std::string manifest((std::istreambuf_iterator<char>(input)),
                             std::istreambuf_iterator<char>());
  const cedar::Status status = cedar::ValidateReleaseEvidenceManifest(manifest);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }
  return 0;
}
