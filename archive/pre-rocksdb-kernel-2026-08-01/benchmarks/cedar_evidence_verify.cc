// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <iostream>

#include "cedar/benchmark/artifact_reader.h"

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: cedar_evidence_verify <evidence-directory>\n";
    return 2;
  }
  const cedar::Status status = cedar::VerifyReleaseEvidenceDirectory(argv[1]);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }
  return 0;
}
