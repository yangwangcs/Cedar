#ifndef CEDAR_CYPHER_FINGERPRINT_H_
#define CEDAR_CYPHER_FINGERPRINT_H_

#include <cstdint>
#include <string>

namespace cedar::cypher {

uint64_t StableFingerprint(const std::string& canonical);
std::string FingerprintHex(uint64_t fingerprint);

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_FINGERPRINT_H_
