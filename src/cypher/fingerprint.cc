#include "cedar/cypher/fingerprint.h"

#include <iomanip>
#include <sstream>

namespace cedar::cypher {

uint64_t StableFingerprint(const std::string& canonical) {
  uint64_t hash = 1469598103934665603ULL;
  for (unsigned char value : canonical) {
    hash ^= value;
    hash *= 1099511628211ULL;
  }
  return hash;
}

std::string FingerprintHex(uint64_t fingerprint) {
  std::ostringstream output;
  output << std::hex << std::setw(16) << std::setfill('0') << fingerprint;
  return output.str();
}

}  // namespace cedar::cypher
