#include "cedar/types/descriptor.h"

#include <sstream>

namespace cedar {

std::string Descriptor::DebugString() const {
  std::ostringstream oss;
  oss << "Descriptor{kind=" << static_cast<int>(GetKind())
      << ", column_id=" << GetColumnId()
      << ", payload=" << GetPayload()
      << ", length=" << static_cast<int>(GetLength())
      << ", compression=" << static_cast<int>(static_cast<uint8_t>(GetCompression()))
      << "}";
  return oss.str();
}

}  // namespace cedar
