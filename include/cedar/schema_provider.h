#ifndef CEDAR_SCHEMA_PROVIDER_H_
#define CEDAR_SCHEMA_PROVIDER_H_

#include <optional>
#include <string>
#include <string_view>

#include "cedar/core/status.h"
#include "cedar/schema.h"

namespace cedar {

// Immutable schema capability consumed by binders and prepared-query caches.
// Implementations may be backed by a catalog snapshot, a remote schema cache,
// or a test fixture; query planning never owns mutable schema state.
class SchemaProvider {
 public:
  virtual ~SchemaProvider() = default;
  virtual StatusOr<std::optional<PropertyDefinition>> Lookup(
      PropertyId property_id) const = 0;
  virtual StatusOr<std::optional<PropertyDefinition>> LookupByName(
      std::string_view name,
      std::optional<PropertyEntityKind> kind = {}) const = 0;
  virtual std::string Fingerprint() const = 0;
};

}  // namespace cedar

#endif  // CEDAR_SCHEMA_PROVIDER_H_
