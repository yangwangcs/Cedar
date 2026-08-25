#ifndef CEDAR_CYPHER_SCHEMA_MANIFEST_H_
#define CEDAR_CYPHER_SCHEMA_MANIFEST_H_

#include <cstdint>
#include <string>

#include "cedar/cypher/binder.h"

namespace cedar::cypher {

struct SchemaManifest {
  std::string graph;
  PartId part_id{0};
  SchemaCatalog catalog;
};

StatusOr<SchemaManifest> ParseSchemaManifest(const std::string& text);
StatusOr<SchemaManifest> LoadSchemaManifest(const std::string& path);

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_SCHEMA_MANIFEST_H_
