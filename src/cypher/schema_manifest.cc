#include "cedar/cypher/schema_manifest.h"

#include <fstream>
#include <sstream>
#include <vector>

namespace cedar::cypher {
namespace {

Status ManifestError(const char* message) {
  return Status::SchemaMismatch("cypher schema manifest", message);
}

PhysicalType ParsePhysical(const std::string& value) {
  if (value == "bool") return PhysicalType::kBool;
  if (value == "int32") return PhysicalType::kInt32;
  if (value == "int64") return PhysicalType::kInt64;
  if (value == "float32") return PhysicalType::kFloat32;
  if (value == "float64") return PhysicalType::kFloat64;
  if (value == "timestamp64") return PhysicalType::kTimestamp64;
  if (value == "string") return PhysicalType::kString;
  return PhysicalType::kBinary;
}

bool IsPhysical(const std::string& value) {
  return value == "bool" || value == "int32" || value == "int64" ||
         value == "float32" || value == "float64" || value == "timestamp64" ||
         value == "string" || value == "binary";
}

std::vector<std::string> Split(const std::string& value) {
  std::vector<std::string> result;
  std::stringstream input(value);
  std::string field;
  while (std::getline(input, field, ',')) result.push_back(field);
  return result;
}

}  // namespace

StatusOr<SchemaManifest> ParseSchemaManifest(const std::string& text) {
  SchemaManifest manifest;
  std::istringstream input(text);
  std::string line;
  uint32_t line_number = 0;
  bool has_part = false;
  while (std::getline(input, line)) {
    ++line_number;
    if (line.empty()) continue;
    const size_t equals = line.find('=');
    if (equals == std::string::npos) return ManifestError("manifest line lacks '='");
    const std::string key = line.substr(0, equals);
    const std::string value = line.substr(equals + 1);
    try {
      if (key == "graph") {
        if (!manifest.graph.empty() || value.empty()) return ManifestError("graph is missing or duplicated");
        manifest.graph = value;
      } else if (key == "part_id") {
        if (has_part) return ManifestError("part_id is duplicated");
        const unsigned long parsed = std::stoul(value);
        if (parsed > UINT32_MAX) return ManifestError("part_id overflows uint32");
        manifest.part_id = PartId{static_cast<uint32_t>(parsed)};
        has_part = true;
      } else if (key == "property") {
        const auto fields = Split(value);
        if (fields.size() != 5 || (fields[1] != "vertex" && fields[1] != "edge") ||
            !IsPhysical(fields[2]) || fields[4].empty()) {
          return ManifestError("invalid property definition");
        }
        const unsigned long id = std::stoul(fields[0]);
        const unsigned long epoch = std::stoul(fields[3]);
        if (id == 0 || id > UINT16_MAX || epoch > UINT32_MAX) {
          return ManifestError("property identifier overflows configured type");
        }
        const auto kind = fields[1] == "vertex" ? PropertyEntityKind::kVertex
                                                  : PropertyEntityKind::kEdge;
        const Status added = manifest.catalog.Add(PropertyDefinition{
            PropertyId{static_cast<uint16_t>(id)}, static_cast<uint32_t>(epoch),
            fields[4], kind, ParsePhysical(fields[2]), 1});
        if (!added.ok()) return added;
      } else {
        return ManifestError("unknown manifest key");
      }
    } catch (...) {
      return ManifestError("invalid numeric manifest field");
    }
  }
  if (manifest.graph.empty()) return ManifestError("graph is required");
  return manifest;
}

StatusOr<SchemaManifest> LoadSchemaManifest(const std::string& path) {
  std::ifstream input(path);
  if (!input) return Status::NotFound("cypher schema manifest", "cannot open manifest");
  std::ostringstream text;
  text << input.rdbuf();
  return ParseSchemaManifest(text.str());
}

}  // namespace cedar::cypher
