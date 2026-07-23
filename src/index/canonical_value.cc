// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/index/canonical_value.h"

#include <cmath>
#include <cstring>
#include <variant>

#include "cedar/blob/blob_store.h"

namespace cedar {
namespace {

template <typename U>
std::string BigEndian(U value) {
  std::string bytes(sizeof(U), '\0');
  for (size_t index = 0; index < sizeof(U); ++index) {
    bytes[index] = static_cast<char>(value >> ((sizeof(U) - 1 - index) * 8));
  }
  return bytes;
}

template <typename F, typename U>
StatusOr<std::string> EncodeFloat(F value) {
  if (std::isnan(value)) return Status::InvalidArgument("index canonical value", "NaN is not indexable");
  if (value == static_cast<F>(0)) value = static_cast<F>(0);
  U bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  const U ordered = (bits & (static_cast<U>(1) << (sizeof(U) * 8 - 1)))
      ? ~bits : (bits ^ (static_cast<U>(1) << (sizeof(U) * 8 - 1)));
  return BigEndian(ordered);
}

}  // namespace

StatusOr<IndexCanonicalValue> EncodeIndexCanonicalValue(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kBool:
      return IndexCanonicalValue{value.type(), std::string(1, std::get<bool>(value.data()) ? '\1' : '\0')};
    case PhysicalType::kInt32: {
      const uint32_t bits = static_cast<uint32_t>(std::get<int32_t>(value.data())) ^ 0x80000000U;
      return IndexCanonicalValue{value.type(), BigEndian(bits)};
    }
    case PhysicalType::kInt64: {
      const uint64_t bits = static_cast<uint64_t>(std::get<int64_t>(value.data())) ^ (1ULL << 63);
      return IndexCanonicalValue{value.type(), BigEndian(bits)};
    }
    case PhysicalType::kTimestamp64:
      return IndexCanonicalValue{value.type(), BigEndian(std::get<uint64_t>(value.data()))};
    case PhysicalType::kFloat32: {
      const auto encoded = EncodeFloat<float, uint32_t>(std::get<float>(value.data()));
      if (!encoded.ok()) return encoded.status();
      return IndexCanonicalValue{value.type(), encoded.ValueOrDie()};
    }
    case PhysicalType::kFloat64: {
      const auto encoded = EncodeFloat<double, uint64_t>(std::get<double>(value.data()));
      if (!encoded.ok()) return encoded.status();
      return IndexCanonicalValue{value.type(), encoded.ValueOrDie()};
    }
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      return IndexCanonicalValue{value.type(), std::get<std::string>(value.data())};
  }
  return Status::InvalidArgument("index canonical value", "unknown physical type");
}

StatusOr<IndexCanonicalValue> EncodeIndexBlobHash(const Value& value) {
  if (value.type() != PhysicalType::kString &&
      value.type() != PhysicalType::kBinary) {
    return Status::SchemaMismatch(
        "index canonical value", "Blob hash equality requires string or binary");
  }
  const std::string& payload = std::get<std::string>(value.data());
  const BlobHash hash = Blake3Hash(payload);
  std::string bytes(reinterpret_cast<const char*>(hash.bytes.data()),
                    hash.bytes.size());
  bytes.append(BigEndian(static_cast<uint64_t>(payload.size())));
  return IndexCanonicalValue{
      PhysicalType::kBinary, std::move(bytes),
      IndexCanonicalKind::kBlobHash};
}

IndexCanonicalValue EncodeIndexBlobHash(const BlobRef& reference) {
  std::string bytes(
      reinterpret_cast<const char*>(reference.content_hash.bytes.data()),
      reference.content_hash.bytes.size());
  bytes.append(BigEndian(reference.raw_length));
  return IndexCanonicalValue{
      PhysicalType::kBinary, std::move(bytes),
      IndexCanonicalKind::kBlobHash};
}

int CompareIndexCanonicalValues(const IndexCanonicalValue& left,
                                const IndexCanonicalValue& right) {
  if (left.type != right.type) {
    return static_cast<uint8_t>(left.type) < static_cast<uint8_t>(right.type) ? -1 : 1;
  }
  if (left.kind != right.kind) {
    return static_cast<uint8_t>(left.kind) < static_cast<uint8_t>(right.kind)
        ? -1
        : 1;
  }
  if (left.bytes == right.bytes) return 0;
  return left.bytes < right.bytes ? -1 : 1;
}

}  // namespace cedar
