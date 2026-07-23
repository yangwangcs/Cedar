// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/columnar/page_format.h"

#include <algorithm>
#include <climits>
#include <cstring>
#include <limits>
#include <map>

#include <lz4.h>
#ifndef CEDAR_HAS_ZSTD
#define CEDAR_HAS_ZSTD 0
#endif
#if CEDAR_HAS_ZSTD
#include <zstd.h>
#endif

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {
constexpr uint32_t kMagic = 0x32475043U;  // CPG2
constexpr uint32_t kDirectoryMagic = 0x33524450U;  // PDR3
constexpr uint16_t kHeaderSize = kPageFormatHeaderSize;
void Put8(std::string* o,uint8_t v){o->push_back(static_cast<char>(v));}
void Put16(std::string* o,uint16_t v){Put8(o,v);Put8(o,v>>8);}
void Put32(std::string* o,uint32_t v){for(int i=0;i<4;++i)Put8(o,v>>(i*8));}
void Put64(std::string* o,uint64_t v){for(int i=0;i<8;++i)Put8(o,v>>(i*8));}
bool Get8(const std::string&i,size_t*p,uint8_t*v){if(*p>=i.size())return false;*v=static_cast<uint8_t>(i[(*p)++]);return true;}
bool Get16(const std::string&i,size_t*p,uint16_t*v){uint8_t a,b;if(!Get8(i,p,&a)||!Get8(i,p,&b))return false;*v=a|(static_cast<uint16_t>(b)<<8);return true;}
bool Get32(const std::string&i,size_t*p,uint32_t*v){*v=0;for(int n=0;n<4;++n){uint8_t b;if(!Get8(i,p,&b))return false;*v|=static_cast<uint32_t>(b)<<(n*8);}return true;}
bool Get64(const std::string&i,size_t*p,uint64_t*v){*v=0;for(int n=0;n<8;++n){uint8_t b;if(!Get8(i,p,&b))return false;*v|=static_cast<uint64_t>(b)<<(n*8);}return true;}
bool ValidPhysical(uint8_t value){return value>=static_cast<uint8_t>(PhysicalType::kBool)&&value<=static_cast<uint8_t>(PhysicalType::kBinary);}
void PutVarint(std::string* output,uint64_t value){while(value>=0x80){output->push_back(static_cast<char>(value|0x80));value>>=7;}output->push_back(static_cast<char>(value));}
bool GetVarint(const std::string& input,size_t* offset,uint64_t* value){*value=0;for(uint32_t shift=0;shift<64;shift+=7){uint8_t byte;if(!Get8(input,offset,&byte))return false;if(shift==63&&(byte&0xfe)!=0)return false;*value|=static_cast<uint64_t>(byte&0x7f)<<shift;if((byte&0x80)==0)return true;}return false;}
std::string EncodeRle(const std::string& input){std::string output;for(size_t offset=0;offset<input.size();){const char value=input[offset];size_t count=1;while(offset+count<input.size()&&input[offset+count]==value&&count<255)++count;Put8(&output,static_cast<uint8_t>(count));output.push_back(value);offset+=count;}return output;}
bool DecodeRle(const std::string& input,uint64_t output_size,std::string* output){output->clear();output->reserve(static_cast<size_t>(output_size));size_t offset=0;while(offset<input.size()){uint8_t count;if(!Get8(input,&offset,&count)||count==0||offset>=input.size()||count>output_size-output->size())return false;output->append(count,input[offset++]);}return output->size()==output_size;}
std::string EncodeDelta(const std::string& input){std::string output;if(input.empty())return output;size_t offset=0;uint64_t previous=0;if(!Get64(input,&offset,&previous))return {};Put64(&output,previous);while(offset<input.size()){uint64_t value;if(!Get64(input,&offset,&value))return {};PutVarint(&output,value-previous);previous=value;}return output;}
bool DecodeDelta(const std::string& input,uint64_t output_size,std::string* output){if(output_size==0){if(!input.empty())return false;output->clear();return true;}if(output_size%sizeof(uint64_t)!=0||input.size()<sizeof(uint64_t))return false;size_t offset=0;uint64_t previous;if(!Get64(input,&offset,&previous))return false;output->clear();output->reserve(static_cast<size_t>(output_size));Put64(output,previous);while(offset<input.size()){uint64_t delta;if(!GetVarint(input,&offset,&delta)||output->size()+sizeof(uint64_t)>output_size)return false;previous+=delta;Put64(output,previous);}return output->size()==output_size;}
StatusOr<std::string> EncodeXor(const std::string& input, uint32_t width) {
  if (width != sizeof(uint32_t) && width != sizeof(uint64_t)) {
    return Status::InvalidArgument("page", "invalid XOR value width");
  }
  if (input.size() % width != 0) {
    return Status::InvalidArgument("page", "XOR input is not value aligned");
  }
  const size_t count = input.size() / width;
  if (count == 0) return std::string();
  std::string output;
  output.reserve(width + count);
  size_t offset = 0;
  uint64_t previous = 0;
  if (width == sizeof(uint32_t)) {
    uint32_t value = 0;
    if (!Get32(input, &offset, &value)) {
      return Status::InvalidArgument("page", "invalid XOR input");
    }
    previous = value;
    Put32(&output, value);
  } else {
    if (!Get64(input, &offset, &previous)) {
      return Status::InvalidArgument("page", "invalid XOR input");
    }
    Put64(&output, previous);
  }
  while (offset < input.size()) {
    uint64_t value = 0;
    if (width == sizeof(uint32_t)) {
      uint32_t narrow = 0;
      if (!Get32(input, &offset, &narrow)) {
        return Status::InvalidArgument("page", "invalid XOR input");
      }
      value = narrow;
    } else if (!Get64(input, &offset, &value)) {
      return Status::InvalidArgument("page", "invalid XOR input");
    }
    PutVarint(&output, value ^ previous);
    previous = value;
  }
  return output;
}
bool DecodeXor(const std::string& input, uint32_t value_count,
               uint64_t output_size, uint32_t width, std::string* output) {
  if ((width != sizeof(uint32_t) && width != sizeof(uint64_t)) ||
      output_size % width != 0 || value_count != output_size / width) {
    return false;
  }
  if (value_count == 0) {
    output->clear();
    return input.empty();
  }
  if (input.size() < width) return false;
  size_t offset = 0;
  uint64_t previous = 0;
  if (width == sizeof(uint32_t)) {
    uint32_t value = 0;
    if (!Get32(input, &offset, &value)) return false;
    previous = value;
    Put32(output, value);
  } else {
    if (!Get64(input, &offset, &previous)) return false;
    Put64(output, previous);
  }
  for (uint32_t index = 1; index < value_count; ++index) {
    uint64_t delta = 0;
    if (!GetVarint(input, &offset, &delta)) return false;
    const uint64_t value = previous ^ delta;
    if (width == sizeof(uint32_t)) {
      if (value > std::numeric_limits<uint32_t>::max()) return false;
      Put32(output, static_cast<uint32_t>(value));
    } else {
      Put64(output, value);
    }
    previous = value;
  }
  return offset == input.size() && output->size() == output_size;
}
StatusOr<std::string> EncodeFrameOfReference(const std::string& input) {
  if (input.size() % sizeof(uint64_t) != 0) {
    return Status::InvalidArgument("page", "frame-of-reference input is not 64-bit aligned");
  }
  const size_t count = input.size() / sizeof(uint64_t);
  if (count == 0) return std::string();
  std::vector<uint64_t> values;
  values.reserve(count);
  size_t offset = 0;
  uint64_t minimum = std::numeric_limits<uint64_t>::max();
  for (size_t index = 0; index < count; ++index) {
    uint64_t value = 0;
    if (!Get64(input, &offset, &value)) {
      return Status::InvalidArgument("page", "invalid frame-of-reference input");
    }
    values.push_back(value);
    minimum = std::min(minimum, value);
  }
  std::string output;
  Put64(&output, minimum);
  for (uint64_t value : values) PutVarint(&output, value - minimum);
  return output;
}
bool DecodeFrameOfReference(const std::string& input, uint32_t value_count,
                            uint64_t output_size, std::string* output) {
  if (output_size % sizeof(uint64_t) != 0 ||
      value_count != output_size / sizeof(uint64_t) ||
      (value_count != 0 && input.size() < sizeof(uint64_t))) {
    return false;
  }
  if (value_count == 0) {
    output->clear();
    return input.empty();
  }
  size_t offset = 0;
  uint64_t minimum = 0;
  if (!Get64(input, &offset, &minimum)) return false;
  output->clear();
  output->reserve(static_cast<size_t>(output_size));
  for (uint32_t index = 0; index < value_count; ++index) {
    uint64_t delta = 0;
    if (!GetVarint(input, &offset, &delta) ||
        delta > std::numeric_limits<uint64_t>::max() - minimum) {
      return false;
    }
    Put64(output, minimum + delta);
  }
  return offset == input.size() && output->size() == output_size;
}
StatusOr<std::string> EncodeBitmap(const std::string& input,
                                   uint32_t value_count) {
  if (input.size() != value_count) {
    return Status::InvalidArgument(
        "page", "bitmap value count differs from payload size");
  }
  std::string output((value_count + 7U) / 8U, '\0');
  for (uint32_t index = 0; index < value_count; ++index) {
    const uint8_t value = static_cast<uint8_t>(input[index]);
    if (value > 1) {
      return Status::InvalidArgument("page", "bitmap input is not boolean");
    }
    if (value != 0) {
      output[index / 8] = static_cast<char>(
          static_cast<uint8_t>(output[index / 8]) | (1U << (index % 8)));
    }
  }
  return output;
}
bool DecodeBitmap(const std::string& input, uint32_t value_count,
                  uint64_t output_size, std::string* output) {
  if (output_size != value_count ||
      input.size() != (static_cast<uint64_t>(value_count) + 7U) / 8U) {
    return false;
  }
  if (value_count != 0 && value_count % 8 != 0) {
    const uint8_t used_mask = static_cast<uint8_t>((1U << (value_count % 8)) - 1U);
    if ((static_cast<uint8_t>(input.back()) & ~used_mask) != 0) return false;
  }
  output->assign(value_count, '\0');
  for (uint32_t index = 0; index < value_count; ++index) {
    (*output)[index] = static_cast<char>(
        (static_cast<uint8_t>(input[index / 8]) >> (index % 8)) & 1U);
  }
  return true;
}
StatusOr<std::string> EncodeBitPacking(const std::string& input) {
  if (input.size() % sizeof(uint64_t) != 0) {
    return Status::InvalidArgument("page", "bit-packing input is not 64-bit aligned");
  }
  const size_t count = input.size() / sizeof(uint64_t);
  if (count == 0) return std::string();
  std::vector<uint64_t> values;
  values.reserve(count);
  size_t offset = 0;
  uint64_t minimum = std::numeric_limits<uint64_t>::max();
  uint64_t maximum = 0;
  for (size_t index = 0; index < count; ++index) {
    uint64_t value = 0;
    if (!Get64(input, &offset, &value)) {
      return Status::InvalidArgument("page", "invalid bit-packing input");
    }
    values.push_back(value);
    minimum = std::min(minimum, value);
    maximum = std::max(maximum, value);
  }
  const uint64_t range = maximum - minimum;
  uint8_t bit_width = 0;
  while (bit_width < 64 && (bit_width == 0
      ? range != 0 : (range >> bit_width) != 0)) {
    ++bit_width;
  }
  const uint64_t packed_bits = static_cast<uint64_t>(count) * bit_width;
  const uint64_t packed_bytes = (packed_bits + 7U) / 8U;
  if (packed_bytes > kHardMaxPageBytes - 9U) {
    return Status::QueryMemoryLimit("page", "bit-packing payload exceeds hard page bound");
  }
  std::string output;
  output.reserve(9 + static_cast<size_t>(packed_bytes));
  Put64(&output, minimum);
  output.push_back(static_cast<char>(bit_width));
  output.resize(9 + static_cast<size_t>(packed_bytes), '\0');
  uint64_t bit_offset = 0;
  for (uint64_t value : values) {
    uint64_t delta = value - minimum;
    for (uint8_t bit = 0; bit < bit_width; ++bit, ++bit_offset) {
      if (((delta >> bit) & 1U) != 0) {
        output[9 + bit_offset / 8] = static_cast<char>(
            static_cast<uint8_t>(output[9 + bit_offset / 8]) |
            (1U << (bit_offset % 8)));
      }
    }
  }
  return output;
}
bool DecodeBitPacking(const std::string& input, uint32_t value_count,
                      uint64_t output_size, std::string* output) {
  if (output_size % sizeof(uint64_t) != 0 ||
      value_count != output_size / sizeof(uint64_t)) {
    return false;
  }
  if (value_count == 0) {
    output->clear();
    return input.empty();
  }
  if (input.size() < 9) return false;
  size_t offset = 0;
  uint64_t minimum = 0;
  if (!Get64(input, &offset, &minimum)) return false;
  const uint8_t bit_width = static_cast<uint8_t>(input[offset++]);
  const uint64_t packed_bits = static_cast<uint64_t>(value_count) * bit_width;
  const uint64_t packed_bytes = (packed_bits + 7U) / 8U;
  if (input.size() != 9 + packed_bytes) return false;
  if (packed_bits % 8 != 0 && packed_bytes != 0) {
    const uint8_t used_mask = static_cast<uint8_t>(
        (1U << (packed_bits % 8)) - 1U);
    if ((static_cast<uint8_t>(input.back()) & ~used_mask) != 0) return false;
  }
  output->clear();
  output->reserve(static_cast<size_t>(output_size));
  uint64_t bit_offset = 0;
  for (uint32_t index = 0; index < value_count; ++index) {
    uint64_t delta = 0;
    for (uint8_t bit = 0; bit < bit_width; ++bit, ++bit_offset) {
      if ((static_cast<uint8_t>(input[9 + bit_offset / 8]) >>
           (bit_offset % 8)) & 1U) {
        delta |= uint64_t{1} << bit;
      }
    }
    if (delta > std::numeric_limits<uint64_t>::max() - minimum) return false;
    Put64(output, minimum + delta);
  }
  return output->size() == output_size;
}
uint64_t ZigZagEncode(int64_t value) {
  return (static_cast<uint64_t>(value) << 1) ^
      static_cast<uint64_t>(-(value < 0));
}
int64_t ZigZagDecode(uint64_t value) {
  const uint64_t magnitude = value >> 1;
  return static_cast<int64_t>(magnitude ^ static_cast<uint64_t>(-
      static_cast<int64_t>(value & 1U)));
}
StatusOr<std::string> EncodeDeltaOfDelta(const std::string& input) {
  if (input.size() % sizeof(uint64_t) != 0) {
    return Status::InvalidArgument("page", "delta-of-delta input is not 64-bit aligned");
  }
  const size_t count = input.size() / sizeof(uint64_t);
  if (count == 0) return std::string();
  std::vector<uint64_t> values;
  values.reserve(count);
  size_t offset = 0;
  for (size_t index = 0; index < count; ++index) {
    uint64_t value = 0;
    if (!Get64(input, &offset, &value)) return Status::InvalidArgument(
        "page", "invalid delta-of-delta input");
    values.push_back(value);
  }
  std::string output;
  Put64(&output, values.front());
  if (values.size() == 1) return output;
  if (values[1] < values[0] || values[1] - values[0] > INT64_MAX) {
    return Status::InvalidArgument("page", "delta-of-delta values are not monotonic");
  }
  int64_t previous_delta = static_cast<int64_t>(values[1] - values[0]);
  PutVarint(&output, ZigZagEncode(previous_delta));
  for (size_t index = 2; index < values.size(); ++index) {
    if (values[index] < values[index - 1] ||
        values[index] - values[index - 1] > INT64_MAX) {
      return Status::InvalidArgument("page", "delta-of-delta values are not monotonic");
    }
    const int64_t delta = static_cast<int64_t>(values[index] - values[index - 1]);
    if ((delta > 0 && previous_delta > INT64_MAX - delta) ||
        (delta < 0 && previous_delta < INT64_MIN - delta)) {
      return Status::InvalidArgument("page", "delta-of-delta delta overflow");
    }
    const int64_t delta_of_delta = delta - previous_delta;
    PutVarint(&output, ZigZagEncode(delta_of_delta));
    previous_delta = delta;
  }
  return output;
}
bool DecodeDeltaOfDelta(const std::string& input, uint32_t value_count,
                        uint64_t output_size, std::string* output) {
  if (output_size % sizeof(uint64_t) != 0 ||
      value_count != output_size / sizeof(uint64_t)) return false;
  if (value_count == 0) return input.empty();
  if (input.size() < sizeof(uint64_t)) return false;
  size_t offset = 0;
  uint64_t current = 0;
  if (!Get64(input, &offset, &current)) return false;
  output->clear();
  output->reserve(static_cast<size_t>(output_size));
  Put64(output, current);
  if (value_count == 1) return offset == input.size();
  uint64_t encoded_delta = 0;
  if (!GetVarint(input, &offset, &encoded_delta)) return false;
  int64_t previous_delta = ZigZagDecode(encoded_delta);
  if (previous_delta < 0 ||
      static_cast<uint64_t>(previous_delta) >
          std::numeric_limits<uint64_t>::max() - current) return false;
  current += static_cast<uint64_t>(previous_delta);
  Put64(output, current);
  for (uint32_t index = 2; index < value_count; ++index) {
    if (!GetVarint(input, &offset, &encoded_delta)) return false;
    const int64_t delta_of_delta = ZigZagDecode(encoded_delta);
    if ((delta_of_delta > 0 && previous_delta > INT64_MAX - delta_of_delta) ||
        (delta_of_delta < 0 && previous_delta < INT64_MIN - delta_of_delta)) {
      return false;
    }
    const int64_t delta = previous_delta + delta_of_delta;
    if (delta < 0 || static_cast<uint64_t>(delta) >
        std::numeric_limits<uint64_t>::max() - current) return false;
    current += static_cast<uint64_t>(delta);
    Put64(output, current);
    previous_delta = delta;
  }
  return offset == input.size() && output->size() == output_size;
}
StatusOr<std::string> EncodeDictionary(const std::string& input,
                                       uint32_t value_count) {
  size_t offset = 0;
  std::map<std::string, uint32_t> indexes;
  std::vector<std::string> dictionary;
  std::vector<uint32_t> encoded_indexes;
  dictionary.reserve(value_count);
  encoded_indexes.reserve(value_count);
  uint64_t dictionary_bytes = sizeof(uint32_t);
  for (uint32_t value = 0; value < value_count; ++value) {
    uint32_t length = 0;
    if (!Get32(input, &offset, &length) || length > input.size() - offset) {
      return Status::InvalidArgument("page", "invalid dictionary input record");
    }
    std::string record = input.substr(offset, length);
    offset += length;
    const auto inserted = indexes.emplace(record,
                                          static_cast<uint32_t>(dictionary.size()));
    if (inserted.second) {
      dictionary_bytes += sizeof(uint32_t) + record.size();
      dictionary.push_back(std::move(record));
    }
    encoded_indexes.push_back(inserted.first->second);
  }
  if (offset != input.size()) {
    return Status::InvalidArgument("page", "trailing dictionary input data");
  }
  const uint64_t encoded_bytes =
      dictionary_bytes + static_cast<uint64_t>(value_count) * sizeof(uint32_t);
  if (encoded_bytes > kHardMaxPageBytes) {
    return Status::QueryMemoryLimit("page", "dictionary payload exceeds hard page bound");
  }
  std::string output;
  output.reserve(static_cast<size_t>(encoded_bytes));
  Put32(&output, static_cast<uint32_t>(dictionary.size()));
  for (const std::string& record : dictionary) {
    Put32(&output, static_cast<uint32_t>(record.size()));
    output.append(record);
  }
  for (uint32_t index : encoded_indexes) Put32(&output, index);
  return output;
}

bool DecodeDictionary(const std::string& input, uint32_t value_count,
                      uint64_t output_size, std::string* output) {
  if (output_size > kHardMaxPageBytes ||
      value_count > output_size / sizeof(uint32_t)) {
    return false;
  }
  size_t offset = 0;
  uint32_t dictionary_count = 0;
  if (!Get32(input, &offset, &dictionary_count) ||
      dictionary_count > value_count) {
    return false;
  }
  std::vector<std::string> dictionary;
  dictionary.reserve(dictionary_count);
  for (uint32_t entry = 0; entry < dictionary_count; ++entry) {
    uint32_t length = 0;
    if (!Get32(input, &offset, &length) || length > input.size() - offset ||
        length > output_size) {
      return false;
    }
    dictionary.emplace_back(input.substr(offset, length));
    offset += length;
  }
  const uint64_t index_bytes =
      static_cast<uint64_t>(value_count) * sizeof(uint32_t);
  if (index_bytes != input.size() - offset) return false;
  output->clear();
  output->reserve(static_cast<size_t>(output_size));
  for (uint32_t value = 0; value < value_count; ++value) {
    uint32_t index = 0;
    if (!Get32(input, &offset, &index) || index >= dictionary.size()) return false;
    const std::string& record = dictionary[index];
    if (sizeof(uint32_t) + record.size() > output_size - output->size()) {
      return false;
    }
    Put32(output, static_cast<uint32_t>(record.size()));
    output->append(record);
  }
  return offset == input.size() && output->size() == output_size;
}

bool ValidEncoding(uint8_t value){return value>=static_cast<uint8_t>(EncodingId::kPlain)&&value<=static_cast<uint8_t>(EncodingId::kXor);}
bool ValidCompression(uint8_t value){return value>=static_cast<uint8_t>(CompressionId::kNone)&&value<=static_cast<uint8_t>(CompressionId::kZstd);}
}

PageCodecCapabilities GetPageCodecCapabilities() {
  PageCodecCapabilities capabilities;
  capabilities.lz4 = PageCodecCapability{"LZ4", LZ4_versionString(), true};
#if CEDAR_HAS_ZSTD
  capabilities.zstd =
      PageCodecCapability{"Zstd", ZSTD_versionString(), true};
#else
  capabilities.zstd = PageCodecCapability{"Zstd", "disabled", false};
#endif
  return capabilities;
}

Status VerifyPageCodecCapabilities() {
  const PageCodecCapabilities capabilities = GetPageCodecCapabilities();
  if (!capabilities.lz4.compiled || capabilities.lz4.version != "1.10.0") {
    return Status::NotSupported(
        "page codec", "Cedar requires bundled LZ4 1.10.0");
  }
#if CEDAR_HAS_ZSTD
  if (!capabilities.zstd.compiled || capabilities.zstd.version != "1.5.7") {
    return Status::NotSupported(
        "page codec", "Cedar requires bundled Zstd 1.5.7");
  }
#endif

  const std::string payload(4096, 'c');
  const std::vector<CompressionId> codecs = {
      CompressionId::kLz4
#if CEDAR_HAS_ZSTD
      , CompressionId::kZstd
#endif
  };
  for (CompressionId compression : codecs) {
    const auto encoded = EncodePageChecked(
        PageHeader{PageType::kTypedValue, PhysicalType::kBinary,
                   EncodingId::kPlain, compression, 0, 1, 1, 0},
        payload);
    if (!encoded.ok()) return encoded.status();
    const auto decoded = DecodePage(encoded.ValueOrDie());
    if (!decoded.ok()) return decoded.status();
    if (decoded.ValueOrDie().payload != payload ||
        decoded.ValueOrDie().header.compression_id != compression) {
      return Status::Corruption("page codec", "codec self-test mismatch");
    }
  }
  return Status::OK();
}

StatusOr<std::string> EncodePageChecked(PageHeader header,
                                         const std::string& payload) {
  if (payload.size() > kHardMaxPageBytes ||
      kHeaderSize > kHardMaxPageBytes - payload.size()) {
    return Status::QueryMemoryLimit("page", "payload exceeds hard page bound");
  }
  if ((header.required_flags & ~kPageSupportedRequiredFlags) != 0) {
    return Status::NotSupported("page", "unknown required feature flags");
  }
  if (!ValidEncoding(static_cast<uint8_t>(header.encoding_id))) {
    return Status::NotSupported("page", "unknown encoding");
  }
  if (!ValidCompression(static_cast<uint8_t>(header.compression_id))) {
    return Status::NotSupported("page", "unknown compression");
  }
  if (header.compression_id == CompressionId::kZstd && !CEDAR_HAS_ZSTD) {
    return Status::NotSupported("page", "Zstd compression is unavailable in this build");
  }
  if (header.encoding_id == EncodingId::kDelta &&
      payload.size() % sizeof(uint64_t) != 0) {
    return Status::InvalidArgument("page", "delta payload is not 64-bit aligned");
  }
  if (header.encoding_id == EncodingId::kDictionary &&
      header.page_type != PageType::kTypedValue) {
    return Status::InvalidArgument("page", "dictionary encoding requires typed values");
  }
  if (header.encoding_id == EncodingId::kFrameOfReference &&
      header.physical_type != PhysicalType::kInt64 &&
      header.physical_type != PhysicalType::kTimestamp64) {
    return Status::InvalidArgument(
        "page", "frame-of-reference encoding requires Int64 or Timestamp64 values");
  }
  if (header.encoding_id == EncodingId::kFrameOfReference &&
      payload.size() % sizeof(uint64_t) != 0) {
    return Status::InvalidArgument(
        "page", "frame-of-reference payload is not 64-bit aligned");
  }
  if (header.encoding_id == EncodingId::kBitmap &&
      header.physical_type != PhysicalType::kBool) {
    return Status::InvalidArgument("page", "bitmap encoding requires Bool values");
  }
  if (header.encoding_id == EncodingId::kBitPacking &&
      header.physical_type != PhysicalType::kInt64 &&
      header.physical_type != PhysicalType::kTimestamp64) {
    return Status::InvalidArgument(
        "page", "bit-packing encoding requires Int64 or Timestamp64 values");
  }
  if (header.encoding_id == EncodingId::kBitPacking &&
      payload.size() % sizeof(uint64_t) != 0) {
    return Status::InvalidArgument("page", "bit-packing payload is not 64-bit aligned");
  }
  if (header.encoding_id == EncodingId::kDeltaOfDelta &&
      (header.physical_type != PhysicalType::kInt64 &&
       header.physical_type != PhysicalType::kTimestamp64)) {
    return Status::InvalidArgument(
        "page", "delta-of-delta encoding requires Int64 or Timestamp64 values");
  }
  if (header.encoding_id == EncodingId::kDeltaOfDelta &&
      payload.size() % sizeof(uint64_t) != 0) {
    return Status::InvalidArgument(
        "page", "delta-of-delta payload is not 64-bit aligned");
  }
  if (header.encoding_id == EncodingId::kXor &&
      header.physical_type != PhysicalType::kFloat32 &&
      header.physical_type != PhysicalType::kFloat64) {
    return Status::InvalidArgument("page", "XOR encoding requires Float32 or Float64 values");
  }
  if (header.encoding_id == EncodingId::kXor) {
    const size_t width = header.physical_type == PhysicalType::kFloat32
        ? sizeof(uint32_t) : sizeof(uint64_t);
    if (payload.size() % width != 0 ||
        header.value_count != payload.size() / width) {
      return Status::InvalidArgument("page", "XOR payload/value count mismatch");
    }
  }

  header.uncompressed_size = payload.size();
  std::string encoded = payload;
  if (header.encoding_id == EncodingId::kRle) {
    encoded = EncodeRle(payload);
  } else if (header.encoding_id == EncodingId::kDelta) {
    encoded = EncodeDelta(payload);
  } else if (header.encoding_id == EncodingId::kDictionary) {
    const auto dictionary = EncodeDictionary(payload, header.value_count);
    if (!dictionary.ok()) return dictionary.status();
    encoded = dictionary.ValueOrDie();
  } else if (header.encoding_id == EncodingId::kFrameOfReference) {
    const auto frame = EncodeFrameOfReference(payload);
    if (!frame.ok()) return frame.status();
    encoded = frame.ValueOrDie();
  } else if (header.encoding_id == EncodingId::kBitmap) {
    const auto bitmap = EncodeBitmap(payload, header.value_count);
    if (!bitmap.ok()) return bitmap.status();
    encoded = bitmap.ValueOrDie();
  } else if (header.encoding_id == EncodingId::kBitPacking) {
    const auto packed = EncodeBitPacking(payload);
    if (!packed.ok()) return packed.status();
    encoded = packed.ValueOrDie();
  } else if (header.encoding_id == EncodingId::kDeltaOfDelta) {
    const auto delta_of_delta = EncodeDeltaOfDelta(payload);
    if (!delta_of_delta.ok()) return delta_of_delta.status();
    encoded = delta_of_delta.ValueOrDie();
  } else if (header.encoding_id == EncodingId::kXor) {
    const uint32_t width = header.physical_type == PhysicalType::kFloat32
        ? sizeof(uint32_t) : sizeof(uint64_t);
    const auto xor_encoded = EncodeXor(payload, width);
    if (!xor_encoded.ok()) return xor_encoded.status();
    encoded = xor_encoded.ValueOrDie();
  }
  // Avoid expansion and require the configured 12.5% minimum gain.
  if (header.encoding_id != EncodingId::kPlain &&
      ((encoded.empty() && !payload.empty()) ||
       encoded.size() * 8 > payload.size() * 7)) {
    header.encoding_id = EncodingId::kPlain;
    encoded = payload;
  }
  if (encoded.size() > kHardMaxPageBytes ||
      kHeaderSize > kHardMaxPageBytes - encoded.size()) {
    return Status::QueryMemoryLimit("page", "encoded payload exceeds hard page bound");
  }
  header.encoded_size = encoded.size();
  std::string stored = encoded;
  if (header.compression_id == CompressionId::kLz4 && !encoded.empty()) {
    if (encoded.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
      return Status::QueryMemoryLimit("page", "LZ4 input exceeds codec bound");
    }
    const int bound = LZ4_compressBound(static_cast<int>(encoded.size()));
    if (bound <= 0) {
      return Status::QueryMemoryLimit("page", "LZ4 bound is invalid");
    }
    std::string compressed(static_cast<size_t>(bound), '\0');
    const int compressed_size = LZ4_compress_default(
        encoded.data(), compressed.data(), static_cast<int>(encoded.size()), bound);
    if (compressed_size > 0 &&
        static_cast<uint64_t>(compressed_size) * 8 <= encoded.size() * 7) {
      compressed.resize(static_cast<size_t>(compressed_size));
      stored = std::move(compressed);
    } else {
      header.compression_id = CompressionId::kNone;
    }
  } else if (header.compression_id == CompressionId::kZstd && !encoded.empty()) {
#if CEDAR_HAS_ZSTD
    const size_t bound = ZSTD_compressBound(encoded.size());
    std::string compressed(bound, '\0');
    const size_t compressed_size = ZSTD_compress(
        compressed.data(), compressed.size(), encoded.data(), encoded.size(), 1);
    if (ZSTD_isError(compressed_size)) {
      return Status::IOError("page", ZSTD_getErrorName(compressed_size));
    }
    if (compressed_size * 8 <= encoded.size() * 7) {
      compressed.resize(compressed_size);
      stored = std::move(compressed);
    } else {
      header.compression_id = CompressionId::kNone;
    }
#endif
  } else if (header.compression_id != CompressionId::kNone) {
    header.compression_id = CompressionId::kNone;
  }
  if (stored.size() > kHardMaxPageBytes ||
      kHeaderSize > kHardMaxPageBytes - stored.size()) {
    return Status::QueryMemoryLimit("page", "stored payload exceeds hard page bound");
  }
  header.compressed_size = stored.size();
  header.payload_crc32c = crc32c::Value(stored.data(), stored.size());
  std::string out;
  out.reserve(kHeaderSize + stored.size());
  Put32(&out, kMagic);
  Put16(&out, kPageFormatVersion);
  Put16(&out, kHeaderSize);
  Put8(&out, static_cast<uint8_t>(header.page_type));
  Put8(&out, static_cast<uint8_t>(header.physical_type));
  Put8(&out, static_cast<uint8_t>(header.encoding_id));
  Put8(&out, static_cast<uint8_t>(header.compression_id));
  Put64(&out, header.first_row);
  Put32(&out, header.row_count);
  Put32(&out, header.value_count);
  Put64(&out, header.uncompressed_size);
  Put64(&out, header.encoded_size);
  Put64(&out, header.compressed_size);
  Put32(&out, header.payload_crc32c);
  Put32(&out, header.required_flags);
  out += stored;
  return out;
}

StatusOr<PageHeader> DecodePageHeader(const std::string& encoded_header) {
  if (encoded_header.size() < kHeaderSize) {
    return Status::Corruption("page", "truncated header");
  }
  size_t p = 0;
  uint32_t magic, flags, rows, values, crc;
  uint16_t version, size;
  uint8_t type, physical, encoding, compression;
  uint64_t first, uncompressed, encoded_size, compressed;
  if (!Get32(encoded_header, &p, &magic) ||
      !Get16(encoded_header, &p, &version) ||
      !Get16(encoded_header, &p, &size) || magic != kMagic ||
      version != kPageFormatVersion || size != kHeaderSize) {
    return Status::Corruption("page", "unsupported header");
  }
  if (!Get8(encoded_header, &p, &type) ||
      !Get8(encoded_header, &p, &physical) ||
      !Get8(encoded_header, &p, &encoding) ||
      !Get8(encoded_header, &p, &compression) ||
      !Get64(encoded_header, &p, &first) ||
      !Get32(encoded_header, &p, &rows) ||
      !Get32(encoded_header, &p, &values) ||
      !Get64(encoded_header, &p, &uncompressed) ||
      !Get64(encoded_header, &p, &encoded_size) ||
      !Get64(encoded_header, &p, &compressed) ||
      !Get32(encoded_header, &p, &crc) ||
      !Get32(encoded_header, &p, &flags)) {
    return Status::Corruption("page", "invalid header");
  }
  if (type < static_cast<uint8_t>(PageType::kEntityId) ||
      type > static_cast<uint8_t>(PageType::kBlobPresence) ||
      !ValidPhysical(physical) || !ValidEncoding(encoding)) {
    return Status::Corruption("page", "invalid type");
  }
  if (!ValidCompression(compression)) {
    return Status::NotSupported("page", "unknown compression");
  }
  if ((flags & ~kPageSupportedRequiredFlags) != 0) {
    return Status::NotSupported("page", "unknown required feature flags");
  }
  if (compressed > kHardMaxPageBytes || encoded_size > kHardMaxPageBytes ||
      uncompressed > kHardMaxPageBytes) {
    return Status::Corruption("page", "invalid payload size");
  }
  return PageHeader{static_cast<PageType>(type), static_cast<PhysicalType>(physical),
                    static_cast<EncodingId>(encoding),
                    static_cast<CompressionId>(compression), first, rows, values,
                    flags, uncompressed, encoded_size, compressed, crc};
}

StatusOr<Page> DecodePage(const std::string& encoded){
  const auto decoded_header = DecodePageHeader(encoded);
  if (!decoded_header.ok()) return decoded_header.status();
  const PageHeader& header = decoded_header.ValueOrDie();
  if (encoded.size() - kHeaderSize != header.compressed_size) {
    return Status::Corruption("page", "invalid payload size");
  }
  const std::string stored=encoded.substr(kHeaderSize);if(crc32c::Value(stored.data(),stored.size())!=header.payload_crc32c)return Status::Corruption("page","payload checksum mismatch");
  std::string encoded_payload;
  if(header.compression_id==CompressionId::kNone){
    if(header.encoded_size!=header.compressed_size)return Status::Corruption("page","uncompressed size mismatch");
    encoded_payload=stored;
  }else if (header.compression_id == CompressionId::kLz4) {
    encoded_payload.resize(static_cast<size_t>(header.encoded_size));
    const int decoded=LZ4_decompress_safe(stored.data(),encoded_payload.data(),
                                           static_cast<int>(stored.size()),
                                           static_cast<int>(encoded_payload.size()));
    if(decoded<0||static_cast<uint64_t>(decoded)!=header.encoded_size)return Status::Corruption("page","invalid LZ4 payload");
  } else if (header.compression_id == CompressionId::kZstd) {
#if CEDAR_HAS_ZSTD
    encoded_payload.resize(static_cast<size_t>(header.encoded_size));
    const size_t decoded = ZSTD_decompress(
        encoded_payload.data(), encoded_payload.size(), stored.data(), stored.size());
    if (ZSTD_isError(decoded) || decoded != header.encoded_size) {
      return Status::Corruption("page", "invalid Zstd payload");
    }
#else
    return Status::NotSupported("page", "Zstd compression is unavailable in this build");
#endif
  } else {
    return Status::NotSupported("page", "unknown compression");
  }
  std::string payload;
  if(header.encoding_id==EncodingId::kPlain)payload=encoded_payload;
  else if(header.encoding_id==EncodingId::kRle){if(!DecodeRle(encoded_payload,header.uncompressed_size,&payload))return Status::Corruption("page","invalid RLE payload");}
  else if(header.encoding_id==EncodingId::kDelta){if(!DecodeDelta(encoded_payload,header.uncompressed_size,&payload))return Status::Corruption("page","invalid delta payload");}
  else if (header.encoding_id == EncodingId::kDictionary) {
    if (header.page_type != PageType::kTypedValue ||
        !DecodeDictionary(encoded_payload, header.value_count,
                          header.uncompressed_size, &payload)) {
      return Status::Corruption("page", "invalid dictionary payload");
    }
  } else if (header.encoding_id == EncodingId::kFrameOfReference) {
    if ((header.physical_type != PhysicalType::kInt64 &&
         header.physical_type != PhysicalType::kTimestamp64) ||
        !DecodeFrameOfReference(encoded_payload, header.value_count,
                                header.uncompressed_size, &payload)) {
      return Status::Corruption("page", "invalid frame-of-reference payload");
    }
  } else if (header.encoding_id == EncodingId::kBitmap) {
    if (header.physical_type != PhysicalType::kBool ||
        !DecodeBitmap(encoded_payload, header.value_count,
                      header.uncompressed_size, &payload)) {
      return Status::Corruption("page", "invalid bitmap payload");
    }
  } else if (header.encoding_id == EncodingId::kBitPacking) {
    if ((header.physical_type != PhysicalType::kInt64 &&
         header.physical_type != PhysicalType::kTimestamp64) ||
        !DecodeBitPacking(encoded_payload, header.value_count,
                          header.uncompressed_size, &payload)) {
      return Status::Corruption("page", "invalid bit-packing payload");
    }
  } else if (header.encoding_id == EncodingId::kDeltaOfDelta) {
    if ((header.physical_type != PhysicalType::kInt64 &&
         header.physical_type != PhysicalType::kTimestamp64) ||
        !DecodeDeltaOfDelta(encoded_payload, header.value_count,
                            header.uncompressed_size, &payload)) {
        return Status::Corruption("page", "invalid delta-of-delta payload");
    }
  } else if (header.encoding_id == EncodingId::kXor) {
    if (header.physical_type != PhysicalType::kFloat32 &&
        header.physical_type != PhysicalType::kFloat64) {
      return Status::Corruption("page", "invalid XOR physical type");
    }
    const uint32_t width = header.physical_type == PhysicalType::kFloat32
        ? sizeof(uint32_t) : sizeof(uint64_t);
    if (!DecodeXor(encoded_payload, header.value_count,
                   header.uncompressed_size, width, &payload)) {
      return Status::Corruption("page", "invalid XOR payload");
    }
  }
  if(payload.size()!=header.uncompressed_size)return Status::Corruption("page","decoded size mismatch");
  return Page{header,std::move(payload)};
}

std::string EncodePageDirectory(const std::vector<PageDirectoryEntry>& entries) {
  constexpr size_t kDirectoryEntryBytes = 53;
  std::string out; out.reserve(8 + entries.size() * kDirectoryEntryBytes); Put32(&out, kDirectoryMagic);
  Put32(&out, static_cast<uint32_t>(entries.size()));
  for (const PageDirectoryEntry& entry : entries) {
    Put8(&out, static_cast<uint8_t>(entry.page_type)); Put32(&out, entry.ordinal);
    Put64(&out, entry.offset); Put64(&out, entry.length);
    out.append(reinterpret_cast<const char*>(entry.content_hash.data()),
               entry.content_hash.size());
  }
  return out;
}

StatusOr<std::vector<PageDirectoryEntry>> DecodePageDirectory(const std::string& encoded) {
  constexpr size_t kDirectoryEntryBytes = 53;
  size_t p = 0; uint32_t magic, count;
  if (!Get32(encoded, &p, &magic) || !Get32(encoded, &p, &count) ||
      magic != kDirectoryMagic || count > 65536 ||
      encoded.size() - p != count * kDirectoryEntryBytes)
    return Status::Corruption("page directory", "invalid header or size");
  std::vector<PageDirectoryEntry> entries; entries.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    uint8_t type; PageDirectoryEntry entry;
    if (!Get8(encoded, &p, &type) || !Get32(encoded, &p, &entry.ordinal) ||
        !Get64(encoded, &p, &entry.offset) || !Get64(encoded, &p, &entry.length) ||
        p + entry.content_hash.size() > encoded.size() ||
        type < static_cast<uint8_t>(PageType::kEntityId) ||
        type > static_cast<uint8_t>(PageType::kBlobPresence) || entry.length == 0)
      return Status::Corruption("page directory", "invalid entry");
    std::memcpy(entry.content_hash.data(), encoded.data() + p,
                entry.content_hash.size());
    p += entry.content_hash.size();
    entry.page_type = static_cast<PageType>(type); entries.push_back(entry);
  }
  return entries;
}

}  // namespace cedar
