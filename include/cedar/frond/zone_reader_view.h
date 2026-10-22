// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// ZoneReaderView - SST 文件的只读零拷贝视图（Task 3.1）
// =============================================================================
// Open 时优先 mmap(PROT_READ)（文件页按需缺页，不复制到堆）；mmap 不可用时
// 回退到整文件 pread 到堆（仍由本对象拥有）。Close 时 munmap/释放。
// 读端所有 zone 都是该视图上的 Slice，decoder 不复制 zone 数据。
// =============================================================================

#ifndef FERN_FROND_ZONE_READER_VIEW_H_
#define FERN_FROND_ZONE_READER_VIEW_H_

#include <cstddef>
#include <string>

#include "cedar/core/status.h"

namespace cedar {

class ZoneReaderView {
 public:
  ZoneReaderView() = default;
  ~ZoneReaderView() { Close(); }

  ZoneReaderView(const ZoneReaderView&) = delete;
  ZoneReaderView& operator=(const ZoneReaderView&) = delete;

  // 打开文件：mmap 成功则 mapped()==true，否则回退 read-all（owned heap）。
  Status OpenFile(const std::string& path);

  // 非拥有视图（测试用内存缓冲区）。
  void Reset(const char* data, size_t size) {
    Close();
    data_ = data;
    size_ = size;
    mapped_ = false;
    owned_ = nullptr;
  }

  void Close();

  const char* data() const { return data_; }
  size_t size() const { return size_; }
  bool mapped() const { return mapped_; }
  bool valid() const { return data_ != nullptr; }

 private:
  const char* data_ = nullptr;
  size_t size_ = 0;
  bool mapped_ = false;
  void* map_addr_ = nullptr;  // mmap 基址
  char* owned_ = nullptr;     // pread fallback 的堆缓冲
};

}  // namespace cedar

#endif  // FERN_FROND_ZONE_READER_VIEW_H_
