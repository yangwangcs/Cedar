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

#include "cedar/transaction/wal.h"

#include <chrono>
#include <cstring>
#include <future>

#include "cedar/core/env.h"
#include "cedar/core/crc32c.h"

namespace cedar {

// Helper functions for encoding
static void EncodeFixed32(char* buf, uint32_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
}

static void EncodeFixed16(char* buf, uint16_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
}

static void EncodeFixed64(char* buf, uint64_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  buf[4] = (value >> 32) & 0xff;
  buf[5] = (value >> 40) & 0xff;
  buf[6] = (value >> 48) & 0xff;
  buf[7] = (value >> 56) & 0xff;
}

static uint32_t DecodeFixed32(const char* ptr) {
  return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
}

static uint16_t DecodeFixed16(const char* ptr) {
  return static_cast<uint16_t>(static_cast<unsigned char>(ptr[0]))
       | (static_cast<uint16_t>(static_cast<unsigned char>(ptr[1])) << 8);
}

static uint64_t DecodeFixed64(const char* ptr) {
  return (static_cast<uint64_t>(static_cast<unsigned char>(ptr[0])))
       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[1])) << 8)
       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[2])) << 16)
       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[3])) << 24)
       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[4])) << 32)
       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[5])) << 40)
       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[6])) << 48)
       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[7])) << 56);
}

// ========== WalRecordHeader ==========

void WalRecordHeader::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  EncodeFixed32(buf, crc32);
  EncodeFixed16(buf + 4, type);
  EncodeFixed16(buf + 6, flags);
  EncodeFixed32(buf + 8, data_length);
  EncodeFixed32(buf + 12, sequence);
  dst->append(buf, kEncodedSize);
}

Status WalRecordHeader::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("WalRecordHeader", "truncated");
  }
  
  const char* p = input->data();
  crc32 = DecodeFixed32(p);
  type = DecodeFixed16(p + 4);
  flags = DecodeFixed16(p + 6);
  data_length = DecodeFixed32(p + 8);
  sequence = DecodeFixed32(p + 12);
  
  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// ========== WalBatch ==========

void WalBatch::Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version) {
  ops_.emplace_back(WalRecordType::kPut, key, descriptor, txn_version);
}

void WalBatch::Delete(const CedarKey& key, Timestamp txn_version) {
  ops_.emplace_back(WalRecordType::kDelete, key, Descriptor::Tombstone(0), txn_version);
}

void WalBatch::Clear() {
  ops_.clear();
}

void WalBatch::EncodeTo(std::string* dst) const {
  // 修复: 格式更新为 [num_ops: 4 bytes][op1][op2]...
  // 每个操作: [type: 1 byte][key_len: 4 bytes][key][desc: 8 bytes][txn_version: 8 bytes]
  char buf[8];
  EncodeFixed32(buf, static_cast<uint32_t>(ops_.size()));
  dst->append(buf, 4);
  
  for (const auto& op : ops_) {
    std::string key_encoded = op.key.Encode();
    
    dst->push_back(static_cast<char>(op.type));
    
    EncodeFixed32(buf, static_cast<uint32_t>(key_encoded.size()));
    dst->append(buf, 4);
    
    dst->append(key_encoded);
    dst->append(op.descriptor.Encode());
    
    // 修复: 添加 txn_version 的编码
    EncodeFixed64(buf, op.txn_version.value());
    dst->append(buf, 8);
  }
}

Status WalBatch::DecodeFrom(Slice* input) {
  Clear();
  
  if (input->size() < 4) {
    return Status::Corruption("WalBatch", "truncated header");
  }
  
  uint32_t num_ops = DecodeFixed32(input->data());
  input->remove_prefix(4);
  
  for (uint32_t i = 0; i < num_ops; i++) {
    // 修复: 更新为新的格式 [type: 1][key_len: 4][key][desc: 8][txn_version: 8]
    if (input->size() < 1 + 4) {
      return Status::Corruption("WalBatch", "truncated op header");
    }
    
    WalRecordType type = static_cast<WalRecordType>(input->data()[0]);
    input->remove_prefix(1);
    
    uint32_t key_len = DecodeFixed32(input->data());
    input->remove_prefix(4);
    
    // 修复: 检查长度需要包含 desc(8) + txn_version(8) = 16 字节
    if (input->size() < key_len + 8 + 8) {
      return Status::Corruption("WalBatch", "truncated op data");
    }
    
    std::string_view key_slice(input->data(), key_len);
    auto key_opt = CedarKey::Decode(key_slice);
    if (!key_opt.has_value()) {
      return Status::Corruption("WalBatch", "invalid key");
    }
    input->remove_prefix(key_len);
    
    Slice desc_slice(input->data(), 8);
    auto desc_opt = Descriptor::Decode(desc_slice);
    if (!desc_opt.has_value()) {
      return Status::Corruption("WalBatch", "invalid descriptor");
    }
    input->remove_prefix(8);
    
    // 修复: 解码 txn_version
    uint64_t txn_ver = DecodeFixed64(input->data());
    input->remove_prefix(8);
    
    ops_.emplace_back(type, key_opt.value(), desc_opt.value(), Timestamp(txn_ver));
  }
  
  return Status::OK();
}

// ========== WalWriter ==========

WalWriter::WalWriter(const std::string& wal_dir,
                     cedar::Env* env,
                     const WalOptions& options)
    : wal_dir_(wal_dir),
      env_(env),
      options_(options),
      current_file_(nullptr),
      current_file_size_(0),
      file_number_(0) {}

WalWriter::~WalWriter() {
  Close();
}

Status WalWriter::Open() {
  // 创建 WAL 目录
  if (!env_->FileExists(wal_dir_)) {
    cedar::Status s = env_->CreateDir(wal_dir_);
    if (!s.ok()) {
      return Status::IOError("WalWriter", s.ToString());
    }
  }
  
  // 列出已有 WAL 文件，确定下一个文件号
  std::vector<std::string> files;
  cedar::Status s = env_->GetChildren(wal_dir_, &files);
  if (!s.ok()) {
    return Status::IOError("WalWriter", s.ToString());
  }
  
  for (const auto& file : files) {
    if (file.size() > 4 && file.substr(file.size() - 4) == ".wal") {
      // 解析文件号 (简单解析，不使用异常)
      uint32_t num = 0;
      bool valid = true;
      for (size_t i = 0; i < file.size() - 4; i++) {
        if (file[i] >= '0' && file[i] <= '9') {
          num = num * 10 + (file[i] - '0');
        } else {
          valid = false;
          break;
        }
      }
      if (valid && num >= file_number_) {
        file_number_ = num + 1;
      }
    }
  }
  
  // 创建新 WAL 文件
  Status st = SwitchWALFile();
  if (!st.ok()) return st;
  
  // 启动组提交线程
  if (options_.group_commit_timeout_us > 0) {
    group_commit_thread_.reset(new std::thread(&WalWriter::GroupCommitThread, this));
  }
  
  return Status::OK();
}

Status WalWriter::Close() {
  if (shutdown_.exchange(true)) {
    return Status::OK();  // 已经关闭
  }
  
  // 唤醒组提交线程
  commit_cv_.notify_all();
  
  if (group_commit_thread_) {
    group_commit_thread_->join();
  }
  
  // 处理剩余队列
  ProcessGroupCommit();
  
  // 关闭文件
  if (current_file_) {
    current_file_->Sync();
    delete current_file_;
    current_file_ = nullptr;
  }
  
  return Status::OK();
}

Status WalWriter::Sync() {
  if (!current_file_) {
    return Status::IOError("WalWriter", "not opened");
  }
  
  cedar::Status s = current_file_->Sync();
  if (!s.ok()) {
    return Status::IOError("WalWriter", s.ToString());
  }
  
  stats_.syncs.fetch_add(1, std::memory_order_relaxed);
  return Status::OK();
}

Status WalWriter::WritePut(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version) {
  WalBatch batch;
  batch.Put(key, descriptor, txn_version);
  return WriteBatch(batch);
}

Status WalWriter::WriteDelete(const CedarKey& key, Timestamp txn_version) {
  WalBatch batch;
  batch.Delete(key, txn_version);
  return WriteBatch(batch);
}

Status WalWriter::WriteBatch(const WalBatch& batch) {
  if (batch.empty()) {
    return Status::OK();
  }
  
  // 如果启用了组提交，加入队列
  if (options_.group_commit_timeout_us > 0) {
    uint64_t seq;
    Status s = WriteBatchAsync(batch, &seq);
    if (!s.ok()) return s;
    return WaitForSequence(seq);
  }
  
  // 直接写入
  return WriteInternal(batch);
}

Status WalWriter::WriteBatchAsync(const WalBatch& batch, uint64_t* sequence) {
  if (!sequence) {
    return Status::InvalidArgument("WalWriter", "sequence is null");
  }
  
  *sequence = next_sequence_.fetch_add(1, std::memory_order_acq_rel);
  
  auto request = std::make_shared<GroupCommitRequest>();
  request->batch = batch;
  request->sequence = *sequence;
  
  {
    std::lock_guard<std::mutex> lock(commit_queue_mutex_);
    commit_queue_.push_back(request);
  }
  
  commit_cv_.notify_one();
  return Status::OK();
}

Status WalWriter::WaitForSequence(uint64_t sequence) {
  // 简单实现: 轮询检查
  // 生产环境可以使用条件变量优化
  while (CurrentSequence() < sequence) {
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }
  return Status::OK();
}

Status WalWriter::WriteCommit(uint64_t txn_id, Timestamp txn_version) {
  WalBatch batch;
  // 使用特殊键存储 txn_id
  CedarKey key;
  // 通过构造空键然后编码 txn_id (简化处理)
  batch.Put(key, Descriptor::InlineInt(0, static_cast<int32_t>(txn_id)), txn_version);
  return WriteBatch(batch);
}

Status WalWriter::WriteAbort(uint64_t txn_id, Timestamp txn_version) {
  WalBatch batch;
  CedarKey key;
  batch.Put(key, Descriptor::Tombstone(0), txn_version);
  return WriteBatch(batch);
}

Status WalWriter::WriteInternal(const WalBatch& batch) {
  if (!current_file_) {
    return Status::IOError("WalWriter", "not opened");
  }
  
  // 编码 batch
  std::string data;
  batch.EncodeTo(&data);
  
  // 构建记录头
  WalRecordHeader header;
  header.crc32 = cedar::crc32c::Value(data.data(), data.size());
  header.type = static_cast<uint16_t>(WalRecordType::kBatch);
  header.flags = 0;
  header.data_length = static_cast<uint32_t>(data.size());
  header.sequence = static_cast<uint32_t>(next_sequence_.fetch_add(1, std::memory_order_acq_rel));
  
  // 编码头部
  std::string header_str;
  header.EncodeTo(&header_str);
  
  // 检查文件大小
  size_t record_size = header_str.size() + data.size();
  if (current_file_size_ + record_size > options_.max_file_size) {
    Status s = SwitchWALFile();
    if (!s.ok()) return s;
  }
  
  // 写入文件
  cedar::Slice header_slice(header_str);
  cedar::Slice data_slice(data);
  
  cedar::Status s = current_file_->Append(header_slice);
  if (!s.ok()) return Status::IOError("WalWriter", s.ToString());
  
  s = current_file_->Append(data_slice);
  if (!s.ok()) return Status::IOError("WalWriter", s.ToString());
  
  // 更新统计
  current_file_size_ += record_size;
  stats_.bytes_written.fetch_add(record_size, std::memory_order_relaxed);
  stats_.records_written.fetch_add(1, std::memory_order_relaxed);
  
  return Status::OK();
}

Status WalWriter::SwitchWALFile() {
  // 同步并关闭当前文件
  if (current_file_) {
    current_file_->Sync();
    delete current_file_;
    current_file_ = nullptr;
  }
  
  // 创建新文件
  std::string file_path = wal_dir_ + "/" + std::to_string(file_number_) + ".wal";
  cedar::Status s = env_->NewWritableFile(file_path, &current_file_);
  if (!s.ok()) {
    return Status::IOError("WalWriter", s.ToString());
  }
  
  // 预分配空间 (如果底层文件系统支持)
  // Note: SetPreallocationBlockSize 可能在某些 leveldb 版本中不可用
  // 这里我们跳过显式预分配，依赖文件系统的自动扩展
  
  current_file_path_ = file_path;
  current_file_size_ = 0;
  file_number_++;
  
  return Status::OK();
}

void WalWriter::GroupCommitThread() {
  while (!shutdown_.load()) {
    std::unique_lock<std::mutex> lock(commit_queue_mutex_);
    
    // 等待直到有请求或超时
    auto timeout = std::chrono::microseconds(options_.group_commit_timeout_us);
    commit_cv_.wait_for(lock, timeout, [this] {
      return !commit_queue_.empty() || shutdown_.load();
    });
    
    if (!commit_queue_.empty()) {
      lock.unlock();
      ProcessGroupCommit();
    }
  }
}

void WalWriter::ProcessGroupCommit() {
  std::deque<std::shared_ptr<GroupCommitRequest>> batch;
  
  {
    std::lock_guard<std::mutex> lock(commit_queue_mutex_);
    batch.swap(commit_queue_);
  }
  
  if (batch.empty()) {
    return;
  }
  
  // 合并批次 (简单实现: 逐个写入)
  // 优化: 可以合并多个小批次为一个大写入
  for (auto& request : batch) {
    Status s = WriteInternal(request->batch);
    request->promise.set_value(s);
    
    if (!s.ok()) {
      // 记录错误，但继续处理其他请求
      fprintf(stderr, "WAL write failed: %s\n", s.ToString().c_str());
    }
  }
  
  // 批量 fsync
  if (!batch.empty()) {
    Sync();
    stats_.batches_committed.fetch_add(batch.size(), std::memory_order_relaxed);
  }
}

// ========== WalReader ==========

WalReader::WalReader(const std::string& wal_file, cedar::Env* env)
    : wal_file_(wal_file),
      env_(env),
      file_(nullptr),
      file_size_(0),
      current_offset_(0) {}

WalReader::~WalReader() {
  if (file_) {
    delete file_;
  }
}

Status WalReader::Open() {
  cedar::Status s = env_->NewRandomAccessFile(wal_file_, &file_);
  if (!s.ok()) {
    return Status::IOError("WalReader", s.ToString());
  }
  
  s = env_->GetFileSize(wal_file_, &file_size_);
  if (!s.ok()) {
    return Status::IOError("WalReader", s.ToString());
  }
  
  return Status::OK();
}

Status WalReader::ReadNextRecord(WalBatch* batch, uint64_t* sequence) {
  if (current_offset_ + WalRecordHeader::kEncodedSize > file_size_) {
    return Status::NotFound("WalReader", "end of file");
  }
  
  // 读取头部
  char header_buf[WalRecordHeader::kEncodedSize];
  cedar::Slice header_slice;
  cedar::Status s = file_->Read(current_offset_, WalRecordHeader::kEncodedSize,
                                   &header_slice, header_buf);
  if (!s.ok()) {
    return Status::IOError("WalReader", s.ToString());
  }
  
  Slice header_input(header_slice.data(), header_slice.size());
  WalRecordHeader header;
  Status st = header.DecodeFrom(&header_input);
  if (!st.ok()) return st;
  
  *sequence = header.sequence;
  
  // 读取数据
  current_offset_ += WalRecordHeader::kEncodedSize;
  
  if (current_offset_ + header.data_length > file_size_) {
    return Status::Corruption("WalReader", "truncated record");
  }
  
  std::unique_ptr<char[]> data_buf(new char[header.data_length]);
  cedar::Slice data_slice;
  s = file_->Read(current_offset_, header.data_length, &data_slice, data_buf.get());
  if (!s.ok()) {
    return Status::IOError("WalReader", s.ToString());
  }
  
  // 验证 CRC
  uint32_t actual_crc = cedar::crc32c::Value(data_slice.data(), data_slice.size());
  if (actual_crc != header.crc32) {
    return Status::Corruption("WalReader", "CRC mismatch");
  }
  
  // 解码 batch
  Slice data_input(data_slice.data(), data_slice.size());
  st = batch->DecodeFrom(&data_input);
  if (!st.ok()) return st;
  
  current_offset_ += header.data_length;
  return Status::OK();
}

Status WalReader::ListWALFiles(const std::string& wal_dir,
                                cedar::Env* env,
                                std::vector<std::string>* files) {
  files->clear();
  
  std::vector<std::string> all_files;
  cedar::Status s = env->GetChildren(wal_dir, &all_files);
  if (!s.ok()) {
    return Status::IOError("WalReader", s.ToString());
  }
  
  for (const auto& file : all_files) {
    if (file.size() > 4 && file.substr(file.size() - 4) == ".wal") {
      files->push_back(wal_dir + "/" + file);
    }
  }
  
  std::sort(files->begin(), files->end());
  return Status::OK();
}

}  // namespace cedar
