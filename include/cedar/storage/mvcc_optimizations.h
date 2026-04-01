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

// Cedar MVCC 优化方案综合头文件
// 
// 本文件汇总了 Cedar MVCC 的四大优化方案：
// 1. 版本链跳表索引 (VersionChainIndex) - O(k) -> O(log k)
// 2. 分片时间戳分配器 (ShardedTimestampAllocator) - 消除全局原子竞争
// 3. 增量版本编码器 (DeltaVersionEncoder) - 节省 50-90% 存储空间
// 4. 时间范围布隆过滤器 (TemporalBloomFilter) - 减少 90%+ 磁盘 I/O
//
// 使用方式:
//   #include "cedar/storage/mvcc_optimizations.h"

#ifndef FERN_MVCC_OPTIMIZATIONS_H_
#define FERN_MVCC_OPTIMIZATIONS_H_

// 方案一：版本链跳表索引
#include "cedar/storage/version_chain_index.h"

// 方案三：分片时间戳分配器
#include "cedar/transaction/sharded_timestamp_allocator.h"

// 方案四：增量版本编码器
#include "cedar/storage/delta_version_encoder.h"

// 方案六：时间范围布隆过滤器
#include "cedar/storage/temporal_bloom_filter.h"

#endif  // FERN_MVCC_OPTIMIZATIONS_H_
