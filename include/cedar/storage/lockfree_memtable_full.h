#pragma once

// 临时占位文件 - lockfree_memtable_full.h
// 实际功能由 CedarMemTable 提供
// 注意：LockFreeMemTableFull 在 async_index_builder.h 中有前向声明

namespace cedar {
// 前向声明 - 避免与 async_index_builder.h 冲突
class LockFreeMemTableFull;
class CedarMemTable;
} // namespace cedar
