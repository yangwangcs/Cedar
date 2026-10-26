#include "query/projection/projection_page_cache.h"

#include <atomic>
#include <utility>

#include "query/runtime/relational.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/cache.h"

namespace cedar::internal {
namespace {
struct Entry {
  ProjectionChain page;
  size_t charge;
};
struct Pin {
  std::shared_ptr<rocksdb::Cache> cache;
  rocksdb::Cache::Handle* handle;
  std::shared_ptr<QueryReservationLease> query;
  std::shared_ptr<QueryReservationLease> handle_memory;
  std::shared_ptr<const void> generation;
  ~Pin() { cache->Release(handle); }
};
}  // namespace

struct ProjectionPageCache::Impl {
  explicit Impl(uint64_t capacity)
      : cache(capacity ? rocksdb::NewLRUCache(capacity, 0, true) : nullptr) {}
  std::shared_ptr<rocksdb::Cache> cache;
  std::atomic<uint64_t> hits{0}, misses{0};
};

ProjectionPageCache::ProjectionPageCache(uint64_t capacity)
    : impl_(std::make_shared<Impl>(capacity)) {}
ProjectionPageCache::~ProjectionPageCache() = default;

StatusOr<std::shared_ptr<const ProjectionChain>> ProjectionPageCache::Lookup(
    const std::string& key, QueryReservation& query,
    std::shared_ptr<const void> generation) const {
  auto* handle = impl_->cache ? impl_->cache->Lookup(key) : nullptr;
  if (!handle) {
    ++impl_->misses;
    return std::shared_ptr<const ProjectionChain>{};
  }
  auto* entry = static_cast<Entry*>(impl_->cache->Value(handle));
  auto handle_memory = query.TryRetain(sizeof(Pin) + 64);
  auto lease = handle_memory ? query.TryRetainOwner(entry, entry->charge) : nullptr;
  if (!lease || !handle_memory) {
    impl_->cache->Release(handle);
    return Status::ResourceExhausted("projection cache", "page pin exceeds query budget");
  }
  auto pin = std::make_shared<Pin>();
  pin->cache = impl_->cache;
  pin->handle = handle;
  pin->query = std::move(lease);
  pin->handle_memory = std::move(handle_memory);
  pin->generation = std::move(generation);
  ++impl_->hits;
  return std::shared_ptr<const ProjectionChain>(std::move(pin), &entry->page);
}

bool ProjectionPageCache::Admit(const std::string& key, ProjectionChain* page) {
  if (!impl_->cache || !page) return false;
  const size_t charge = ProjectionChainMemoryBytes(*page) + sizeof(Entry) + key.size() + 64;
  if (charge > impl_->cache->GetCapacity()) return false;
  auto entry = std::make_unique<Entry>();
  entry->charge = charge;
  entry->page = std::move(*page);
  static const rocksdb::Cache::CacheItemHelper helper(
      rocksdb::CacheEntryRole::kDataBlock,
      [](rocksdb::Cache::ObjectPtr value, rocksdb::MemoryAllocator*) {
        delete static_cast<Entry*>(value);
      });
  rocksdb::Cache::Handle* handle = nullptr;
  const auto status = impl_->cache->Insert(key, entry.get(), &helper, charge, &handle);
  if (!status.ok()) {
    *page = std::move(entry->page);
    return false;
  }
  entry.release();
  impl_->cache->Release(handle);
  return true;
}

uint64_t ProjectionPageCache::resident_bytes() const {
  return impl_->cache ? impl_->cache->GetUsage() : 0;
}
uint64_t ProjectionPageCache::hits() const { return impl_->hits.load(); }
uint64_t ProjectionPageCache::misses() const { return impl_->misses.load(); }

}  // namespace cedar::internal
