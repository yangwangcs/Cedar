// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/transaction.h"

#include <algorithm>
#include <cassert>
#include <map>
#include <limits>
#include <optional>
#include <set>
#include <type_traits>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cedar/database.h"
#include "cedar/snapshot.h"
#include "cedar/fact/canonical_reader.h"
#include "cedar/fact/fact_scan.h"
#include "kernel/database_impl.h"
#include "kernel/epoch_completion.h"
#include "kernel/temporal_validation.h"
#include "kernel/transaction_mutation.h"
#include "query/temporal/state_reader.h"
#include "storage/facts/read_memory_scope.h"

namespace cedar {

namespace {

// Identifies one staged fact mutation for duplicate rejection without the
// O(n) scan an ordered vector key list would require.
struct MutationKey {
  uint32_t part_id = 0;
  uint8_t family = 0;
  uint16_t property_id = 0;
  uint64_t entity_id = 0;
  uint64_t valid_from = 0;

  bool operator==(const MutationKey&) const = default;
};

struct MutationKeyHash {
  size_t operator()(const MutationKey& key) const noexcept {
    uint64_t hash = static_cast<uint64_t>(key.part_id) * 0x9E3779B97F4A7C15ULL;
    hash ^= static_cast<uint64_t>(key.family) << 56;
    hash ^= static_cast<uint64_t>(key.property_id) << 40;
    hash = (hash ^ (hash >> 29)) * 0xBF58476D1CE4E5B9ULL;
    hash ^= key.entity_id * 0x94D049BB133111EBULL;
    hash = (hash ^ (hash >> 31)) * 0xD6E8FEB86659FD93ULL;
    hash ^= key.valid_from + 0x9E3779B97F4A7C15ULL + (hash << 6) + (hash >> 2);
    return static_cast<size_t>(hash ^ (hash >> 32));
  }
};

bool MatchesOverlaySpec(const FactReadSpec& spec, const FactEvent& event) {
  return spec.part_scope.Contains(event.ref.part_id()) &&
         spec.family == event.ref.family() &&
         spec.property_id == event.ref.property_id() &&
         (!spec.entity_range.min.has_value() ||
          event.ref.entity_id() >= *spec.entity_range.min) &&
         (!spec.entity_range.max_exclusive.has_value() ||
          event.ref.entity_id() < *spec.entity_range.max_exclusive) &&
         (!spec.valid_from_min.has_value() ||
          event.valid_from.value >= spec.valid_from_min->value) &&
         (!spec.valid_from_max.has_value() ||
          event.valid_from.value <= spec.valid_from_max->value) &&
         (!spec.commit_seq_min.has_value() ||
          event.commit_seq.value >= spec.commit_seq_min->value) &&
         (!spec.commit_seq_max.has_value() ||
          event.commit_seq.value <= spec.commit_seq_max->value);
}

const FactColumn* FindOverlayColumn(const FactColumnarBatch& batch,
                                    FactColumnId id) {
  for (const FactColumn& column : batch.columns) {
    if (column.id == id) return &column;
  }
  return nullptr;
}

Status MakeOverlayColumn(FactColumnId id, FactColumn* column) {
  column->id = id;
  switch (id) {
    case FactColumnId::kPartId:
    case FactColumnId::kFactFamily:
    case FactColumnId::kPropertyId:
    case FactColumnId::kOperation:
    case FactColumnId::kSchemaEpoch:
    case FactColumnId::kPhysicalType:
    case FactColumnId::kSourcePartId:
    case FactColumnId::kTargetPartId:
      column->values = std::vector<uint32_t>{}; return Status::OK();
    case FactColumnId::kEntityId:
    case FactColumnId::kValidFrom:
    case FactColumnId::kCedarCommitSeq:
    case FactColumnId::kStorageSequence:
    case FactColumnId::kTimestamp64Value:
    case FactColumnId::kSourceVertexId:
    case FactColumnId::kTargetVertexId:
    case FactColumnId::kEdgeType:
      column->values = std::vector<uint64_t>{}; return Status::OK();
    case FactColumnId::kBoolValue: column->values = std::vector<uint8_t>{}; return Status::OK();
    case FactColumnId::kInt32Value: column->values = std::vector<int32_t>{}; return Status::OK();
    case FactColumnId::kInt64Value: column->values = std::vector<int64_t>{}; return Status::OK();
    case FactColumnId::kFloat32Value: column->values = std::vector<float>{}; return Status::OK();
    case FactColumnId::kFloat64Value: column->values = std::vector<double>{}; return Status::OK();
    case FactColumnId::kBytesValue: column->values = std::vector<std::string>{}; return Status::OK();
  }
  return Status::InvalidArgument("transaction overlay", "unknown fact column");
}

template <typename T>
Status AppendOverlayValue(FactColumn* column, std::optional<T> value) {
  auto* values = std::get_if<std::vector<T>>(&column->values);
  if (values == nullptr) return Status::Corruption("transaction overlay", "column type mismatch");
  values->push_back(value.value_or(T{}));
  column->present.push_back(value.has_value() ? 1 : 0);
  return Status::OK();
}

Status AppendOverlayEvent(FactColumn* column, const FactEvent& event) {
  const auto& value = event.value;
  const auto& identity = event.edge_identity;
  switch (column->id) {
    case FactColumnId::kPartId: return AppendOverlayValue<uint32_t>(column, event.ref.part_id().value);
    case FactColumnId::kFactFamily: return AppendOverlayValue<uint32_t>(column, static_cast<uint32_t>(event.ref.family()));
    case FactColumnId::kPropertyId: return AppendOverlayValue<uint32_t>(column, static_cast<uint32_t>(event.ref.property_id().value));
    case FactColumnId::kEntityId: return AppendOverlayValue<uint64_t>(column, event.ref.entity_id());
    case FactColumnId::kValidFrom: return AppendOverlayValue<uint64_t>(column, event.valid_from.value);
    case FactColumnId::kCedarCommitSeq: return AppendOverlayValue<uint64_t>(column, event.commit_seq.value);
    case FactColumnId::kOperation: return AppendOverlayValue<uint32_t>(column, static_cast<uint32_t>(event.operation));
    case FactColumnId::kSchemaEpoch: return AppendOverlayValue<uint32_t>(column, event.schema_epoch);
    case FactColumnId::kPhysicalType: return AppendOverlayValue<uint32_t>(column, static_cast<uint32_t>(value ? value->type() : PhysicalType{}));
    case FactColumnId::kBoolValue: return AppendOverlayValue<uint8_t>(column, value && value->type() == PhysicalType::kBool ? std::optional<uint8_t>{static_cast<uint8_t>(std::get<bool>(value->data()))} : std::nullopt);
    case FactColumnId::kInt32Value: return AppendOverlayValue<int32_t>(column, value && value->type() == PhysicalType::kInt32 ? std::optional<int32_t>{std::get<int32_t>(value->data())} : std::nullopt);
    case FactColumnId::kInt64Value: return AppendOverlayValue<int64_t>(column, value && value->type() == PhysicalType::kInt64 ? std::optional<int64_t>{std::get<int64_t>(value->data())} : std::nullopt);
    case FactColumnId::kFloat32Value: return AppendOverlayValue<float>(column, value && value->type() == PhysicalType::kFloat32 ? std::optional<float>{std::get<float>(value->data())} : std::nullopt);
    case FactColumnId::kFloat64Value: return AppendOverlayValue<double>(column, value && value->type() == PhysicalType::kFloat64 ? std::optional<double>{std::get<double>(value->data())} : std::nullopt);
    case FactColumnId::kTimestamp64Value: return AppendOverlayValue<uint64_t>(column, value && value->type() == PhysicalType::kTimestamp64 ? std::optional<uint64_t>{std::get<uint64_t>(value->data())} : std::nullopt);
    case FactColumnId::kBytesValue: return AppendOverlayValue<std::string>(column, value && (value->type() == PhysicalType::kString || value->type() == PhysicalType::kBinary) ? std::optional<std::string>{std::get<std::string>(value->data())} : std::nullopt);
    case FactColumnId::kSourcePartId: return AppendOverlayValue<uint32_t>(column, identity ? std::optional<uint32_t>{identity->source_part_id.value} : std::nullopt);
    case FactColumnId::kSourceVertexId: return AppendOverlayValue<uint64_t>(column, identity ? std::optional<uint64_t>{identity->source_vertex_id.value} : std::nullopt);
    case FactColumnId::kTargetPartId: return AppendOverlayValue<uint32_t>(column, identity ? std::optional<uint32_t>{identity->target_part_id.value} : std::nullopt);
    case FactColumnId::kTargetVertexId: return AppendOverlayValue<uint64_t>(column, identity ? std::optional<uint64_t>{identity->target_vertex_id.value} : std::nullopt);
    case FactColumnId::kEdgeType: return AppendOverlayValue<uint64_t>(column, identity ? std::optional<uint64_t>{identity->edge_type} : std::nullopt);
    case FactColumnId::kStorageSequence: return Status::NotSupported("transaction overlay", "storage sequence is not durable in overlay");
  }
  return Status::InvalidArgument("transaction overlay", "unknown fact column");
}

class TransactionOverlayReader final : public CanonicalFactReader {
 public:
  TransactionOverlayReader(Snapshot base, std::vector<PendingFactMutation> mutations,
                            std::vector<EdgeIdentity> identities, bool include_staged,
                            CommitSeq overlay_seq)
      : base_(std::move(base)), mutations_(std::move(mutations)),
        identities_(std::move(identities)), include_staged_(include_staged),
        overlay_seq_(overlay_seq) {}

  StatusOr<std::optional<FactEvent>> ReadStateAt(
      const FactReadSpec& spec, ValidTime time, CommitSeq snapshot_seq) const override {
    const CommitSeq durable_seq =
        snapshot_seq.value > base_.commit_seq().value ? base_.commit_seq()
                                                       : snapshot_seq;
    internal::FactReadMemoryScope result_memory(spec.read_memory.get());
    auto current = base_.canonical_reader().ReadStateAt(spec, time, durable_seq);
    if (!current.ok() || !include_staged_) return current;
    std::optional<FactEvent> winner = std::move(current).ConsumeValueOrDie();
    std::optional<FactEvent> staged_winner;
    const std::optional<Value>* payload = nullptr;
    const Status selected = VisitStaged(spec, [&](const FactEvent& event, const std::optional<Value>& value) {
      if (event.valid_from.value > time.value) return Status::OK();
      const auto& candidate = staged_winner ? staged_winner : winner;
      if (!candidate || event.valid_from.value > candidate->valid_from.value ||
          (event.valid_from == candidate->valid_from && event.commit_seq.value >= candidate->commit_seq.value)) {
        staged_winner = event;
        payload = value ? &value : nullptr;
      }
      return Status::OK();
    });
    if (!selected.ok()) return selected;
    if (!staged_winner) return winner;
    Status reserved = result_memory.Reserve(sizeof(FactEvent));
    if (!reserved.ok()) return reserved;
    reserved = result_memory.Reserve(payload ? internal::FactValueHeapBytes(*payload) : 0);
    if (!reserved.ok()) return reserved;
    if (payload) staged_winner->value = *payload;
    auto owner = result_memory.Retain(spec.read_memory);
    if (!owner.ok()) return owner.status();
    staged_winner->read_memory_owner = std::move(owner).ConsumeValueOrDie();
    winner = std::move(staged_winner);
    return winner;
  }

  Status ReadStateRows(const CanonicalStateReadSpec& spec,
                       const CanonicalStateBatchVisitor& visitor) const override {
    if (!visitor) return Status::InvalidArgument("transaction overlay", "missing visitor");
    if (!include_staged_) return base_.canonical_reader().ReadStateRows(spec, visitor);
    if (spec.max_rows.has_value() && *spec.max_rows == 0) return Status::OK();
    internal::StateRowStream stream(spec, visitor);
    FactReadSpec events = spec.facts;
    events.max_rows.reset();
    Status status = ReadEvents(events, [&stream](const FactEventBatch& batch) {
      return stream.Consume(batch);
    });
    if (status.IsQueryCancelled() && stream.limit_reached()) return Status::OK();
    if (!status.ok()) return status;
    return stream.Finish();
  }

  Status ReadEvents(const FactReadSpec& spec,
                    const CanonicalFactBatchVisitor& visitor) const override {
    if (!visitor) return Status::InvalidArgument("transaction overlay", "missing visitor");
    const Status valid = spec.Validate();
    if (!valid.ok()) return valid;
    if (spec.max_rows && *spec.max_rows == 0) return Status::OK();
    if (!include_staged_) return base_.canonical_reader().ReadEvents(spec, visitor);
    internal::FactReadMemoryScope event_memory(spec.read_memory.get());
    std::vector<FactEvent> events;
    auto append = [&](const FactEvent& event, const std::optional<Value>& value) {
      Status status = internal::ReserveFactAppend(&events, &event_memory);
      if (!status.ok()) return status;
      status = event_memory.Reserve(internal::FactValueHeapBytes(value));
      if (!status.ok()) return status;
      events.push_back(FactEvent{event.ref, event.valid_from, event.commit_seq,
          event.operation, event.schema_epoch, value, event.edge_identity});
      return Status::OK();
    };
    Status status = base_.canonical_reader().ReadEvents(spec, [&](const FactEventBatch& batch) {
      for (const auto& event : batch.events) {
        const Status copied = append(event, event.value);
        if (!copied.ok()) return copied;
      }
      return Status::OK();
    });
    if (!status.ok()) return status;
    status = VisitStaged(spec, append);
    if (!status.ok()) return status;
    // stable_sort may allocate a second row buffer while the input stays live.
    internal::FactReadMemoryScope sort_memory(spec.read_memory.get());
    status = sort_memory.ReserveElements(events.size(), sizeof(FactEvent));
    if (!status.ok()) return status;
    std::stable_sort(events.begin(), events.end(), [](const FactEvent& left, const FactEvent& right) {
      if (left.ref != right.ref) {
        return std::tuple{left.ref.part_id().value,
                          static_cast<uint8_t>(left.ref.family()),
                          left.ref.property_id().value, left.ref.entity_id()} <
               std::tuple{right.ref.part_id().value,
                          static_cast<uint8_t>(right.ref.family()),
                          right.ref.property_id().value, right.ref.entity_id()};
      }
      if (left.valid_from != right.valid_from) return left.valid_from.value < right.valid_from.value;
      return left.commit_seq.value < right.commit_seq.value;
    });
    sort_memory.Reset();
    const size_t output_size = spec.max_rows
        ? std::min<uint64_t>(events.size(), *spec.max_rows) : events.size();
    size_t offset = 0;
    while (offset < output_size) {
      const size_t count = std::min<size_t>(spec.batch_row_limit, output_size - offset);
      internal::FactReadMemoryScope batch_memory(spec.read_memory.get());
      status = batch_memory.ReserveElements(count, sizeof(FactEvent));
      if (!status.ok()) return status;
      for (size_t i = offset; i < offset + count; ++i) {
        status = batch_memory.Reserve(internal::FactValueHeapBytes(events[i].value));
        if (!status.ok()) return status;
      }
      FactEventBatch batch{std::vector<FactEvent>(events.begin() + offset, events.begin() + offset + count)};
      status = visitor(batch);
      if (!status.ok()) return status;
      offset += count;
    }
    return Status::OK();
  }

  Status ReadColumnar(const FactReadSpec& spec,
                      const CanonicalColumnarBatchVisitor& visitor) const override {
    if (spec.projection.empty()) return Status::InvalidArgument("transaction overlay", "missing projection");
    if (!visitor) return Status::InvalidArgument("transaction overlay", "missing visitor");
    return ReadEvents(spec, [&](const FactEventBatch& events) {
      internal::FactReadMemoryScope column_memory(spec.read_memory.get());
      Status status = column_memory.ReserveElements(spec.projection.size(), sizeof(FactColumn));
      if (!status.ok()) return status;
      FactColumnarBatch batch;
      batch.columns.reserve(spec.projection.size());
      for (FactColumnId id : spec.projection) {
        FactColumn column;
        status = MakeOverlayColumn(id, &column);
        if (!status.ok()) return status;
        status = std::visit([&](auto& values) {
          using Element = typename std::decay_t<decltype(values)>::value_type;
          Status reserved = column_memory.ReserveElements(
              events.events.size(), sizeof(Element) + sizeof(uint8_t));
          if (!reserved.ok()) return reserved;
          values.reserve(events.events.size());
          return Status::OK();
        }, column.values);
        if (!status.ok()) return status;
        column.present.reserve(events.events.size());
        batch.columns.push_back(std::move(column));
      }
      for (const auto& event : events.events) {
        for (auto& column : batch.columns) {
          if (column.id == FactColumnId::kBytesValue) {
            // The optional argument and value_or copy overlap during append.
            status = column_memory.ReserveElements(internal::FactValueHeapBytes(event.value), 2);
            if (!status.ok()) return status;
          }
          status = AppendOverlayEvent(&column, event);
          if (!status.ok()) return status;
        }
      }
      return visitor(batch);
    });
  }

  StatusOr<FactEventBatch> ReadExact(
      const std::vector<std::string>& keys,
      std::shared_ptr<FactReadMemory> read_memory = {}) const override {
    return base_.canonical_reader().ReadExact(keys, std::move(read_memory));
  }

 private:
  template <typename Visitor>
  Status VisitStaged(const FactReadSpec& spec, const Visitor& visitor) const {
    if (!include_staged_) return Status::OK();
    for (const PendingFactMutation& mutation : mutations_) {
      FactEvent event{mutation.ref, mutation.valid_from, overlay_seq_, mutation.operation,
                      mutation.schema_epoch, std::nullopt, std::nullopt};
      if (MatchesOverlaySpec(spec, event)) {
        const Status status = visitor(event, mutation.value);
        if (!status.ok()) return status;
      }
    }
    if (spec.family == FactFamily::kEdgeIdentity) {
      for (const EdgeIdentity& identity : identities_) {
        FactEvent event{FactRef{identity.home_part_id, FactFamily::kEdgeIdentity,
                                PropertyId{}, identity.edge_id.value},
                        ValidTime{0}, overlay_seq_, FactOperation::kPut,
                        0, std::nullopt, identity};
        if (MatchesOverlaySpec(spec, event)) {
          const Status status = visitor(event, event.value);
          if (!status.ok()) return status;
        }
      }
    }
    return Status::OK();
  }

  Snapshot base_;
  std::vector<PendingFactMutation> mutations_;
  std::vector<EdgeIdentity> identities_;
  bool include_staged_;
  CommitSeq overlay_seq_;
};

}  // namespace

CommitHandle::CommitHandle(std::shared_ptr<State> state) : state_(std::move(state)) {}

TxnId CommitHandle::txn_id() const {
  return state_ == nullptr ? TxnId{} : state_->txn_id;
}

CommitAcceptance CommitHandle::acceptance() const {
  return state_ == nullptr ? CommitAcceptance::kIndeterminate : state_->acceptance;
}

StatusOr<CommitResult> CommitHandle::Wait() const {
  if (state_ == nullptr) return Status::InvalidArgument("async commit", "empty handle");
  std::unique_lock<std::mutex> lock(state_->mutex);
  state_->completed.wait(lock, [this] {
    return state_->result.has_value() || state_->epoch_completion != nullptr;
  });
  const std::shared_ptr<internal::EpochCompletion> completion =
      state_->epoch_completion;
  if (completion == nullptr) return *state_->result;
  // A request bound to an ordered epoch gets its terminal outcome from that
  // epoch, including WAL-less results that never invoked the WAL-durable
  // callback (pre-write error, RecoveryRequired, pre-durability indeterminate).
  // The epoch completion is authoritative even if a shutdown sweep raced the
  // bind and wrote a synthetic ShutdownInProgress into `result`; repeated Wait()
  // calls keep returning the same published epoch result.
  const size_t ordinal = state_->epoch_result_ordinal;
  lock.unlock();
  return completion->WaitForResult(ordinal);
}

class Transaction::State {
 public:
  explicit State(std::shared_ptr<Database::Impl> database,
                 TransactionOptions options, StoreSnapshot snapshot, TxnId txn_id)
      : database(std::move(database)),
        options(options),
        snapshot(std::move(snapshot)),
        txn_id(txn_id) {}

  std::shared_ptr<Database::Impl> database;
  TransactionOptions options;
  std::optional<StoreSnapshot> snapshot;
  TxnId txn_id;
  std::vector<PendingFactMutation> mutations;
  std::vector<EdgeIdentity> edge_identities;
  std::vector<StrictReadDependency> strict_read_dependencies;
  std::unordered_set<MutationKey, MutationKeyHash> mutation_keys;
  // First-use memo only: the store's live schema catalog can be refreshed in
  // place by a concurrent RegisterProperty (property registration does not
  // advance the visible sequence), so a pinned snapshot does not freeze schemas.
  // Caching here deliberately keeps this transaction's first resolution for the
  // rest of its writes, which preserves per-transaction schema consistency while
  // a multi-fact write avoids re-resolving the same property per fact.
  std::unordered_map<uint16_t, PropertyDefinition> property_definitions;
  bool terminal = false;

  Status CheckDatabaseOpen() const {
    // Lock-free: `closed`/`closing` are already atomics, and staging does not
    // touch any state that Close releases - the transaction's own StoreSnapshot
    // keeps its epoch (and therefore the schema catalog it resolves against)
    // alive, so Close reports SnapshotPinned instead of tearing that down.
    // Ordering matches the previous locked check: closed wins over closing.
    if (database->closed.load(std::memory_order_acquire)) {
      return Status::InvalidArgument("transaction", "database is closed");
    }
    if (database->closing.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress("transaction", "database close is in progress");
    }
    return Status::OK();
  }

  Status BeginCommit() {
    // Lock-free registration with the Close drain. The seq_cst increment happens
    // before the closing re-check while Close publishes `closing` (seq_cst)
    // before it observes the counter, so a race resolves one of two ways: Close
    // sees this call and waits for EndCommit, or this call observes the close
    // transition and withdraws without touching the store. All `closing`/`closed`
    // flips are seq_cst stores for exactly this reason: a release store would not
    // join the seq_cst total order, so this recheck could miss the transition
    // while the drain load misses the registration. The re-check reads `closing`
    // before `closed` so the successful-close tail (`closed=true;
    // closing=false`) is still caught by the `closed` load.
    if (database->closed.load(std::memory_order_seq_cst)) {
      return Status::InvalidArgument("transaction", "database is closed");
    }
    database->active_commit_calls.fetch_add(1, std::memory_order_seq_cst);
    if (database->transaction_commit_registration_observer_for_testing) {
      database->transaction_commit_registration_observer_for_testing();
    }
    if (database->closing.load(std::memory_order_seq_cst)) {
      EndCommit();
      return Status::ShutdownInProgress("transaction", "database close is in progress");
    }
    if (database->closed.load(std::memory_order_seq_cst)) {
      EndCommit();
      return Status::InvalidArgument("transaction", "database is closed");
    }
    if (database->transaction_commit_admitted_observer_for_testing) {
      database->transaction_commit_admitted_observer_for_testing();
    }
    return Status::OK();
  }

  void EndCommit() {
    const size_t previous =
        database->active_commit_calls.fetch_sub(1, std::memory_order_seq_cst);
    // Only the transition to zero can release a Close drain waiter. A waiter
    // that observed a nonzero value is guaranteed to wake here: every nonzero
    // count eventually reaches this decrement, and atomic::wait re-checks the
    // value after the notification.
    assert(previous > 0);
    if (previous == 1) {
      database->active_commit_calls.notify_all();
    }
  }

  StatusOr<PropertyDefinition> ResolveProperty(PropertyFact property) {
    const uint16_t property_id = property.ref().property_id().value;
    PropertyDefinition definition;
    const auto cached = property_definitions.find(property_id);
    if (cached != property_definitions.end()) {
      definition = cached->second;
    } else {
      const auto schema = database->store.LookupProperty(
          *snapshot, property.ref().property_id());
      if (!schema.ok()) return schema.status();
      if (!schema.ValueOrDie().has_value()) {
        return Status::SchemaMismatch("transaction",
                                      "property is not registered");
      }
      definition = *schema.ValueOrDie();
      property_definitions.emplace(property_id, definition);
    }
    const PropertyEntityKind expected_kind =
        property.ref().family() == FactFamily::kVertexProperty
            ? PropertyEntityKind::kVertex
            : PropertyEntityKind::kEdge;
    if (definition.entity_kind != expected_kind) {
      return Status::SchemaMismatch("transaction", "property entity kind differs");
    }
    return definition;
  }

  Status CaptureStrictRead(const FactRef& ref, ValidTime valid_time) {
    if (options.isolation != IsolationLevel::kStrict) return Status::OK();
    const auto duplicate = std::find_if(
        strict_read_dependencies.begin(), strict_read_dependencies.end(),
        [&ref, valid_time](const StrictReadDependency& dependency) {
          return dependency.ref == ref && dependency.valid_time == valid_time;
        });
    if (duplicate != strict_read_dependencies.end()) return Status::OK();
    auto dependency = CaptureStrictReadDependency(
        database->store, *snapshot, ref, valid_time);
    if (!dependency.ok()) return dependency.status();
    strict_read_dependencies.push_back(dependency.ConsumeValueOrDie());
    return Status::OK();
  }

  StatusOr<bool> EdgeVisible(EdgeRef edge, ValidTime valid_time) {
    const FactRef edge_ref = EntityFact::Edge(edge).ref();
    const Status edge_dependency = CaptureStrictRead(edge_ref, valid_time);
    if (!edge_dependency.ok()) return edge_dependency;
    const auto edge_event = database->store.Read(*snapshot, edge_ref, valid_time);
    if (!edge_event.ok()) return edge_event.status();
    if (!edge_event.ValueOrDie().has_value()) return false;
    const auto identity = database->store.LookupEdgeIdentity(*snapshot, edge);
    if (!identity.ok()) return identity.status();
    if (!identity.ValueOrDie().has_value()) return false;

    for (VertexRef endpoint : {identity.ValueOrDie()->source_ref(),
                               identity.ValueOrDie()->target_ref()}) {
      const FactRef endpoint_ref = EntityFact::Vertex(endpoint).ref();
      const Status endpoint_dependency = CaptureStrictRead(endpoint_ref, valid_time);
      if (!endpoint_dependency.ok()) return endpoint_dependency;
      const auto state = database->store.Read(*snapshot, endpoint_ref, valid_time);
      if (!state.ok()) return state.status();
      if (!state.ValueOrDie().has_value()) return false;
    }
    return true;
  }

  Status Stage(PendingFactMutation mutation) {
    const Status valid = mutation.Validate();
    if (!valid.ok()) return valid;
    const MutationKey key{mutation.ref.part_id().value,
                          static_cast<uint8_t>(mutation.ref.family()),
                          mutation.ref.property_id().value,
                          mutation.ref.entity_id(),
                          mutation.valid_from.value};
    if (!mutation_keys.insert(key).second) {
      return Status::InvalidArgument("transaction", "duplicate fact mutation");
    }
    mutations.push_back(std::move(mutation));
    return Status::OK();
  }

  Status StageEdge(EdgeIdentity identity, ValidTime valid_time) {
    const Status valid = identity.Validate();
    if (!valid.ok()) return valid;
    const auto existing = std::find_if(
        edge_identities.begin(), edge_identities.end(),
        [&identity](const EdgeIdentity& candidate) {
          return candidate.edge_ref() == identity.edge_ref();
        });
    if (existing != edge_identities.end() && *existing != identity) {
      return Status::IdentityConflict("transaction", "edge ID has a different identity");
    }
    const Status staged = Stage(PendingFactMutation{
        EntityFact::Edge(identity.edge_ref()).ref(), valid_time, FactOperation::kPut,
        0, std::nullopt});
    if (!staged.ok()) return staged;
    if (existing == edge_identities.end()) edge_identities.push_back(identity);
    return Status::OK();
  }

  void Finish() {
    terminal = true;
    snapshot.reset();
  }
};

Transaction::Transaction(std::unique_ptr<State> state) : state_(std::move(state)) {}
Transaction::~Transaction() = default;
Transaction::Transaction(Transaction&&) noexcept = default;
Transaction& Transaction::operator=(Transaction&&) noexcept = default;

StatusOr<bool> Transaction::Exists(EntityFact entity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  if (entity.ref().family() == FactFamily::kEdgeState) {
    return state_->EdgeVisible(
        EdgeRef{entity.ref().part_id(), EdgeId{entity.ref().entity_id()}}, valid_time);
  }
  const Status dependency = state_->CaptureStrictRead(entity.ref(), valid_time);
  if (!dependency.ok()) return dependency;
  const auto event = state_->database->store.Read(
      *state_->snapshot, entity.ref(), valid_time);
  if (!event.ok()) return event.status();
  return event.ValueOrDie().has_value();
}

StatusOr<std::optional<Value>> Transaction::Get(PropertyFact property,
                                                ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  const Status dependency = state_->CaptureStrictRead(property.ref(), valid_time);
  if (!dependency.ok()) return dependency;
  const auto event = state_->database->store.Read(
      *state_->snapshot, property.ref(), valid_time);
  if (!event.ok()) return event.status();
  if (!event.ValueOrDie().has_value()) return std::optional<Value>{};
  if (property.ref().family() == FactFamily::kEdgeProperty) {
    const auto visible = state_->EdgeVisible(
        EdgeRef{property.ref().part_id(), EdgeId{property.ref().entity_id()}}, valid_time);
    if (!visible.ok()) return visible.status();
    if (!visible.ValueOrDie()) return std::optional<Value>{};
  }
  return event.ValueOrDie()->value;
}

Status Transaction::Scan(FactFamily family, PropertyId property_id,
                         const TransactionFactVisitor& visitor) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  if (state_->options.isolation == IsolationLevel::kStrict) {
    return Status::UnsupportedSerializablePredicate(
        "transaction", "strict transactions support only exact reads");
  }
  return state_->database->store.Scan(*state_->snapshot,
                                      FactPrefix::Family(PartId{}, family, property_id),
                                      visitor);
}

StatusOr<Snapshot> Transaction::BeginReadSnapshot(bool include_staged) const {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal || !state_->snapshot.has_value()) {
    return Status::InvalidArgument("transaction", "terminal transaction");
  }
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  const CommitSeq base_seq = state_->snapshot->commit_seq();
  Database database(state_->database, false);
  auto reader_snapshot = database.BeginSnapshot(SnapshotOptions{base_seq});
  if (!reader_snapshot.ok()) return reader_snapshot.status();
  auto output_snapshot = database.BeginSnapshot(SnapshotOptions{base_seq});
  if (!output_snapshot.ok()) return output_snapshot.status();
  const CommitSeq overlay_seq =
      base_seq.value == std::numeric_limits<uint64_t>::max()
          ? base_seq
          : CommitSeq{std::max<uint64_t>(1, base_seq.value + 1)};
  auto reader = std::make_shared<TransactionOverlayReader>(
      std::move(reader_snapshot).ConsumeValueOrDie(), state_->mutations,
      state_->edge_identities, include_staged, overlay_seq);
  return Snapshot::WithCanonicalReader(
      std::move(output_snapshot).ConsumeValueOrDie(), std::move(reader),
      include_staged ? std::optional<CommitSeq>{overlay_seq} : std::nullopt);
}

Status Transaction::Assert(EntityFact entity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return state_->Stage(PendingFactMutation{entity.ref(), valid_time,
                                            FactOperation::kPut, 0,
                                            std::nullopt});
}

Status Transaction::Assert(EdgeIdentity identity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return state_->StageEdge(std::move(identity), valid_time);
}

Status Transaction::Retract(EntityFact entity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return state_->Stage(PendingFactMutation{entity.ref(), valid_time,
                                            FactOperation::kDelete, 0,
                                            std::nullopt});
}

Status Transaction::Set(PropertyFact property, ValidTime valid_time, Value value) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  const auto schema = state_->ResolveProperty(property);
  if (!schema.ok()) return schema.status();
  if (schema.ValueOrDie().physical_type != value.type()) {
    return Status::SchemaMismatch("transaction", "property value type differs");
  }
  return state_->Stage(PendingFactMutation{property.ref(), valid_time,
                                            FactOperation::kPut,
                                            schema.ValueOrDie().schema_epoch,
                                            std::move(value)});
}

Status Transaction::Unset(PropertyFact property, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  const auto schema = state_->ResolveProperty(property);
  if (!schema.ok()) return schema.status();
  return state_->Stage(PendingFactMutation{property.ref(), valid_time,
                                            FactOperation::kDelete,
                                            schema.ValueOrDie().schema_epoch,
                                            std::nullopt});
}

StatusOr<CommitResult> Transaction::Commit() {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status begun = state_->BeginCommit();
  if (!begun.ok()) return begun;
  // Staging appends in staging order; commit establishes the canonical fact
  // order once instead of shifting the vector on every assertion.
  std::stable_sort(state_->mutations.begin(), state_->mutations.end(),
                   CanonicalMutationLess);
  // Dependencies are derived from the transaction's pinned snapshot at commit
  // time. One batched pass replaces one iterator pair per staged fact.
  auto dependencies = DeriveSnapshotWriteDependencies(
      state_->database->store, *state_->snapshot, state_->mutations);
  if (!dependencies.ok()) {
    const Status status = dependencies.status();
    state_->Finish();
    state_->EndCommit();
    return CommitResult{CommitOutcome::kAborted, CommitSeq{}, state_->txn_id,
                        status};
  }
  StoreCommitBatch batch{state_->txn_id,
                         state_->txn_id.value,
                         std::move(state_->mutations),
                         std::move(state_->edge_identities),
                         dependencies.ConsumeValueOrDie(),
                         std::move(state_->strict_read_dependencies), true};
  const auto committed = state_->database->SubmitSyncCommit(
      std::move(batch), state_->options.commit_deadline_us);
  state_->Finish();
  state_->EndCommit();
  if (!committed.ok()) {
    const Status status = committed.status();
    return CommitResult{status.IsIndeterminate() || status.IsRecoveryRequired()
                            ? CommitOutcome::kIndeterminate
                            : CommitOutcome::kAborted,
                        CommitSeq{}, state_->txn_id, status};
  }
  return CommitResult{CommitOutcome::kCommitted, committed.ValueOrDie().commit_seq,
                      state_->txn_id, Status::OK()};
}

StatusOr<CommitHandle> Transaction::CommitAsync() {
  return CommitAsyncImpl(/*pipelined=*/false);
}

StatusOr<CommitHandle> Transaction::CommitAsyncPipelined() {
  return CommitAsyncImpl(/*pipelined=*/true);
}

StatusOr<CommitHandle> Transaction::CommitAsyncImpl(bool pipelined) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status begun = state_->BeginCommit();
  if (!begun.ok()) return begun;
  std::stable_sort(state_->mutations.begin(), state_->mutations.end(),
                   CanonicalMutationLess);
  auto dependencies = DeriveSnapshotWriteDependencies(
      state_->database->store, *state_->snapshot, state_->mutations);
  if (!dependencies.ok()) {
    const Status status = dependencies.status();
    state_->Finish();
    state_->EndCommit();
    return status;
  }
  StoreCommitBatch batch{state_->txn_id,
                         state_->txn_id.value,
                         std::move(state_->mutations),
                         std::move(state_->edge_identities),
                         dependencies.ConsumeValueOrDie(),
                         std::move(state_->strict_read_dependencies), true};
  auto handle_state = std::make_shared<CommitHandle::State>(
      batch.txn_id, pipelined ? CommitAcceptance::kEnqueued
                              : CommitAcceptance::kAccepted);
  const Status enqueued = state_->database->SubmitAsyncCommit(
      std::move(batch), handle_state, state_->options.commit_deadline_us,
      pipelined);
  state_->Finish();
  state_->EndCommit();
  if (!enqueued.ok()) return enqueued;
  return CommitHandle(std::move(handle_state));
}

Status Transaction::Rollback() {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  state_->Finish();
  return Status::OK();
}

StatusOr<std::unique_ptr<Transaction>> Database::BeginTransaction(
    TransactionOptions options) {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  if (impl_->closed.load(std::memory_order_acquire)) {
    return Status::InvalidArgument("database", "database is closed");
  }
  if (impl_->closing.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  auto txn_id = impl_->store.AllocateTransactionId();
  if (!txn_id.ok()) return txn_id.status();
  // A transaction with no as_of is a current-vintage pin, so use the lock-free
  // current-epoch path directly (the same lock-free pin the point-read path
  // uses, without lifecycle_mutex_/epoch_mutex). It is not necessarily the exact
  // snapshot BeginSnapshot() would take: during the PublishVisible->retire window
  // it can pin the still-current pre-publication epoch, which linearizes this
  // begin before that publication (linearizable begin, no isolation
  // regression). The fast path falls back to the registered BeginSnapshot path
  // internally when the store is closing or the current epoch has not been
  // created yet, so the close handshake and error statuses are unchanged.
  auto snapshot = impl_->store.BeginCurrentSnapshot();
  if (!snapshot.ok()) return snapshot.status();
  return std::unique_ptr<Transaction>(
      new Transaction(std::make_unique<Transaction::State>(
          impl_, options, snapshot.ConsumeValueOrDie(),
          txn_id.ConsumeValueOrDie())));
}

}  // namespace cedar
