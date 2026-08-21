// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/database.h"

#include <memory>
#include <utility>
#include <vector>

#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "kernel/database_impl.h"
#include "query/runtime/query_runtime.h"

namespace cedar {

class PreparedQuery::State {
 public:
  State(std::weak_ptr<Database::Impl> database,
        internal::PreparedQueryPlan plan,
        std::vector<PropertyDefinition> schema_fingerprint)
      : database(std::move(database)),
        plan(std::move(plan)),
        schema_fingerprint(std::move(schema_fingerprint)) {}

  std::weak_ptr<Database::Impl> database;
  internal::PreparedQueryPlan plan;
  std::vector<PropertyDefinition> schema_fingerprint;
};

StatusOr<PreparedQuery> Database::PrepareQuery(const Query& query) const {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  auto plan = internal::AnalyzeQuery(query);
  if (!plan.ok()) return plan.status();

  std::vector<PropertyDefinition> fingerprint;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->closed) {
      return Status::InvalidArgument("database", "database is closed");
    }
    if (impl_->closing) {
      return Status::ShutdownInProgress("database",
                                        "database close is in progress");
    }
    fingerprint.reserve(plan.ValueOrDie().referenced_properties.size());
    for (PropertyId property : plan.ValueOrDie().referenced_properties) {
      const auto definition = impl_->store.LookupProperty(property);
      if (!definition.ok()) return definition.status();
      if (!definition.ValueOrDie().has_value()) {
        return Status::SchemaMismatch(
            "query", "referenced property is not registered");
      }
      fingerprint.push_back(*definition.ValueOrDie());
    }
  }
  return PreparedQuery(std::make_shared<const PreparedQuery::State>(
      impl_, std::move(plan).ConsumeValueOrDie(), std::move(fingerprint)));
}

StatusOr<QueryCursor> PreparedQuery::Execute(
    Snapshot snapshot, const Bindings& bindings,
    const QueryOptions& options) const {
  if (!state_) {
    return Status::InvalidArgument("prepared query", "moved-from query");
  }
  const std::shared_ptr<Database::Impl> database = state_->database.lock();
  if (!database) {
    return Status::ShutdownInProgress("query", "database no longer exists");
  }
  const Status valid = database->ValidatePreparedQuery(
      snapshot.commit_seq(), state_->schema_fingerprint);
  if (!valid.ok()) return valid;
  if (!state_->plan.canonical_vertex_state_at) {
    return Status::NotSupported(
        "query", "only VertexScan + StateAt + Project is executable");
  }
  return internal::QueryRuntime::Execute(
      state_->plan, std::move(snapshot), bindings, options);
}

}  // namespace cedar
