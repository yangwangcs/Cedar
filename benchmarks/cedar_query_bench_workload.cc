#include "benchmarks/cedar_query_bench_workload.h"
#include <chrono>
#include <filesystem>
#include <sstream>
#include "cedar/database.h"
namespace cedar::benchmark {
namespace { Status WriteFact(Database* db, uint64_t id) { auto txn=db->BeginTransaction(); if(!txn.ok()) return txn.status(); auto s=txn.ValueOrDie()->Assert(EntityFact::Vertex({PartId{1},VertexId{id}}),ValidTime{1}); if(!s.ok()) return s; auto c=txn.ValueOrDie()->Commit(); return c.ok()?Status::OK():c.status(); } }
StatusOr<QueryBenchmarkResult> RunQueryBenchmark(const QueryBenchmarkOptions& o) {
  std::filesystem::remove_all(o.path); DatabaseOptions d; d.path=o.path; d.storage_profile=StorageProfile::kProductionAppend; d.production.memory_budget_bytes=1ULL<<30; d.production.kernel_mode=true; d.query_runtime.query_memory_bytes=32ULL<<20; d.query_runtime.projection_cache_bytes=32ULL<<20; d.query_runtime.query_delta_bytes=32ULL<<20;
  auto opened=Database::Open(d); if(!opened.ok()) return opened.status(); auto db=std::move(opened).ConsumeValueOrDie(); QueryBenchmarkResult r; auto start=std::chrono::steady_clock::now();
  const uint64_t total = static_cast<uint64_t>(o.degree) * 4;
  for(uint64_t base=0;base<total;){ const uint64_t batch = std::min<uint64_t>(o.facts_per_txn, total-base); for(uint64_t i=0;i<batch;++i){ auto s=WriteFact(db.get(),base+i+1); if(!s.ok()){r.terminal_status=s.ToString();break;} ++r.facts; } ++r.transactions; base += batch; }
  auto snap=db->BeginSnapshot(); if(snap.ok()){ auto e=snap.ValueOrDie().Exists(EntityFact::Vertex({PartId{1},VertexId{1}}),ValidTime{1}); if(e.ok()&&e.ValueOrDie()) r.rows=1; }
  r.elapsed_seconds=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
  if(o.verify_reopen&&r.terminal_status=="OK"){ snap = StatusOr<Snapshot>(Status::InvalidArgument("benchmark", "released")); db->Close().IgnoreError(); db.reset(); auto re=Database::Open(d); if(!re.ok()){r.terminal_status=re.status().ToString();} else {auto s=re.ValueOrDie()->BeginSnapshot(); auto e=s.ok()?s.ValueOrDie().Exists(EntityFact::Vertex({PartId{1},VertexId{1}}),ValidTime{1}):StatusOr<bool>(s.status()); r.reopen_verified=e.ok()&&e.ValueOrDie(); re.ValueOrDie()->Close().IgnoreError(); if(!r.reopen_verified) r.terminal_status="reopen verification failed"; }}
  return r;
}
std::string QueryBenchmarkCsvHeader(){return "operation,projection_state,degree,selectivity_percent,readers,cache_state,transactions,facts,rows,elapsed_seconds,transactions_per_second,facts_per_second,group_fill_p50,wal_sync_p99,end_to_end_p99,write_amplification,space_amplification,projection_lag,terminal_status,reopen_verified";}
std::string QueryBenchmarkCsvRow(const QueryBenchmarkOptions&o,const QueryBenchmarkResult&r){std::ostringstream x;double t=r.elapsed_seconds? r.transactions/r.elapsed_seconds:0,f=r.elapsed_seconds?r.facts/r.elapsed_seconds:0;x<<QueryBenchmarkOperationName(o.operation)<<','<<ProjectionStateName(o.projection)<<','<<o.degree<<','<<o.selectivity_percent<<','<<o.readers<<','<<(o.cache==QueryCacheState::kCold?"cold":"warm")<<','<<r.transactions<<','<<r.facts<<','<<r.rows<<','<<r.elapsed_seconds<<','<<t<<','<<f<<",0,0,0,0,0,0,"<<r.terminal_status<<','<<(r.reopen_verified?"true":"false");return x.str();}
std::string QueryBenchmarkJson(const QueryBenchmarkOptions&o,const QueryBenchmarkResult&r){std::ostringstream x;x<<"{\"operation\":\""<<QueryBenchmarkOperationName(o.operation)<<"\",\"projection_state\":\""<<ProjectionStateName(o.projection)<<"\",\"transactions\":"<<r.transactions<<",\"facts\":"<<r.facts<<",\"rows\":"<<r.rows<<",\"terminal_status\":\""<<r.terminal_status<<"\",\"reopen_verified\":"<<(r.reopen_verified?"true":"false")<<"}";return x.str();}
}
