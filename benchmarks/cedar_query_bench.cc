#include <iostream>
#include <vector>
#include "benchmarks/cedar_query_bench_options.h"
#include "benchmarks/cedar_query_bench_workload.h"
int main(int argc,char**argv){std::vector<std::string>a;for(int i=1;i<argc;++i)a.emplace_back(argv[i]);auto o=cedar::benchmark::ParseQueryBenchmarkOptions(a);if(!o.ok()){std::cerr<<o.status().ToString()<<'\n';return 2;}auto r=cedar::benchmark::RunQueryBenchmark(o.ValueOrDie());if(!r.ok()){std::cerr<<r.status().ToString()<<'\n';return 1;}std::cout<<cedar::benchmark::QueryBenchmarkCsvHeader()<<'\n'<<cedar::benchmark::QueryBenchmarkCsvRow(o.ValueOrDie(),r.ValueOrDie())<<'\n';std::cerr<<cedar::benchmark::QueryBenchmarkJson(o.ValueOrDie(),r.ValueOrDie())<<'\n';return r.ValueOrDie().hard_gate_pass?0:1;}
