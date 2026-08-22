#include <gtest/gtest.h>
#include "benchmarks/cedar_query_bench_options.h"
namespace cedar::benchmark {
TEST(QueryBenchOptions, ParsesDocumentedMatrix){auto p=ParseQueryBenchmarkOptions({"--path=/tmp/q","--operation=state-at","--projection-state=canonical-only","--degree=100","--selectivity-percent=1","--readers=8","--cache-state=warm","--duration-seconds=1"});ASSERT_TRUE(p.ok())<<p.status().ToString();EXPECT_EQ(p.ValueOrDie().degree,100U);}
TEST(QueryBenchOptions, RejectsUnknownAndZero){EXPECT_FALSE(ParseQueryBenchmarkOptions({"--path=/tmp/q","--operation=legacy"}).ok());EXPECT_FALSE(ParseQueryBenchmarkOptions({"--path=/tmp/q","--duration-seconds=0"}).ok());}
TEST(QueryBenchOptions, ParsesWriteSweepControls){auto p=ParseQueryBenchmarkOptions({"--path=/tmp/q","--projection-work=active","--writers=8","--facts-per-txn=2048"});ASSERT_TRUE(p.ok());EXPECT_EQ(p.ValueOrDie().writers,8U);EXPECT_EQ(p.ValueOrDie().facts_per_txn,2048U);}
}
