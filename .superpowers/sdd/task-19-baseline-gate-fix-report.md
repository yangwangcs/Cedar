# Task 19 baseline gate fix

基于 `46d2aa4` 修复 `compare_active_overhead()` 的 baseline fail-closed 行为。

- active phase 现在显式要求 baseline 文件存在。
- `avg_facts_per_second` 与 `avg_end_to_end_p99_us` 必须是正的有限十进制数；缺失、空值、非数字、零、负数和溢出值都会使 gate 失败。
- active 样本的 throughput/p99 也经过同样的正数有限性检查，避免空值被 `awk` 当作零而绕过 gate。
- gate 失败始终写入 `summary.jsonl`，包含失败原因与实际读取值。
- contract test 覆盖 active phase 缺 baseline 非零退出，以及通过临时 input baseline 执行实际 gate 判定。

验证：

```text
bash -n benchmarks/run_cedar_query_campaign.sh
git diff --check
```

上述检查通过。完整 `QueryCampaignOptionsContract` 需要构建 `cedar_query_bench` 后运行。
