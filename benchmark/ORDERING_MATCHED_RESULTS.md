# AUTIG–Themis 相同证据对照：r=10 的九个补充案例

后续仅增加本地成本测量，保持本页原结果不变：[相同证据成本对照](ORDERING_COST_RESULTS.md)。

**9/9 个案例通过；AUTIG 与已有 Themis 结果的每笔交易批次编号完全一致。**
不仅汇总比例相同，九组各 1000 笔交易的有序批次划分也逐项相同。
原来的 AUTIG 分块实验和 Themis 回放结果完整保留，未重跑或覆盖。

## 最小修改与执行

- 仅给已有 `runOrdering` 测试辅助函数增加可选固定发送者列表。默认调用仍执行原来的轮换选证、多轮分块实验。
- 新增 `pkg/ofo/ordering_matched_test.go`，直接读取原始输入 JSON 与已完成的 Themis JSONL。
- 新增小规模固定证据测试，检查所选发送者、完整反转列表、单 proposal 完成和原有公平性 oracle。
- 没有修改生产 Go 代码、攻击分支、AWS 配置或 Themis 仓库，没有新增实验运行器。
- `go test ./pkg/ofo -run '^TestOrdering' -count=1` 通过；`go test ./pkg/ofo -count=1` 全包测试通过。
- 九个完整案例通过，Go 命令总耗时约 206.56 s，包含构建及测试执行；该时间不是跨协议性能指标。

## 对齐条件

| 条件 | 此次 AUTIG | 已有 Themis |
|---|---|---|
| n / F / gamma | 10 / 2 / 1 | 相同 |
| r / seeds | 10 / 1,7,19 | 相同 |
| 实际攻击节点数 | b=0,1,2，最后 b 个节点 | 相同 |
| 输入与 ID | 已保存的 1000 笔原始轨迹和交易 ID | 相同，核对原始 SHA-256 |
| 每份 LO 的交易数 | 1000，仅在新测试中指定 | 1000 |
| 反转范围 | 原生函数反转整份 1000 笔 LO | 原生函数反转整份列表 |
| 采用的发送者 | 固定节点 2..9，所有攻击者均入选 | 相同 |
| 实际采用的列表 | 八份交易索引列表逐项核对 | 与 AUTIG 完全一致 |
| proposal 数 / 完成交易数 | 1 / 1000 | 1 / 1000 |
| 额外提交延迟 | 无，有限输入同步回放 | 无 |

新 AUTIG 案例依然调用生产接收、签名、LO 生成、候选构建、全部 follower 验证和全部副本提交函数。
Themis 结果沿用其已有 leader/follower 验证及提交记录。本次没有运行网络共识或吞吐实验。

## 严格生成顺序保持率

以下为全交易对百分比的三个种子均值 ± 样本标准差。

| b | 原 AUTIG：200 笔块、轮换选证 | 对齐 AUTIG：1000 笔、固定选证 | Themis：1000 笔、固定选证 |
|---:|---:|---:|---:|
| 0 | 97.8114 ± 0.3721 | 98.1945 ± 0.2700 | 98.1945 ± 0.2700 |
| 1 | 96.4618 ± 0.2377 | 94.7542 ± 1.0317 | 94.7542 ± 1.0317 |
| 2 | 91.8853 ± 3.2590 | 59.0693 ± 10.7962 | 59.0693 ± 10.7962 |

原来 b=2 的 32.8159 个百分点差距，在相同证据条件下消失。
这说明原生配置的比较不能单独证明 AUTIG 核心排序算法具有更强的反转攻击抵抗能力。
此次同时对齐了分块/反转范围、发送者选择和 proposal 处理方式，不能进一步把差距单独归因于其中某一项。

## 对齐后其他粒度指标

下列各项在两个实现之间逐种子一致。表中列出共同的三个种子均值。

| b | 全交易对同批 % | 全交易对反向 % | 相邻生成交易对严格 % | 最大批次交易数 |
|---:|---:|---:|---:|---:|
| 0 | 1.7300 | 0.0755 | 22.1555 | 56.0000 |
| 1 | 5.2073 | 0.0385 | 9.3093 | 120.6667 |
| 2 | 40.9257 | 0.0050 | 1.0010 | 527.0000 |

全部 d=1、d<=5、d<=10、all 范围共 36 组逐案例分类计数，与 Themis 原始记录匹配。
由于同一 seed 的各 b 输出划分均匹配，无攻击基线中的严格参考集合及其配对变化也相同。
浮点汇总末位可能因运算顺序产生舍入差异，原始整数计数和批次编号完全相同。

## 公平性检查

保留 AUTIG 的原有 qH=8 原始诚实接收者 oracle，共检查 **4,378,331** 个约束，违反数为 **0**。
另对两个输出采用共同的十节点原始接收顺序前提（阈值 10），各检查 **4,293,657** 个约束，违反数均为 **0**。
Themis 已保存的该口径约束计数亦通过独立重算核对。

共同口径覆盖率为 **95.5101%**（三个种子均值），不随 b 改变。
AUTIG 原生 qH=8 口径覆盖率仍为 98.5279%、97.7207%、95.9324%，因诚实子集随 b 改变而变化。
两个口径分开报告，不能用不同分母/前提下的零违反直接比较理论公平性强弱。

这些是三个输入种子上的有限回放证据。交易对共享交易，不是独立重复样本。
“相同批次”不代表同批内部的执行序列相同：Themis 的原生线性化与 AUTIG 的 ID 顺序不同，
本次没有据此宣称两者拥有相同的批内或最终输出邻接公平性。

## 论文中应如何使用

可以新增一个简短的相同证据对照段落，保留原生配置结果作为排序粒度的敏感性分析。

> To separate reporting configuration from ordering behavior, we replay the same eight complete local-order lists in both implementations. For all nine matched executions at r=10, AUTIG and Themis produce identical ordered batch partitions, including under one or two reversing replicas. The difference observed between their native configurations therefore does not establish an intrinsic ordering-quality advantage for AUTIG. This control provides evidence of batch-output agreement under matched complete evidence, while the native experiments characterize sensitivity to reporting and evidence-processing choices.

不应继续写“相同攻击下 AUTIG 的排序准确率提高 32.82 个百分点”。
如果论文的主要贡献是增量维护、证据认证与验证成本，其优势应由已有的性能和消融实验支持。
本对照提供的是所测完整证据下的批次输出一致性，不能单独证明更高效率，也不否定已有性能结果。

## 产物与复现

输出目录：[ordering-matched-r10-20260914](results/ordering-matched-r10-20260914/)

- `run.log`：九个完整案例的通过记录。
- `measurements.jsonl`：实际所选列表、批次编号、原生/共同 oracle 结果和逐案例相等标记。
- `cases` / `pair_scopes` CSV、JSON：逐案例指标与分母。
- `case_summary` / `pair_summary` CSV、JSON：均值、样本标准差和有效种子数。
- [tables.md](results/ordering-matched-r10-20260914/tables.md)：对齐 AUTIG 的详细统计表。
- [comparison.json](results/ordering-matched-r10-20260914/comparison.json)：原 AUTIG、对齐 AUTIG、Themis 三方汇总。
- `metadata.json`：命令、版本、参数、源代码与输入哈希、后处理所复用模块的哈希。

在 AUTIG 根目录运行；输出目录须事先创建，结果文件必须不存在：

```powershell
$env:GOMAXPROCS='1'
go test ./pkg/ofo -run '^TestOrderingMatchedThemis$' -count=1 -v -timeout=10m -args -ordering-matched-input=./benchmark/results/ordering-1000-20260913 -ordering-matched-themis=/path/to/themis/benchmark/ordering-results/20260914-native-replay-final/measurements.jsonl -ordering-matched-output=/path/to/new-output/measurements.jsonl
```

原 AUTIG measurements.json、三个 r=10 输入文件以及 Themis 原始 JSONL 的 SHA-256 均已核对，保持不变。
