# 相同反转证据下的本地处理成本

本补充复用已有 r=10、seed=1/7/19、b=0/1/2 的九个相同证据案例。
固定 n=10、F=2、gamma=1、1000 笔交易，采用节点 2..9 的八份完整列表，
每个实际恶意节点反转整份原生列表。不模拟网络、ticker 或额外上报延迟。
这是相同完整证据下的成本对照，不替代原 AUTIG 的 200 笔分块、多轮实验。

## 最小代码修改

- AUTIG 原有排序测试增加 `-ordering-cost`：记录耗时，并在该模式下只让 leader 和一个 follower 执行构造、验证和提交。
- 原有匹配测试在计时模式强制检查批次与已保存的 Themis 参考逐项一致。
- Themis 原有回放增加 `-ordering-replay-cost`：只运行上述九个案例，并记录相同职责的耗时。
- 两边仍通过十个逻辑副本的原生接收和签名函数生成报告。默认测试路径保持原来的副本验证范围和输入矩阵。
- 没有修改生产算法、认证、选证、攻击逻辑、AWS 参数或旧结果。

## 计时口径

| 指标 | AUTIG | Themis |
|---|---|---|
| leader_build_ns | constructCandidate | buildProposal |
| leader_commit_ns | CommitPending | CommitVerifiedProposal |
| follower_accept_ns | 一个 follower 连续 verifyCandidate + CommitPending | 一个 follower 连续 VerifyProposal + CommitVerifiedProposal |

主比较采用 **leader 构造与提交耗时之和**及 **单个 follower 验证与提交耗时**。
Themis 在提交阶段执行最终排序，不能仅用其构造或验证函数时间代表完整职责。
leader 两段之和来自同一次执行，但中间有 follower 工作；它是两段服务调用的累计耗时，
不是连续墙钟延迟，也不是端到端提交延迟。

计时从已签名、已选定的原生证据开始，包含被调用生产函数内部的校验、分配、复制、
排序和摘要等操作，保留正常 GC；不包含输入加载、接收、报告生成、签发、网络收集、
hosting 共识、测试用公平性 oracle、诊断 SCC 重算或结果写盘。
没有手动扣除某个协议的内部工作，也没有额外补造对方原生接口不存在的认证字段。
两者原生验证契约和批内线性化不同，批次划分一致不等于最终执行序列或全部公平性语义相同。

每次重复启动新的 Go 测试进程，每个案例构建新服务状态，避免 pending 快速返回。
每个协议执行三次完整九案例运行，GOMAXPROCS=1；协议执行顺序按重复轮交替，
不并行运行两协议或在测量期间运行其他测试。
先对每个 (protocol, seed, b) 的三次计时取中位数，再报告三个输入种子的均值与样本标准差。
计时重复不视为新的输入种子，交易对也不视为独立样本。
这是一组固定主机上的小规模服务调用计时，不是稳态吞吐基准或跨轮增量收益实验。

## 复现与数据

验证完成：AUTIG 的 `go test ./pkg/ofo -run '^TestOrdering' -count=1` 通过；
两个仓库的 `go test ./pkg/ofo -count=1` 均通过。六次专项测量运行均通过，
总命令耗时约 422.68 s；该总时长包含构建、初始化、oracle 和结果写盘，不作为性能指标。

一次性运行及汇总命令保存在 `results/ordering-cost-20260914/run_cost.py`。
脚本使用两个仓库的现有 Go 测试，依次执行六次命令，保存原始 JSONL、完整日志、
环境与命令 metadata，以及源文件、输入和旧参考结果的 SHA-256。
后处理检查 54 条记录的输入哈希、采用列表、发送者及每笔交易批次与旧参考一致，
并检查完成数、公平性违反数和原生输出一致性。输出文件必须不存在，防止覆盖历史。

结果目录：`benchmark/results/ordering-cost-20260914/`。
`samples.csv` 保留每次执行的原始毫秒值；`summary.csv` 保留按种子汇总的结果。
仅重新校验与汇总已有数据可运行：

```powershell
python benchmark/results/ordering-cost-20260914/run_cost.py --analyze-only
```

## 实测结果

**54/54 个执行通过。** 两协议各九个案例、每案例三次执行，全部完成 1000 笔交易。
实际采用的八份列表及每笔交易的批次编号与旧 Themis 参考逐项一致；原生公平性检查
未发现违反。AUTIG 的 leader/follower 状态与 fragment 摘要一致；Themis 的 leader/follower
最终序列一致，其循环依据及最终相邻顺序检查也均无违反。所有原输入、旧参考和
实验期间的 Go 源文件哈希核对通过。

下表单位为 **秒/1000 笔完整证据案例**。先对每个种子的三次计时取中位数，再对
三个种子报告均值 ± 样本标准差。加速比为表中 Themis 均值 / AUTIG 均值。

| b | AUTIG leader 构建＋提交 | Themis leader 构建＋提交 | AUTIG follower 验证＋提交 | Themis follower 验证＋提交 | follower 加速比 |
|---:|---:|---:|---:|---:|---:|
| 0 | 3.876 ± 0.149 | 3.565 ± 0.056 | 1.708 ± 0.023 | 4.756 ± 0.052 | 2.78× |
| 1 | 4.201 ± 0.071 | 3.643 ± 0.042 | 1.952 ± 0.067 | 4.805 ± 0.022 | 2.46× |
| 2 | 4.287 ± 0.095 | 4.027 ± 0.067 | 1.922 ± 0.011 | 5.196 ± 0.115 | 2.70× |

本补充支持的优势是：**在所测相同反转证据与相同批次输出下，AUTIG 的原生 follower
验证及提交耗时降低 59.37%–64.08%**。在 b=2 时，两者为 1.922 s 与 5.196 s，
降低 63.01%，对应 2.70× 加速。这里完整包含 Themis 在提交阶段执行的排序，
不是只比较 verifier 函数名称。

leader 的构建及提交累计耗时没有优势：AUTIG 在 b=0/1/2 下分别高 8.73%、15.32%、6.46%。
因此不能称为“所有角色都更快”或据此推出系统 TPS 加速比。本实验一次处理完整 1000 笔，
没有跨轮复用场景，不能验证或否定跨轮增量处理收益。两协议的额外认证承诺和批内
输出语义也不同；这里比较各自原生服务路径的成本，不宣称完整验证契约等价。

AUTIG 27 个 leader 提交样本有 26 个被时钟记录为 0 ns。保留这些原始值，解释为计时器
未分辨出非零时长，而不是零计算开销；不对这一短步骤单独计算加速比。另一个样本
为非零。主指标是数秒尺度的构造＋提交及 follower 验证＋提交。
首次自动汇总因为原先要求每个子阶段严格大于零而停止；六次 Go 测试均已成功。
只将汇总校验修正为允许 leader 短提交读数为零，随后用 `--analyze-only` 复核原始数据，
没有重跑或筛选任何协议执行。各分项中位数之和不必等于同次累计耗时的中位数，
表格的 leader 累计值直接对每次的 build+commit 取中位数。

建议论文保留现有 AUTIG 公平性与粒度分析，在本地成本比较中增加这一小表或引用
follower 的结果，同时报告 leader 的相应代价。不要把本表的完整列表配置写成
原 AUTIG 的 lo_size=200 实验，也不要与 AWS 毫秒级负载延迟混用。

可支持的英文结果句：

> Across the nine matched complete-evidence cases, both implementations produce
> identical ordered batch partitions. AUTIG reduces native follower verification
> and commit time by 59.4–64.1%, including under reversing replicas, while its
> combined leader construction and commit time is 6.5–15.3% higher in this
> single-proposal workload.

## b=1 独立复测（2026-09-14）

按追加请求，预先固定只增加一轮，覆盖原 seed=1/7/19，不以达到某个百分比为停止条件。
未修改任何 Go 源码或计时边界，GOMAXPROCS=1，两协议串行执行。
通过 Go 子测试过滤保留 b=0/1：b=0 是既有配对 oracle 的前置基线，不计入本轮 b=1 汇总；
b=2 不运行。原结果及原始文件全部保留。

| b=1 比较 | AUTIG leader 构建＋提交 | Themis leader 构建＋提交 | AUTIG 相对增加 |
|---|---:|---:|---:|
| 原实验：每种子三次中位数，再对三种子平均 | 4.201 s | 3.643 s | 15.32% |
| 新复测：每种子一次，对三种子平均 | 4.272 s | 3.674 s | 16.27% |

本次没有降至约 6%。follower 验证＋提交为 AUTIG 1.963 s、Themis 4.827 s，
对应 2.46× 加速。共 12 个执行（两个协议，各三个种子的 b=0/1）全部完成，
实际证据和批次与旧参考一致、公平性检查通过、源代码与输入哈希保持一致。

原 b=1 的较大百分比不对应 AUTIG 的绝对耗时峰值：其 b=1 为 4.201 s，b=2 为 4.287 s；
Themis 则从 3.643 s 增至 4.027 s。因此 b=2 的相对差距缩小，不能据此期待 b=1 也应约为 6%。
现有阶段计时中，Themis 的提交阶段在 b=2 更贵；具体归因于哪项内部操作仍需 profiling，
本次复测没有测量或声称该因果机制。

原三次重复主结果保持不变，新结果作为补充复测，不选择更低样本替代主表。
本轮命令、完整日志、原始 JSONL、samples.csv、summary.json 与哈希记录均保存于
`benchmark/results/ordering-cost-b1-recheck-20260914/`，一次性复现脚本为 `run_recheck.py`。
