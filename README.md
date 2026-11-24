# message-producer-sequence-gap-recovery-planner

幂等生产者（idempotent producer）重试后的序号恢复规划工具：当 broker 的
sequence 出现缺口、旧 epoch 消息晚到、新 epoch 已开始发送、分区 leader
也发生切换时，基于输入的 producer ID、epoch、sequence、broker 去重窗口、
分区 leader、提交结果和重试事件，对每条消息判断应当 **重发（RESEND）**、
**跳过（SKIP）** 还是 **阻塞（BLOCK）**。

## 重要声明：所有 broker 状态均为本地模型

本项目 **不连接任何真实 Kafka 集群或 broker**。`BrokerModel` 中的全部状态
（producer ID、epoch、sequence 去重窗口、分区 leader、提交位置、确认标志）
都只保存在当前进程的内存中，是对 broker 幂等语义的本地推演模型：

- `BrokerModel.handle_attempt` 只是本地模拟 broker 对一次生产尝试的应答，不产生网络请求；
- `BrokerModel.handle_leader_switch` 只在本地把确认标志置为未确认；
- `BrokerModel.apply_commit_result` 只是把外部传入的提交结果合并进本地集合；
- 进程退出后所有状态丢失，没有任何持久化或远程状态。

因此规划结论只在“输入事件真实反映集群状态”的前提下成立；真实集群中的最终
判定仍需以实际 broker 响应为准。

## 环境与运行

- 代码按 **Python 3.8.2** 语法编写（`dataclasses`、`typing`，不使用 3.9+ 语法）。
- 无第三方依赖。

```bash
python -m unittest          # 运行全部单元测试（测试文件位于仓库根目录，可被默认发现）
python demo.py              # 运行综合场景演示
```

## 目录结构

```text
sequence_recovery/
  __init__.py     # 公共 API 导出
  model.py        # 本地 broker 状态模型（去重窗口、epoch、leader、提交位置）
  planner.py      # 恢复规划器与报告数据类
test_sequence_recovery.py  # 单元测试：缺口 / 重复 / epoch 回退 / leader 切换
demo.py                     # 综合场景演示
```

## 输入

`plan_sequence_recovery(broker, producer_id, retry_events, commit_results)` 的输入：

| 输入 | 类型 | 含义 |
| --- | --- | --- |
| `broker` | `BrokerModel` | 本地 broker 模型，注册时给定 producer ID、epoch、去重窗口大小 |
| `producer_id` | `int` | 幂等生产者 ID |
| `retry_events` | `List[RetryEvent]` | 重试事件，每条含 producer ID、epoch、sequence |
| `commit_results` | `List[CommitResult]` | 可选，提交结果，含 epoch、leader、连续提交上限与非连续序号集合 |

分区 leader 通过 `BrokerModel(partition_leader=...)` 与
`handle_leader_switch(new_leader)` 建模。

## 判定规则

规划器对每条重试事件按以下优先级判定：

1. **leader 切换后未重新确认**：所有事件 `BLOCK / LEADER_UNCONFIRMED`，
   安全序号范围返回 `None`。必须先用同 epoch、同 leader 的 `CommitResult`
   调用 `apply_commit_result` 重新确认提交位置。
2. **旧 epoch 消息晚到**：事件 epoch 小于 broker 当前 epoch 时
   `BLOCK / OLD_EPOCH_MESSAGE`。旧 epoch 不能覆盖新 epoch；旧 epoch 的
   提交结果同样会被模型拒绝（`EpochMismatchError`）。
3. **同一 epoch 内 sequence 必须单调**：按事件输入顺序检查，序号回退或
   重复出现时 `BLOCK / NON_MONOTONIC_ATTEMPT`。
4. **重复消息**：
   - 落在连续已提交前缀 `[0, contiguous_upto]` 内，或仍在去重窗口
     `(highest - window_size, highest]` 内：`SKIP / IN_WINDOW_DUPLICATE`；
   - 已提交但滑出窗口：`BLOCK / OUT_OF_WINDOW_DUPLICATE`，
     **不能假定安全**，禁止自动重发（可能造成重复写入）。
5. **缺口与未提交序号**：
   - 连续前缀之后、重试序号之前若存在没有任何事件佐证的缺口，
     生成 `BLOCK / GAP_UNKNOWN_FATE`（合成条目，`synthetic=True`），
     其后的重发按序号顺序 `BLOCK / BLOCKED_BY_PRIOR_GAP`；
   - 缺口有事件佐证或序号紧邻连续前缀：`RESEND / GAP_RESEND`；
   - 其它未提交序号：`RESEND / UNCOMMITTED_RESEND`。

broker 模型本身的应答（`handle_attempt`）还包括 `ACCEPTED`、
`DUPLICATE_SKIPPED`、`OUT_OF_WINDOW_DUPLICATE`、`OUT_OF_ORDER`、
`EPOCH_FENCED`、`LEADER_UNCONFIRMED` 六类，用于在本地复现 broker 语义。

## 输出（RecoveryReport）

- `safe_sequence_range`：可安全跳过的连续序号区间 `(0, contiguous_upto)`；
  leader 未确认时为 `None`。
- `resend_sequences`：需要重发的序号列表，严格升序。
- `decisions`：全部消息的逐条结论（`SequenceDecision`，含决策、原因、说明）。
- `dedup_conclusions`：可确定的去重结论（RESEND 与 SKIP）。
- `undecidable`：不可判断、必须阻塞的条目（含合成的缺口条目）。

## 测试覆盖

`test_sequence_recovery.py` 共 10 个用例，按四类场景组织：

- **缺口**：缺口序号按序重发；缺口命运未知时阻塞并级联阻塞后续重发。
- **重复发送**：窗口内重复安全跳过；超出窗口的重复阻塞，不假定安全。
- **epoch 回退**：旧 epoch 消息隔离、新 epoch 正常推进；同 epoch 序号
  非单调被阻塞；旧 epoch 注册与提交结果被拒绝。
- **leader 切换**：切换后一律阻塞，同 leader 同 epoch 提交结果恢复确认，
  过期 leader 的提交结果保持未确认。

## 使用示例

```python
from sequence_recovery import BrokerModel, RetryEvent, plan_sequence_recovery

broker = BrokerModel(partition_leader=1)
broker.register_producer(producer_id=7, epoch=0, dedup_window_size=5)
broker.handle_attempt(7, 0, 0)
broker.handle_attempt(7, 0, 1)
broker.handle_attempt(7, 0, 3)  # seq 2 形成缺口

report = plan_sequence_recovery(
    broker,
    7,
    [RetryEvent(7, epoch=0, sequence=1),   # 窗口内重复 -> SKIP
     RetryEvent(7, epoch=0, sequence=2)],  # 缺口补齐 -> RESEND
)
print(report.safe_sequence_range)  # (0, 1)
print(report.resend_sequences)     # [2]
```
