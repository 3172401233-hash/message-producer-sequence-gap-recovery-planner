"""幂等生产者序号缺口与 epoch 恢复规划工具。

注意：本包中的所有 broker 状态都是进程内的本地模型，
不会连接任何真实 Kafka 集群，仅用于推演去重、epoch 隔离与
leader 切换后的提交位置确认逻辑。
"""

from .model import (
    BrokerAck,
    BrokerModel,
    EpochMismatchError,
    ProducerFencedError,
    ProducerState,
    SequenceRecoveryError,
    UnknownProducerError,
)
from .planner import (
    CommitResult,
    Decision,
    Reason,
    RecoveryReport,
    RetryEvent,
    SequenceDecision,
    plan_sequence_recovery,
)

__all__ = [
    "BrokerAck",
    "BrokerModel",
    "CommitResult",
    "Decision",
    "EpochMismatchError",
    "ProducerFencedError",
    "ProducerState",
    "Reason",
    "RecoveryReport",
    "RetryEvent",
    "SequenceDecision",
    "SequenceRecoveryError",
    "UnknownProducerError",
    "plan_sequence_recovery",
]
