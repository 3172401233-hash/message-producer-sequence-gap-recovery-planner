"""序号恢复规划器。

规划器只读取 :class:`~sequence_recovery.model.BrokerModel` 中的
本地状态，结合提交结果与重试事件，对每条重试消息给出三种结论：

- RESEND：需要重发（缺口、未提交、或可恢复的乱序序号）
- SKIP：安全跳过（连续已提交前缀、或仍在去重窗口内的重复）
- BLOCK：无法判断，必须阻塞等待人工/集群确认
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .model import BrokerModel, UnknownProducerError


class Decision:
    """单条重试消息的最终决策。"""

    RESEND = "RESEND"
    SKIP = "SKIP"
    BLOCK = "BLOCK"


class Reason:
    """决策原因，用于报告分类。"""

    LEADER_UNCONFIRMED = "LEADER_UNCONFIRMED"
    OLD_EPOCH_MESSAGE = "OLD_EPOCH_MESSAGE"
    IN_WINDOW_DUPLICATE = "IN_WINDOW_DUPLICATE"
    OUT_OF_WINDOW_DUPLICATE = "OUT_OF_WINDOW_DUPLICATE"
    NON_MONOTONIC_ATTEMPT = "NON_MONOTONIC_ATTEMPT"
    GAP_UNKNOWN_FATE = "GAP_UNKNOWN_FATE"
    BLOCKED_BY_PRIOR_GAP = "BLOCKED_BY_PRIOR_GAP"
    GAP_RESEND = "GAP_RESEND"
    UNCOMMITTED_RESEND = "UNCOMMITTED_RESEND"


@dataclass(frozen=True)
class RetryEvent:
    """一次重试事件：producer 准备重试 (epoch, sequence) 的消息。"""

    producer_id: int
    epoch: int
    sequence: int


@dataclass(frozen=True)
class CommitResult:
    """外部回传的提交结果，用于补录 broker 本地模型的提交位置。"""

    producer_id: int
    epoch: int
    leader: int
    contiguous_upto: Optional[int] = None
    committed_sequences: Tuple[int, ...] = ()


@dataclass(frozen=True)
class SequenceDecision:
    """单条消息的规划结论。"""

    decision: str
    reason: str
    producer_id: int
    epoch: int
    sequence: int
    detail: str
    synthetic: bool = False


@dataclass
class RecoveryReport:
    """一次恢复规划的完整报告。"""

    producer_id: int
    broker_epoch: int
    broker_leader: int
    commit_confirmed: bool
    safe_sequence_range: Optional[Tuple[int, int]]
    resend_sequences: List[int] = field(default_factory=list)
    decisions: List[SequenceDecision] = field(default_factory=list)

    @property
    def dedup_conclusions(self) -> List[SequenceDecision]:
        """可以给出确定去重结论的条目（重发或安全跳过）。"""
        return [item for item in self.decisions if item.decision != Decision.BLOCK]

    @property
    def undecidable(self) -> List[SequenceDecision]:
        """无法判断、必须阻塞的条目。"""
        return [item for item in self.decisions if item.decision == Decision.BLOCK]

    def decisions_for(self, decision: str) -> List[SequenceDecision]:
        return [item for item in self.decisions if item.decision == decision]


def plan_sequence_recovery(
    broker: BrokerModel,
    producer_id: int,
    retry_events: List[RetryEvent],
    commit_results: Optional[List[CommitResult]] = None,
) -> RecoveryReport:
    """根据本地 broker 模型与重试事件规划序号恢复。

    判定优先级：
    1. leader 切换后提交位置未确认 -> BLOCK
    2. 旧 epoch 消息晚到 -> BLOCK（旧 epoch 不能覆盖新 epoch）
    3. 同一 epoch 内序号必须单调，回退的尝试 -> BLOCK
    4. 已提交重复：连续前缀或去重窗口内 -> SKIP，否则 -> BLOCK
    5. 未提交序号及其与连续前缀之间的缺口 -> RESEND；
       缺口内没有任何重试事件可佐证的序号 -> BLOCK
    """
    state = broker.get_state(producer_id)
    if state is None:
        raise UnknownProducerError(
            "producer %s 未在 broker 本地模型中注册" % producer_id
        )

    for commit_result in commit_results or []:
        broker.apply_commit_result(commit_result)

    decisions = []

    if not state.commit_confirmed:
        for event in retry_events:
            decisions.append(
                SequenceDecision(
                    Decision.BLOCK,
                    Reason.LEADER_UNCONFIRMED,
                    event.producer_id,
                    event.epoch,
                    event.sequence,
                    "分区 leader 已切换到 %s，提交位置尚未重新确认"
                    % broker.partition_leader,
                )
            )
        return _build_report(broker, producer_id, [], decisions)

    resend_sequences = []
    candidate_decisions = {}
    blocked_pairs = set()
    last_sequence_by_epoch = {}

    for event in retry_events:
        pair = (event.epoch, event.sequence)
        last_seen = last_sequence_by_epoch.get(event.epoch)

        if event.epoch < state.epoch:
            decisions.append(
                SequenceDecision(
                    Decision.BLOCK,
                    Reason.OLD_EPOCH_MESSAGE,
                    event.producer_id,
                    event.epoch,
                    event.sequence,
                    "旧 epoch %s 消息晚到，broker 已在 epoch %s 发送，"
                    "旧 epoch 不能覆盖新 epoch"
                    % (event.epoch, state.epoch),
                )
            )
            blocked_pairs.add(pair)
            continue
        if event.epoch > state.epoch:
            decisions.append(
                SequenceDecision(
                    Decision.BLOCK,
                    Reason.OLD_EPOCH_MESSAGE,
                    event.producer_id,
                    event.epoch,
                    event.sequence,
                    "事件 epoch %s 高于 broker 已知 epoch %s，本地模型无法确认"
                    % (event.epoch, state.epoch),
                )
            )
            blocked_pairs.add(pair)
            continue
        if last_seen is not None and event.sequence <= last_seen:
            decisions.append(
                SequenceDecision(
                    Decision.BLOCK,
                    Reason.NON_MONOTONIC_ATTEMPT,
                    event.producer_id,
                    event.epoch,
                    event.sequence,
                    "同一 epoch %s 内序号必须单调递增，上一条序号为 %s"
                    % (event.epoch, last_seen),
                )
            )
            blocked_pairs.add(pair)
            continue
        last_sequence_by_epoch[event.epoch] = event.sequence

        if event.sequence <= state.contiguous_upto or (
            event.sequence in state.committed_sequences
            and state.in_dedup_window(event.sequence)
        ):
            decisions.append(
                SequenceDecision(
                    Decision.SKIP,
                    Reason.IN_WINDOW_DUPLICATE,
                    event.producer_id,
                    event.epoch,
                    event.sequence,
                    "重复消息仍可由 broker 去重（连续已提交前缀或去重窗口内）",
                )
            )
            continue
        if event.sequence in state.committed_sequences:
            decisions.append(
                SequenceDecision(
                    Decision.BLOCK,
                    Reason.OUT_OF_WINDOW_DUPLICATE,
                    event.producer_id,
                    event.epoch,
                    event.sequence,
                    "重复消息已超出 broker 去重窗口，不能假定安全，需要人工确认",
                )
            )
            blocked_pairs.add(pair)
            continue

        candidate_decisions[pair] = SequenceDecision(
            Decision.RESEND,
            Reason.UNCOMMITTED_RESEND,
            event.producer_id,
            event.epoch,
            event.sequence,
            "序号未在 broker 本地模型中提交，需要重发",
        )

    candidate_sequences = sorted(
        sequence for (epoch, sequence) in candidate_decisions if epoch == state.epoch
    )
    gap_blocked = set()

    for candidate in candidate_sequences:
        start = state.contiguous_upto + 1
        for sequence in range(start, candidate):
            pair = (state.epoch, sequence)
            if pair in blocked_pairs or pair in gap_blocked:
                # 已有明确 BLOCK 结论（非单调、超窗重复等）的序号，
                # 不再重复生成“缺口命运未知”结论
                continue
            if pair in candidate_decisions:
                continue
            if sequence in state.committed_sequences:
                continue
            gap_blocked.add(pair)
            decisions.append(
                SequenceDecision(
                    Decision.BLOCK,
                    Reason.GAP_UNKNOWN_FATE,
                    producer_id,
                    state.epoch,
                    sequence,
                    "序号缺口 %s 没有对应重试事件，无法判断提交命运，阻塞后续重发"
                    % sequence,
                    synthetic=True,
                )
            )

    for candidate in candidate_sequences:
        pair = (state.epoch, candidate)
        prior_gap = {
            (state.epoch, sequence)
            for sequence in range(state.contiguous_upto + 1, candidate)
        }
        if prior_gap & gap_blocked:
            decisions.append(
                SequenceDecision(
                    Decision.BLOCK,
                    Reason.BLOCKED_BY_PRIOR_GAP,
                    producer_id,
                    state.epoch,
                    candidate,
                    "前面的序号缺口无法判断，按序号顺序阻塞该重发",
                )
            )
            continue
        resend_sequences.append(candidate)
        if candidate == state.contiguous_upto + 1:
            reason = Reason.GAP_RESEND
            detail = "补齐连续序号缺口，按顺序重发"
        else:
            reason = Reason.UNCOMMITTED_RESEND
            detail = candidate_decisions[pair].detail
        decisions.append(
            SequenceDecision(
                Decision.RESEND,
                reason,
                producer_id,
                state.epoch,
                candidate,
                detail,
            )
        )

    decisions.sort(key=lambda item: (item.epoch, item.sequence))
    return _build_report(broker, producer_id, resend_sequences, decisions)


def _build_report(
    broker: BrokerModel,
    producer_id: int,
    resend_sequences: List[int],
    decisions: List[SequenceDecision],
) -> RecoveryReport:
    state = broker.get_state(producer_id)
    return RecoveryReport(
        producer_id=producer_id,
        broker_epoch=state.epoch,
        broker_leader=broker.partition_leader,
        commit_confirmed=state.commit_confirmed,
        safe_sequence_range=broker.safe_sequence_range(producer_id),
        resend_sequences=resend_sequences,
        decisions=decisions,
    )
