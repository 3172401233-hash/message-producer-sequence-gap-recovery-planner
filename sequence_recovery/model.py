"""本地 broker 状态模型。

本模块不连接任何真实 Kafka broker。所有状态（producer ID、epoch、
序号去重窗口、分区 leader、提交位置）都保存在进程内存中，
仅用于模拟幂等生产者场景下的判定逻辑。
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple


class SequenceRecoveryError(Exception):
    """本包所有异常的基类。"""


class UnknownProducerError(SequenceRecoveryError):
    """broker 本地模型中没有注册该 producer ID。"""


class ProducerFencedError(SequenceRecoveryError):
    """使用旧 epoch 注册 producer，已被新 epoch 隔离。"""


class EpochMismatchError(SequenceRecoveryError):
    """事件或提交结果的 epoch 与 broker 当前 epoch 不一致。"""


class BrokerAck:
    """broker 本地模型对一次生产尝试的应答类别。"""

    ACCEPTED = "ACCEPTED"
    DUPLICATE_SKIPPED = "DUPLICATE_SKIPPED"
    OUT_OF_WINDOW_DUPLICATE = "OUT_OF_WINDOW_DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    EPOCH_FENCED = "EPOCH_FENCED"
    LEADER_UNCONFIRMED = "LEADER_UNCONFIRMED"


@dataclass
class ProducerState:
    """单个 producer ID 在 broker 本地模型中的序号状态。"""

    producer_id: int
    epoch: int
    dedup_window_size: int
    current_leader: int
    commit_confirmed: bool = True
    next_expected_sequence: int = 0
    contiguous_upto: int = -1
    highest_sequence: int = -1
    committed_sequences: Set[int] = field(default_factory=set)

    def in_dedup_window(self, sequence: int) -> bool:
        """序号是否仍落在 broker 的去重窗口内。"""
        if sequence not in self.committed_sequences:
            return False
        # 窗口只覆盖最近 dedup_window_size 个序号：
        # (highest - window_size, highest]
        return sequence > self.highest_sequence - self.dedup_window_size


@dataclass
class BrokerModel:
    """分区 leader 与 producer 序号状态的本地模型。"""

    partition_leader: int
    _states: Dict[int, ProducerState] = field(default_factory=dict)

    def register_producer(
        self, producer_id: int, epoch: int, dedup_window_size: int
    ) -> ProducerState:
        state = self._states.get(producer_id)
        if state is None:
            state = ProducerState(
                producer_id=producer_id,
                epoch=epoch,
                dedup_window_size=dedup_window_size,
                current_leader=self.partition_leader,
            )
            self._states[producer_id] = state
            return state
        if epoch < state.epoch:
            raise ProducerFencedError(
                "producer %s 旧 epoch %s 已被 epoch %s 隔离"
                % (producer_id, epoch, state.epoch)
            )
        if epoch > state.epoch:
            raise EpochMismatchError(
                "producer %s 的新 epoch %s 需要先调用 bump_epoch"
                % (producer_id, epoch)
            )
        return state

    def bump_epoch(self, producer_id: int, new_epoch: int) -> ProducerState:
        """模拟新 producer 实例以更高 epoch 接管，序号追踪重新开始。"""
        state = self._require_state(producer_id)
        if new_epoch <= state.epoch:
            raise EpochMismatchError(
                "新 epoch %s 必须大于当前 epoch %s"
                % (new_epoch, state.epoch)
            )
        state.epoch = new_epoch
        state.next_expected_sequence = 0
        state.contiguous_upto = -1
        state.highest_sequence = -1
        state.committed_sequences.clear()
        state.current_leader = self.partition_leader
        state.commit_confirmed = True
        return state

    def get_state(self, producer_id: int) -> Optional[ProducerState]:
        return self._states.get(producer_id)

    def handle_attempt(
        self, producer_id: int, epoch: int, sequence: int
    ) -> str:
        """模拟 broker 收到一次生产尝试后的应答（会修改本地状态）。"""
        state = self._require_state(producer_id)
        if not state.commit_confirmed:
            return BrokerAck.LEADER_UNCONFIRMED
        if epoch < state.epoch:
            return BrokerAck.EPOCH_FENCED
        if epoch > state.epoch:
            raise EpochMismatchError(
                "未知 epoch %s，broker 当前 epoch 为 %s"
                % (epoch, state.epoch)
            )
        if sequence <= state.contiguous_upto:
            # 连续已提交前缀内的重复，即使滑出窗口也可安全跳过
            return BrokerAck.DUPLICATE_SKIPPED
        if sequence in state.committed_sequences:
            if state.in_dedup_window(sequence):
                return BrokerAck.DUPLICATE_SKIPPED
            return BrokerAck.OUT_OF_WINDOW_DUPLICATE
        if sequence < state.next_expected_sequence:
            return BrokerAck.OUT_OF_ORDER
        # 本地模型允许非连续写入，以便复现“序号缺口”场景
        state.committed_sequences.add(sequence)
        if sequence > state.highest_sequence:
            state.highest_sequence = sequence
        while state.contiguous_upto + 1 in state.committed_sequences:
            state.contiguous_upto += 1
        state.next_expected_sequence = state.highest_sequence + 1
        return BrokerAck.ACCEPTED

    def handle_leader_switch(self, new_leader: int) -> None:
        """分区 leader 切换：所有 producer 的提交位置都要重新确认。"""
        self.partition_leader = new_leader
        for state in self._states.values():
            state.current_leader = new_leader
            state.commit_confirmed = False

    def apply_commit_result(self, commit_result: "CommitResult") -> bool:
        """录入一次提交结果，用于 leader 切换后重新确认提交位置。

        旧 epoch 的提交结果一律拒绝，不能覆盖新 epoch 的状态；
        只有 leader 与当前分区 leader 一致时才恢复确认状态。
        """
        state = self._require_state(commit_result.producer_id)
        if commit_result.epoch != state.epoch:
            raise EpochMismatchError(
                "提交结果 epoch %s 与 broker 当前 epoch %s 不一致，"
                "旧 epoch 不能覆盖新 epoch"
                % (commit_result.epoch, state.epoch)
            )
        merged = set(state.committed_sequences)
        merged.update(commit_result.committed_sequences)
        if commit_result.contiguous_upto is not None:
            for sequence in range(commit_result.contiguous_upto + 1):
                merged.add(sequence)
        state.committed_sequences = merged
        if merged:
            state.highest_sequence = max(state.highest_sequence, max(merged))
        contiguous = -1
        while contiguous + 1 in state.committed_sequences:
            contiguous += 1
        state.contiguous_upto = contiguous
        state.next_expected_sequence = state.highest_sequence + 1
        if commit_result.leader == self.partition_leader:
            state.current_leader = commit_result.leader
            state.commit_confirmed = True
        return state.commit_confirmed

    def safe_sequence_range(self, producer_id: int) -> Optional[Tuple[int, int]]:
        """返回可安全跳过的连续序号区间 [0, contiguous_upto]。

        leader 切换未重新确认时返回 None，表示无安全区间可言。
        """
        state = self._require_state(producer_id)
        if not state.commit_confirmed:
            return None
        return 0, state.contiguous_upto

    def _require_state(self, producer_id: int) -> ProducerState:
        state = self._states.get(producer_id)
        if state is None:
            raise UnknownProducerError(
                "producer %s 未在 broker 本地模型中注册" % producer_id
            )
        return state
