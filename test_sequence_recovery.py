"""序号恢复规划器的单元测试。

覆盖场景：
- 序号缺口（可重发补齐 / 缺口命运未知阻塞）
- 重复发送（去重窗口内安全跳过 / 超窗不可假定安全）
- epoch 回退（旧 epoch 隔离 / 同 epoch 序号必须单调）
- leader 切换（提交位置重新确认前一律阻塞）
"""

import unittest

from sequence_recovery import (
    BrokerAck,
    BrokerModel,
    CommitResult,
    Decision,
    EpochMismatchError,
    ProducerFencedError,
    Reason,
    RetryEvent,
    plan_sequence_recovery,
)


PID = 1000
WINDOW = 5


def build_broker(leader=1, epoch=0):
    broker = BrokerModel(partition_leader=leader)
    broker.register_producer(PID, epoch=epoch, dedup_window_size=WINDOW)
    return broker


class GapRecoveryTest(unittest.TestCase):
    def test_gap_sequence_is_resent_in_order(self):
        broker = build_broker()
        for sequence in (0, 1, 3):
            self.assertEqual(
                broker.handle_attempt(PID, 0, sequence), BrokerAck.ACCEPTED
            )

        report = plan_sequence_recovery(
            broker, PID, [RetryEvent(PID, epoch=0, sequence=2)]
        )

        self.assertEqual(report.safe_sequence_range, (0, 1))
        self.assertEqual(report.resend_sequences, [2])
        decision = report.decisions[0]
        self.assertEqual(decision.decision, Decision.RESEND)
        self.assertEqual(decision.reason, Reason.GAP_RESEND)

    def test_unknown_fate_gap_blocks_later_resend(self):
        broker = build_broker()
        for sequence in (0, 1, 3):
            broker.handle_attempt(PID, 0, sequence)

        # 只重试 seq 4，缺口 seq 2 没有任何事件佐证
        report = plan_sequence_recovery(
            broker, PID, [RetryEvent(PID, epoch=0, sequence=4)]
        )

        self.assertEqual(report.resend_sequences, [])
        reasons = {item.sequence: item.reason for item in report.undecidable}
        self.assertEqual(reasons[2], Reason.GAP_UNKNOWN_FATE)
        self.assertEqual(reasons[4], Reason.BLOCKED_BY_PRIOR_GAP)
        self.assertTrue(report.decisions[0].synthetic)


class DuplicateSendTest(unittest.TestCase):
    def test_in_window_duplicate_is_skipped(self):
        broker = build_broker()
        for sequence in (0, 1, 2, 5):
            broker.handle_attempt(PID, 0, sequence)

        report = plan_sequence_recovery(
            broker,
            PID,
            [
                RetryEvent(PID, epoch=0, sequence=1),
                RetryEvent(PID, epoch=0, sequence=5),
            ],
        )

        self.assertEqual(report.resend_sequences, [])
        self.assertEqual(
            [item.decision for item in report.decisions],
            [Decision.SKIP, Decision.SKIP],
        )
        self.assertTrue(
            all(
                item.reason == Reason.IN_WINDOW_DUPLICATE
                for item in report.decisions
            )
        )
        self.assertEqual(len(report.undecidable), 0)

    def test_out_of_window_duplicate_cannot_be_assumed_safe(self):
        broker = build_broker()
        # 构造非连续提交：{0,1,2,8,13}，窗口大小 5，seq 8 已滑出窗口
        for sequence in (0, 1, 2, 8, 13):
            broker.handle_attempt(PID, 0, sequence)
        self.assertEqual(
            broker.handle_attempt(PID, 0, 8),
            BrokerAck.OUT_OF_WINDOW_DUPLICATE,
        )

        report = plan_sequence_recovery(
            broker, PID, [RetryEvent(PID, epoch=0, sequence=8)]
        )

        self.assertEqual(report.resend_sequences, [])
        self.assertEqual(len(report.decisions), 1)
        decision = report.decisions[0]
        self.assertEqual(decision.decision, Decision.BLOCK)
        self.assertEqual(decision.reason, Reason.OUT_OF_WINDOW_DUPLICATE)


class EpochRollbackTest(unittest.TestCase):
    def test_old_epoch_message_is_fenced_and_new_epoch_proceeds(self):
        broker = build_broker(epoch=0)
        broker.handle_attempt(PID, 0, 0)
        broker.bump_epoch(PID, new_epoch=1)
        broker.handle_attempt(PID, 1, 0)

        report = plan_sequence_recovery(
            broker,
            PID,
            [
                RetryEvent(PID, epoch=0, sequence=0),
                RetryEvent(PID, epoch=1, sequence=1),
            ],
        )

        by_pair = {(item.epoch, item.sequence): item for item in report.decisions}
        self.assertEqual(by_pair[(0, 0)].decision, Decision.BLOCK)
        self.assertEqual(by_pair[(0, 0)].reason, Reason.OLD_EPOCH_MESSAGE)
        self.assertEqual(by_pair[(1, 1)].decision, Decision.RESEND)
        self.assertEqual(report.resend_sequences, [1])

    def test_same_epoch_sequence_must_be_monotonic(self):
        broker = build_broker(epoch=1)
        broker.handle_attempt(PID, 1, 0)
        report = plan_sequence_recovery(
            broker,
            PID,
            [
                RetryEvent(PID, epoch=1, sequence=2),
                RetryEvent(PID, epoch=1, sequence=1),
            ],
        )

        by_sequence = {item.sequence: item for item in report.decisions}
        self.assertEqual(by_sequence[1].decision, Decision.BLOCK)
        self.assertEqual(by_sequence[1].reason, Reason.NON_MONOTONIC_ATTEMPT)
        self.assertEqual(by_sequence[2].decision, Decision.RESEND)
        self.assertEqual(report.resend_sequences, [2])

    def test_broker_rejects_regressing_epoch_registration(self):
        broker = build_broker(epoch=1)
        with self.assertRaises(ProducerFencedError):
            broker.register_producer(PID, epoch=0, dedup_window_size=WINDOW)
        with self.assertRaises(EpochMismatchError):
            broker.bump_epoch(PID, new_epoch=1)


class LeaderSwitchTest(unittest.TestCase):
    def test_switch_blocks_until_commit_position_reconfirmed(self):
        broker = build_broker(leader=1)
        broker.handle_attempt(PID, 0, 0)
        broker.handle_attempt(PID, 0, 1)
        broker.handle_leader_switch(new_leader=2)

        self.assertIsNone(broker.safe_sequence_range(PID))
        blocked_report = plan_sequence_recovery(
            broker, PID, [RetryEvent(PID, epoch=0, sequence=2)]
        )
        self.assertEqual(
            blocked_report.decisions[0].reason, Reason.LEADER_UNCONFIRMED
        )
        self.assertEqual(blocked_report.decisions[0].decision, Decision.BLOCK)

        confirmed = broker.apply_commit_result(
            CommitResult(
                producer_id=PID,
                epoch=0,
                leader=2,
                contiguous_upto=1,
            )
        )
        self.assertTrue(confirmed)

        reconfirmed_report = plan_sequence_recovery(
            broker, PID, [RetryEvent(PID, epoch=0, sequence=2)]
        )
        self.assertEqual(reconfirmed_report.safe_sequence_range, (0, 1))
        self.assertEqual(reconfirmed_report.resend_sequences, [2])

    def test_stale_leader_commit_result_keeps_state_unconfirmed(self):
        broker = build_broker(leader=1)
        broker.handle_leader_switch(new_leader=3)

        confirmed = broker.apply_commit_result(
            CommitResult(
                producer_id=PID,
                epoch=0,
                leader=2,
                contiguous_upto=0,
            )
        )
        self.assertFalse(confirmed)
        report = plan_sequence_recovery(
            broker, PID, [RetryEvent(PID, epoch=0, sequence=0)]
        )
        self.assertEqual(
            report.decisions[0].reason, Reason.LEADER_UNCONFIRMED
        )

    def test_old_epoch_commit_result_cannot_overwrite_new_epoch(self):
        broker = build_broker(epoch=0)
        broker.bump_epoch(PID, new_epoch=2)
        with self.assertRaises(EpochMismatchError):
            broker.apply_commit_result(
                CommitResult(
                    producer_id=PID,
                    epoch=0,
                    leader=1,
                    contiguous_upto=3,
                )
            )


if __name__ == "__main__":
    unittest.main()
