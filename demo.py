"""综合场景演示：缺口 + 重复 + epoch 回退 + leader 切换。

运行：python demo.py
所有 broker 状态均为本地内存模型，不连接真实 Kafka。
"""

from sequence_recovery import (
    BrokerAck,
    BrokerModel,
    CommitResult,
    Decision,
    RetryEvent,
    plan_sequence_recovery,
)


def show(title, report):
    print("== %s ==" % title)
    print(
        "broker epoch=%s leader=%s commit_confirmed=%s safe_range=%s"
        % (
            report.broker_epoch,
            report.broker_leader,
            report.commit_confirmed,
            report.safe_sequence_range,
        )
    )
    print("resend:", report.resend_sequences)
    for item in report.decisions:
        print(
            "  epoch=%s seq=%s -> %s (%s) %s"
            % (item.epoch, item.sequence, item.decision, item.reason, item.detail)
        )
    print()


def main():
    broker = BrokerModel(partition_leader=1)
    broker.register_producer(producer_id=7, epoch=0, dedup_window_size=5)

    # 1. 正常发送 0、1，seq 2 丢失（ACK 未返回），3 已提交 -> 产生缺口
    broker.handle_attempt(7, 0, 0)
    broker.handle_attempt(7, 0, 1)
    broker.handle_attempt(7, 0, 3)

    report = plan_sequence_recovery(
        broker,
        7,
        [
            RetryEvent(7, epoch=0, sequence=1),  # 窗口内重复
            RetryEvent(7, epoch=0, sequence=2),  # 缺口补齐
        ],
    )
    show("缺口 + 重复发送", report)

    # 2. 新 producer 实例以 epoch 1 接管，旧 epoch 消息晚到
    broker.bump_epoch(7, new_epoch=1)
    broker.handle_attempt(7, 1, 0)
    report = plan_sequence_recovery(
        broker,
        7,
        [
            RetryEvent(7, epoch=0, sequence=3),  # 旧 epoch，隔离
            RetryEvent(7, epoch=1, sequence=1),  # 新 epoch 继续
        ],
    )
    show("epoch 回退", report)

    # 3. 分区 leader 切换，提交位置必须重新确认
    broker.handle_leader_switch(new_leader=2)
    report = plan_sequence_recovery(
        broker, 7, [RetryEvent(7, epoch=1, sequence=1)]
    )
    show("leader 切换后未确认", report)

    # 4. 新 leader 回传同 epoch 的提交结果后恢复
    broker.apply_commit_result(
        CommitResult(
            producer_id=7,
            epoch=1,
            leader=2,
            contiguous_upto=0,
        )
    )
    report = plan_sequence_recovery(
        broker, 7, [RetryEvent(7, epoch=1, sequence=1)]
    )
    show("leader 切换后重新确认", report)

    # 5. 超出窗口的重复不能假定安全。
    #    非连续提交 seq 9 后再提交 seq 15，窗口只覆盖 (10, 15]，
    #    seq 9 已滑出窗口且不在连续前缀内。
    broker.handle_attempt(7, 1, 9)
    broker.handle_attempt(7, 1, 15)
    report = plan_sequence_recovery(
        broker, 7, [RetryEvent(7, epoch=1, sequence=9)]
    )
    show("超出去重窗口的重复", report)
    assert report.decisions[0].decision == Decision.BLOCK


if __name__ == "__main__":
    main()
