"""通道判定单驱动的批次分片调度测试。

复现并验证业务场景：
同一批次切两片，首片已发送、第二片仍在队列；通道时限由 90 秒缩为 45 秒，
第二片预计耗时 60 秒。每片都能回查实际生效时限、规则版本与发送结果；
暂停或超时的片保留续传位置；暂存侧只确认真正交付的片；
传输批次读取有效通道判定单分配发送范围，结果同步落地与对账；
首片成功后启用新版时限，再提交第二片并观察发送/等待状态；
随后恢复受影响的片并核对暂存范围、规则版本和最终条数。
"""
from __future__ import annotations

import json

import pytest

from app.services.base import DATA_ROOT
from app.services.channel_config import ChannelConfigService
from app.services.channel_verdict import ChannelVerdictService
from app.services.transfer_batch import (
    TransferBatchService,
    CHUNK_STATUS_SENT,
    CHUNK_STATUS_WAITING,
    CHUNK_STATUS_PAUSED,
    CHUNK_STATUS_TIMEOUT,
)
from app.services.staging_area import StagingAreaService
from app.services.arrival_reconcile import ArrivalReconcileService
from app.store import store


@pytest.fixture(autouse=True)
def reset_store():
    """每个用例前重置内存仓库，保证模块数据隔离。"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _make_channel(timeout: int = 90) -> dict:
    """建一条已生效通道，初始时限 90 秒、规则版本 v1.0。"""
    svc = ChannelConfigService()
    channel = svc.create_channel("HTTPS", 4, "目标端通道")
    svc.run_action(channel["id"], "确认生效")
    channel["超时秒数"] = str(timeout)
    channel["配置版本"] = "v1.0"
    return channel


def _make_batch(code: str, record_count: int = 10) -> dict:
    svc = TransferBatchService()
    return svc.create_batch(f"WIN-{code}", record_count, "TARG-0001")


class TestChannelVerdictLifecycle:
    """有效通道判定单：发送前按实际时限判定，稳定引用、可回查、不可变。"""

    def test_verdict_valid_when_deadline_covers_estimate(self):
        channel = _make_channel(timeout=90)
        verdict = ChannelVerdictService().issue_verdict(
            "TRAN-V1", channel["id"], estimated_seconds=60)
        assert verdict["valid"] is True
        assert verdict["deadline_seconds"] == 90
        assert verdict["rule_version"] == "v1.0"
        stored = ChannelVerdictService().get_verdict(verdict["verdict_id"])
        assert stored["判定结论"] == "有效"
        assert stored["通道时限"] == "90"

    def test_verdict_invalid_when_deadline_shrinks_below_estimate(self):
        channel = _make_channel(timeout=90)
        svc = ChannelConfigService()
        svc.update_timeout(channel["id"], 45)
        channel = svc.get_entry(channel["id"])
        assert channel["超时秒数"] == "45"
        assert channel["配置版本"] == "v2.0"

        verdict = ChannelVerdictService().issue_verdict(
            "TRAN-V2", channel["id"], estimated_seconds=60)
        assert verdict["valid"] is False
        assert verdict["deadline_seconds"] == 45
        assert verdict["rule_version"] == "v2.0"
        assert any("45秒小于" in r for r in verdict["reasons"])
        # 失效判定同样落单、同样可回查（磁盘产物按批次累积，校验本批次本次结论即可）
        artifacts = ChannelVerdictService().read_verdict_artifacts("TRAN-V2")
        assert artifacts and artifacts[-1]["valid"] is False
        assert artifacts[-1]["deadline_seconds"] == 45

    def test_verdict_rejects_non_effective_channel(self):
        channel = ChannelConfigService().create_channel("TCP", 2, "源端通道")
        verdict = ChannelVerdictService().issue_verdict(
            "TRAN-V3", channel["id"], estimated_seconds=10)
        assert verdict["valid"] is False
        assert any("未生效" in r for r in verdict["reasons"])

    def test_historical_verdicts_are_immutable_after_timeout_change(self):
        channel = _make_channel(timeout=90)
        vs = ChannelVerdictService()
        first = vs.issue_verdict("TRAN-V4", channel["id"], 60)
        ChannelConfigService().update_timeout(channel["id"], 45)
        second = vs.issue_verdict("TRAN-V4", channel["id"], 60)
        # 历史判定单不受后续规则变更影响，两张单各自定格当时的时限与版本
        old = vs.get_verdict(first["verdict_id"])
        new = vs.get_verdict(second["verdict_id"])
        assert (old["通道时限"], old["规则版本"], old["status"]) == ("90", "v1.0", "有效")
        assert (new["通道时限"], new["规则版本"], new["status"]) == ("45", "v2.0", "失效")


class TestDispatchScenario:
    """完整场景：两片批次、时限缩短、续传、落地与对账。"""

    def test_full_two_chunk_scenario_with_timeout_shrink_and_resume(self):
        channel = _make_channel(timeout=90)
        batch = _make_batch("SCEN-A", record_count=10)
        tb = TransferBatchService()

        # 发送前准备：两片各 5 条，预计耗时 30s 与 60s，初始时限 90s 均判定有效
        prep = tb.prepare_dispatch(
            batch["batch_id"], channel["id"],
            chunk_sizes=[5, 5], chunk_seconds=[30, 60])
        plan = prep["plan"]
        assert len(plan["chunks"]) == 2
        assert [c["line_start"] for c in plan["chunks"]] == [1, 6]
        assert [c["line_end"] for c in plan["chunks"]] == [5, 10]
        assert all(c["status"] == CHUNK_STATUS_WAITING for c in plan["chunks"])
        # 每片都稳定绑定一张判定单
        assert all(c["verdict_id"] and c["verdict_code"] for c in plan["chunks"])

        # 首片发送成功：回查实际生效时限、版本与发送结果
        first_send = tb.send_chunk(batch["batch_code"], 0)
        assert first_send["state"] == "已发送"
        first_chunk = first_send["chunk"]
        assert first_chunk["status"] == CHUNK_STATUS_SENT
        assert first_chunk["delivered"] is True
        assert first_chunk["deadline_seconds"] == 90
        assert first_chunk["rule_version"] == "v1.0"
        assert first_chunk["result"]["records_delivered"] == 5

        # 首片成功后启用新版时限：90s → 45s，第二片仍在队列等待且预计 60s
        upd = ChannelConfigService().update_timeout(channel["id"], 45)
        assert upd["rule_version"] == "v2.0"

        # 提交第二片：当前规则判定失效，只能继续等待，不产生交付
        second_attempt = tb.send_chunk(batch["batch_code"], 1)
        assert second_attempt["delivered"] is False
        assert second_attempt["state"] == CHUNK_STATUS_WAITING
        second_chunk = second_attempt["chunk"]
        assert second_chunk["deadline_seconds"] == 45
        assert second_chunk["rule_version"] == "v2.0"
        assert "失效" in second_chunk["remark"]

        # 交付产物此时只含首片 5 条
        delivered_file = tb._delivered_path(batch["batch_code"])
        delivered_records = tb._read_jsonl(delivered_file)
        assert len(delivered_records) == 5
        assert all(r["chunk_index"] == 0 for r in delivered_records)

        # 受影响的第二片按超时挂起：保留继续发送的位置并登记超时断点
        timed = tb.timeout_chunk(batch["batch_code"], 1, reason="时限缩短后预计耗时超时限")
        assert timed["chunk"]["status"] == CHUNK_STATUS_TIMEOUT
        assert timed["resume_point"]["resume_position"] == 1

        # 同步给暂存与对账：暂存只确认已交付片，对账识别第二片缺失
        sync = tb.sync_delivery_to_staging_and_reconcile(batch["batch_code"])
        assert sync["auto_staging"]["records_landed"] == 5
        assert sync["auto_staging"]["line_ranges"] == ["1-5"]
        assert sync["auto_reconcile"]["balanced"] is False
        assert sync["auto_reconcile"]["blocked_chunks"] == [1]
        assert sync["auto_reconcile"]["missing"] == 5
        assert sync["auto_reconcile"]["delivered_rule_versions"] == ["v1.0"]

        # 通道恢复到可承载第二片的时限（仍记录为新版本 v3.0），恢复第二片并发送
        ChannelConfigService().update_timeout(channel["id"], 90)
        resumed = tb.resume_chunk(batch["batch_code"], 1)
        assert resumed["resumed_from"] == CHUNK_STATUS_TIMEOUT
        assert resumed["state"] == "已发送"
        assert resumed["chunk"]["rule_version"] == "v3.0"
        assert resumed["chunk"]["delivered"] is True

        # 再次同步：暂存范围覆盖 1-10，规则版本含 v1.0/v3.0，最终条数 10
        final_sync = tb.sync_delivery_to_staging_and_reconcile(batch["batch_code"])
        staging = final_sync["auto_staging"]
        assert staging["records_landed"] == 10
        assert staging["line_ranges"] == ["1-5", "6-10"]
        assert sorted(staging["rule_versions"]) == ["v1.0", "v3.0"]
        recon = final_sync["auto_reconcile"]
        assert recon["balanced"] is True
        assert recon["expected"] == recon["delivered"] == recon["staging_count"] == 10
        assert recon["missing"] == 0 and recon["blocked_chunks"] == []

        # 批次水位与最终条数
        final_plan = tb.get_dispatch_plan(batch["batch_code"])
        assert all(c["status"] == CHUNK_STATUS_SENT for c in final_plan["chunks"])
        batch_entry = tb.get_entry(batch["batch_id"])
        assert batch_entry["批次水位"] == "10"
        assert batch_entry["status"] == "已完成"

    def test_paused_chunk_keeps_position_and_registers_resume_point(self):
        channel = _make_channel(timeout=90)
        batch = _make_batch("SCEN-B", record_count=8)
        tb = TransferBatchService()
        tb.prepare_dispatch(batch["batch_id"], channel["id"],
                            chunk_sizes=[4, 4], chunk_seconds=[10, 20])

        paused = tb.pause_chunk(batch["batch_code"], 1, reason="窗口维护暂停")
        assert paused["chunk"]["status"] == CHUNK_STATUS_PAUSED
        # 暂停片在计划中的位置（分片序号 1）保留，并生成续传断点
        resume = paused["resume_point"]
        assert resume["chunk_index"] == 1
        assert resume["point_type"] == "暂停中断"
        rp_rows = [r for r in store.rows("resume_point")
                   if r.get("批次编号") == batch["batch_code"]]
        assert rp_rows and rp_rows[0]["恢复位置"] == "1"

        # 未恢复不能直接发送
        blocked = tb.send_chunk(batch["batch_code"], 1)
        assert "须先恢复" in blocked["error"]

        resumed = tb.resume_chunk(batch["batch_code"], 1)
        assert resumed["state"] == "已发送"
        assert resumed["chunk"]["status"] == CHUNK_STATUS_SENT
        assert len(tb._read_jsonl(tb._delivered_path(batch["batch_code"]))) == 4

    def test_timeout_chunk_keeps_position_and_staging_ignores_it(self):
        channel = _make_channel(timeout=45)
        batch = _make_batch("SCEN-C", record_count=6)
        tb = TransferBatchService()
        # 时限 45s：片0 预计 30s 有效，片1 预计 60s 准备阶段即失效、排队等待
        tb.prepare_dispatch(batch["batch_id"], channel["id"],
                            chunk_sizes=[3, 3], chunk_seconds=[30, 60])
        tb.send_chunk(batch["batch_code"], 0)

        timed = tb.timeout_chunk(batch["batch_code"], 1, reason="发送超过通道时限")
        assert timed["chunk"]["status"] == CHUNK_STATUS_TIMEOUT
        assert timed["resume_point"]["point_type"] == "超时中断"
        assert timed["resume_point"]["resume_position"] == 1

        # 暂存侧只确认真正交付的首片
        staging = StagingAreaService().land_delivered_chunks(batch["batch_code"])
        assert staging["records_landed"] == 3
        assert staging["line_ranges"] == ["1-3"]

        # 放宽时限后恢复超时片
        ChannelConfigService().update_timeout(channel["id"], 90)
        resumed = tb.resume_chunk(batch["batch_code"], 1)
        assert resumed["state"] == "已发送"
        plan = tb.get_dispatch_plan(batch["batch_code"])
        assert [c["status"] for c in plan["chunks"]] == [CHUNK_STATUS_SENT, CHUNK_STATUS_SENT]

    def test_each_chunk_is_traceable_to_verdict_deadline_and_result(self):
        """每片可回查实际生效的时限、规则版本和发送结果（判定单 ↔ 分片稳定引用）。"""
        channel = _make_channel(timeout=90)
        batch = _make_batch("SCEN-D", record_count=10)
        tb = TransferBatchService()
        tb.prepare_dispatch(batch["batch_id"], channel["id"],
                            chunk_sizes=[5, 5], chunk_seconds=[30, 60])
        tb.send_chunk(batch["batch_code"], 0)
        ChannelConfigService().update_timeout(channel["id"], 45)
        tb.send_chunk(batch["batch_code"], 1)

        plan = tb.get_dispatch_plan(batch["batch_code"])
        c0, c1 = plan["chunks"]
        # 首片定格 v1.0/90s，第二片最近一次判定定格 v2.0/45s
        assert (c0["rule_version"], c0["deadline_seconds"]) == ("v1.0", 90)
        assert (c1["rule_version"], c1["deadline_seconds"]) == ("v2.0", 45)
        assert c0["result"]["state"] == "已发送"
        assert c1["result"] is None and c1["status"] == CHUNK_STATUS_WAITING

        # 通过判定单 ID 回查判定单实体，引用稳定存在
        vs = ChannelVerdictService()
        assert vs.get_verdict(c0["verdict_id"])["判定单编号"] == c0["verdict_code"]
        batch_verdicts = vs.list_for_batch(batch["batch_code"])
        # 准备阶段 2 张 + 两次发送各 1 张
        assert len(batch_verdicts) == 4

    def test_dispatch_plan_and_verdict_artifacts_persisted_on_disk(self):
        channel = _make_channel(timeout=90)
        batch = _make_batch("SCEN-E", record_count=4)
        tb = TransferBatchService()
        tb.prepare_dispatch(batch["batch_id"], channel["id"],
                            chunk_sizes=[2, 2], chunk_seconds=[10, 10])
        plan_file = tb._plan_path(batch["batch_code"])
        assert plan_file.exists()
        on_disk = json.loads(plan_file.read_text(encoding="utf-8"))
        assert on_disk["total_records"] == 4
        verdict_file = DATA_ROOT / "verdicts" / f"{batch['batch_code']}_verdicts.jsonl"
        assert verdict_file.exists()

    def test_reconcile_reports_version_inconsistency(self):
        """暂存规则版本与交付判定单版本不一致时对账给出差异结论。"""
        channel = _make_channel(timeout=90)
        batch = _make_batch("SCEN-F", record_count=4)
        tb = TransferBatchService()
        tb.prepare_dispatch(batch["batch_id"], channel["id"],
                            chunk_sizes=[2, 2], chunk_seconds=[10, 10])
        tb.send_chunk(batch["batch_code"], 0)
        # 先落地一次真实交付（v1.0）
        StagingAreaService().land_delivered_chunks(batch["batch_code"])
        # 伪造一条版本不一致的暂存登记，模拟暂存侧引用了错误规则版本
        bad = StagingAreaService().create_entry({
            "暂存编号": f"STAG-{batch['batch_code']}-DELIV-BAD",
            "批次编号": batch["batch_code"],
            "落地路径": "/staging/fake/",
        })[0]
        bad["规则版本"] = "v9.9"
        bad["暂存范围"] = "1-2"

        ar = ArrivalReconcileService()
        recon = ar.create_reconciliation(batch["batch_code"], 4)
        result = ar.reconcile_by_verdict(recon["recon_id"])
        assert result["version_consistent"] is False
        assert result["balanced"] is False


class TestDispatchGuardRails:
    """调度守卫：重复发送、错误恢复、缺失计划等非法路径被拦截。"""

    def test_cannot_send_twice(self):
        channel = _make_channel(timeout=90)
        batch = _make_batch("GUARD-1", record_count=4)
        tb = TransferBatchService()
        tb.prepare_dispatch(batch["batch_id"], channel["id"],
                            chunk_sizes=[2, 2], chunk_seconds=[10, 10])
        tb.send_chunk(batch["batch_code"], 0)
        again = tb.send_chunk(batch["batch_code"], 0)
        assert "不能重复发送" in again["error"]

    def test_cannot_pause_sent_chunk(self):
        channel = _make_channel(timeout=90)
        batch = _make_batch("GUARD-2", record_count=4)
        tb = TransferBatchService()
        tb.prepare_dispatch(batch["batch_id"], channel["id"],
                            chunk_sizes=[2, 2], chunk_seconds=[10, 10])
        tb.send_chunk(batch["batch_code"], 0)
        result = tb.pause_chunk(batch["batch_code"], 0)
        assert "只有等待中的分片可暂停" in result["error"]

    def test_resume_without_interruption_is_rejected(self):
        channel = _make_channel(timeout=90)
        batch = _make_batch("GUARD-3", record_count=4)
        tb = TransferBatchService()
        tb.prepare_dispatch(batch["batch_id"], channel["id"],
                            chunk_sizes=[2, 2], chunk_seconds=[10, 10])
        result = tb.resume_chunk(batch["batch_code"], 0)
        assert "无需恢复" in result["error"]

    def test_plan_missing_raises(self):
        with pytest.raises(ValueError):
            TransferBatchService().get_dispatch_plan("TRAN-NOT-EXIST")

    def test_prepare_rejects_mismatched_chunk_sizes(self):
        channel = _make_channel(timeout=90)
        batch = _make_batch("GUARD-4", record_count=4)
        result = TransferBatchService().prepare_dispatch(
            batch["batch_id"], channel["id"], chunk_sizes=[3, 3])
        assert "不一致" in result["error"]

    def test_invalid_timeout_update_rejected(self):
        channel = _make_channel(timeout=90)
        result = ChannelConfigService().update_timeout(channel["id"], 0)
        assert "正整数" in result["error"]
