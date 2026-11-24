"""有效通道判定单领域服务：发送前按通道实际时限对每个分片做可发送判定，留下稳定、可回查的判定凭据。

背景：通道校验结果与发送分片之间原本没有稳定引用关系，通道时限调整后无法回答
“这个片当时是按哪版规则、哪条时限判定的”。本模块在每次发送判定时生成一张不可变的
有效通道判定单，记录通道编号、规则版本、实际生效时限、分片预计耗时与判定结论，
传输批次的每个分片持久引用判定单 ID，从而建立“判定单 → 分片 → 发送结果”的稳定链路。
"""
from __future__ import annotations

import itertools
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ChannelVerdictService(BaseModuleService):
    MODULE = "channel_verdict"
    ENTITY = "有效通道判定单"
    OPERATION_TERM = "判定"
    # 判定单编号需要在进程生命周期内全局唯一：内存仓库可能被重置，但已出具的
    # 判定单凭据（磁盘产物、分片引用）仍在，不能因重置而产生重复编号。
    _seq_counter = itertools.count(1)

    LIST_FIELDS = [
        "判定单编号", "批次编号", "通道编号", "通道状态", "通道时限",
        "预计耗时", "规则版本", "判定结论", "判定时间",
    ]
    REQUIRED_FIELDS = ["判定单编号", "批次编号", "通道编号"]
    STATUS_ORDER = ["草稿", "有效", "失效"]
    ACTION_RULES = {"判定有效": "有效", "判定失效": "失效"}
    NEGATIVE_ACTIONS: list[str] = []

    def issue_verdict(
        self,
        batch_code: str,
        channel_id: int,
        estimated_seconds: int,
        *,
        chunk_index: int | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """发送前按通道当前实际时限判定一个分片是否可发送，并落一张不可变判定单。

        判定规则：通道须处于“已生效”，且通道时限（超时秒数）不小于分片预计耗时。
        无论结论有效还是失效都生成判定单，失败判定同样留下凭据，供分片回查与对账。
        """
        now = time.time() if now is None else now
        channel = store.find("channel_config", channel_id)
        if channel is None:
            raise ValueError(f"传输通道 {channel_id} 不存在，无法出具通道判定单")

        channel_code = channel["通道编号"]
        channel_status = channel.get("status", "")
        rule_version = str(channel.get("配置版本", "v1.0"))
        try:
            deadline_seconds = int(channel.get("超时秒数", 0))
        except (TypeError, ValueError):
            deadline_seconds = 0

        reasons: list[str] = []
        if channel_status != "已生效":
            reasons.append(f"通道状态为{channel_status}，未生效")
        if deadline_seconds < estimated_seconds:
            reasons.append(
                f"通道时限{deadline_seconds}秒小于分片预计耗时{estimated_seconds}秒"
            )
        valid = not reasons

        seq = next(self._seq_counter)
        verdict_code = f"VERD-{batch_code}-{seq:04d}"
        values = {
            "判定单编号": verdict_code,
            "批次编号": batch_code,
            "通道编号": channel_code,
            "通道状态": channel_status,
            "通道时限": str(deadline_seconds),
            "预计耗时": str(estimated_seconds),
            "规则版本": rule_version,
            "判定结论": "有效" if valid else "失效：" + "；".join(reasons),
            "判定时间": str(int(now)),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建有效通道判定单失败: {missing}")
        # 判定单一经出具不可变：有效/失效都直接落到终态
        self.run_action(entry["id"], "判定有效" if valid else "判定失效")
        # 补充跨模块关联字段，便于分片与回查稳定引用
        entry["通道ID"] = channel_id
        entry["分片序号"] = "" if chunk_index is None else str(chunk_index)
        entry["时限秒数"] = deadline_seconds

        record = {
            "verdict_id": entry["id"],
            "verdict_code": verdict_code,
            "batch_code": batch_code,
            "chunk_index": chunk_index,
            "channel_id": channel_id,
            "channel_code": channel_code,
            "channel_status": channel_status,
            "deadline_seconds": deadline_seconds,
            "estimated_seconds": estimated_seconds,
            "rule_version": rule_version,
            "valid": valid,
            "reasons": reasons,
            "issued_at": now,
        }
        self._write_jsonl(
            self._data_dir("verdicts") / f"{batch_code}_verdicts.jsonl", [record]
        )
        return record

    def get_verdict(self, verdict_id: int) -> dict[str, Any] | None:
        """按判定单 ID 回查判定单（分片通过该 ID 与判定单保持稳定引用）。"""
        return self.get_entry(verdict_id)

    def list_for_batch(self, batch_code: str) -> list[dict[str, Any]]:
        """列出某批次全部判定单，按出具顺序返回。"""
        return [
            row for row in store.rows(self.MODULE)
            if row.get("批次编号") == batch_code
        ]

    def latest_for_batch(self, batch_code: str) -> dict[str, Any] | None:
        """取批次最新一张判定单（通道规则变更后，新判定体现当前实际时限）。"""
        rows = self.list_for_batch(batch_code)
        return rows[-1] if rows else None

    def read_verdict_artifacts(self, batch_code: str) -> list[dict[str, Any]]:
        """从磁盘判定单产物回查批次的全部判定凭据（含失效判定与原因）。"""
        return self._read_jsonl(
            self._data_dir("verdicts") / f"{batch_code}_verdicts.jsonl"
        )
