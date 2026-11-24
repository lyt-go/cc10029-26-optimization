"""传输批次领域服务：维护传输批次的分片组织、记录条数与字节总量登记及批次水位推进。

本模块读取抽取窗口产物文件后创建传输批次，分片写入传输数据文件，
传输完成后自动驱动暂存落地和校验核对，形成窗口抽取→批次传输→落地校验的贯通链路。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 分片生命周期状态：等待中的片在队列，暂停/超时的片保留续传位置，只有已发送才算真正交付。
CHUNK_STATUS_WAITING = "等待中"
CHUNK_STATUS_SENT = "已发送"
CHUNK_STATUS_PAUSED = "已暂停"
CHUNK_STATUS_TIMEOUT = "已超时"
RESUMABLE_STATUSES = (CHUNK_STATUS_PAUSED, CHUNK_STATUS_TIMEOUT)
BLOCKED_STATUSES = (CHUNK_STATUS_WAITING, CHUNK_STATUS_PAUSED, CHUNK_STATUS_TIMEOUT)


class TransferBatchService(BaseModuleService):
    MODULE = "transfer_batch"
    ENTITY = "传输批次"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["批次编号", "窗口编号", "目标编号", "记录条数", "字节总量", "分片大小", "传输状态", "批次水位"]
    REQUIRED_FIELDS = ["批次编号", "窗口编号", "目标编号"]
    STATUS_ORDER = ["草稿", "待传输", "传输中", "已完成", "已失败"]
    ACTION_RULES = {"启动传输": "传输中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def create_batch(self, window_code: str, record_count: int, target_code: str = "TARG-0001") -> dict[str, Any]:
        """基于抽取窗口创建传输批次，传入产物文件的 record_count 并写入批次事件日志。"""
        batch_code = f"TRAN-{window_code}-{int(time.time())}"
        values = {
            "批次编号": batch_code,
            "窗口编号": window_code,
            "目标编号": target_code,
            "记录条数": str(record_count),
            "字节总量": "0",
            "分片大小": "1000",
            "传输状态": "草稿",
            "批次水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建传输批次失败: {missing}")
        # 写入批次创建事件
        self._write_jsonl(
            self._data_dir("events") / "batch_create_log.jsonl",
            [{"action": "create_batch", "batch_code": batch_code, "window_code": window_code,
              "record_count": record_count, "target": target_code, "at": time.time()}]
        )
        return {"batch_code": batch_code, "batch_id": entry["id"], "window_code": window_code,
                "record_count": record_count, "target_code": target_code}

    def start_transfer(self, batch_id: int, chunk_size: int = 3) -> dict[str, Any]:
        """启动传输批次执行：读取抽取窗口产物文件，按分片大小写入传输产物文件并推进水位。

        本方法执行实际的批量数据传输：从抽取产物文件读取数据，切分为分片，
        写入传输产物文件（jsonl），计算字节总量，然后自动触发暂存落地和校验核对。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}

        entry, msg = self.run_action(batch_id, "启动传输")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}

        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        # 读取上游抽取窗口的产物文件
        artifact_dir = self._data_dir("extracted")
        source_file = artifact_dir / f"{window_code}.jsonl" if window_code != "unknown" else None

        records = []
        if source_file and source_file.exists():
            records = self._read_jsonl(source_file)
        else:
            # 如果上游产物文件不存在，生成模拟数据
            records = self._simulate_records(batch_code, count=int(batch_entry.get("记录条数", 10)),
                                              field_prefix="trans")

        # 分片写入传输产物文件
        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        total_bytes = 0
        total_written = 0
        chunks = []
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            for rec in chunk:
                rec["batch_code"] = batch_code
                rec["chunk_index"] = i // chunk_size
                rec["transferred_at"] = time.time()
            self._write_jsonl(output_file, chunk)
            chunk_bytes = sum(len(str(r).encode("utf-8")) for r in chunk)
            chunks.append({"chunk_index": i // chunk_size, "record_count": len(chunk),
                           "bytes": chunk_bytes})
            total_bytes += chunk_bytes
            total_written += len(chunk)

        # 更新批次水位和字节总量
        entry["字节总量"] = str(total_bytes)
        entry["记录条数"] = str(total_written)
        entry["批次水位"] = str(total_written)

        # 写入传输水位断点
        self._write_checkpoint(f"batch_{batch_code}", {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunk_count": len(chunks),
            "watermark": str(total_written),
            "last_transfer_at": time.time(),
        })

        result = {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunks": len(chunks),
            "artifact_path": str(output_file),
        }

        # 传输完成后自动触发暂存落地
        try:
            sa_svc = self._safe_import("app.services.staging_area", "StagingAreaService")()
            staging_result = sa_svc.land_batch(batch_code, total_written)
            result["auto_staging"] = staging_result
        except Exception as e:
            result["auto_staging"] = {"error": str(e)}

        # 传输完成后自动创建校验核对任务
        try:
            cv_svc = self._safe_import("app.services.checksum_verify", "ChecksumVerifyService")()
            verify_result = cv_svc.create_verification(batch_code, total_written)
            result["auto_verify"] = verify_result
        except Exception as e:
            result["auto_verify"] = {"error": str(e)}

        # 传输完成后自动创建到达对账单
        try:
            ar_svc = self._safe_import("app.services.arrival_reconcile", "ArrivalReconcileService")()
            reconcile_result = ar_svc.create_reconciliation(batch_code, total_written)
            result["auto_reconcile"] = reconcile_result
        except Exception as e:
            result["auto_reconcile"] = {"error": str(e)}

        return result

    def mark_completed(self, batch_id: int) -> dict[str, Any]:
        """标记传输批次已完成，写入完成事件日志并记录批次汇总统计。"""
        entry, msg = self.run_action(batch_id, "标记完成")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        # 从产物文件验证批次完整性
        artifact_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "batch_complete_log.jsonl",
            [{"action": "complete", "batch_code": batch_code, "record_count": len(records),
              "checksum": checksum, "completed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已完成", "record_count": len(records),
                "checksum": checksum}

    def mark_failed(self, batch_id: int, reason: str = "") -> dict[str, Any]:
        """标记传输批次失败，写入失败事件日志并记录中断原因。"""
        entry, msg = self.run_action(batch_id, "标记失败")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        self._write_jsonl(
            self._data_dir("events") / "batch_fail_log.jsonl",
            [{"action": "fail", "batch_code": batch_code, "reason": reason, "failed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已失败", "reason": reason}

    # ------------------------------------------------------------------
    # 有效通道判定单驱动的批次分片调度（判定单 ↔ 分片稳定引用）
    # ------------------------------------------------------------------

    def _plan_path(self, batch_code: str):
        return self._data_dir("dispatches") / f"{batch_code}_plan.json"

    def _delivered_path(self, batch_code: str):
        return self._data_dir("dispatches") / f"{batch_code}_delivered.jsonl"

    def _load_plan(self, batch_code: str) -> dict[str, Any]:
        """读取批次调度计划；计划文件缺失视为尚未做发送前准备。"""
        plan_file = self._plan_path(batch_code)
        if not plan_file.exists():
            raise ValueError(f"批次 {batch_code} 尚未完成发送前准备，缺少调度计划")
        with open(plan_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_plan(self, plan: dict[str, Any]) -> None:
        plan_file = self._plan_path(plan["batch_code"])
        plan["updated_at"] = time.time()
        with open(plan_file, "w", encoding="utf-8") as f:
            json.dump(plan, f, ensure_ascii=False, indent=2, default=str)

    def prepare_dispatch(
        self,
        batch_id: int,
        channel_id: int,
        *,
        chunk_count: int = 2,
        chunk_seconds: list[int] | None = None,
        chunk_sizes: list[int] | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """发送前准备：按通道实际时限逐片判定并生成调度计划，计划给每个分片分配发送范围。

        流程：读取（或模拟生成）批次记录 → 切分分片并分配记录范围 → 每片发送前按
        当前通道时限出具有效通道判定单 → 落调度计划。判定单 ID 与分片一一绑定，
        之后无论通道时限如何变化，每片都能回查当时生效的时限、规则版本与判定结论。
        """
        now = time.time() if now is None else now
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}
        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        source_file = self._data_dir("extracted") / f"{window_code}.jsonl"
        if source_file.exists():
            records = self._read_jsonl(source_file)
        else:
            records = self._simulate_records(
                batch_code,
                count=int(batch_entry.get("记录条数") or 10),
                field_prefix="dispatch",
            )
        # 全量源记录写批次产物，作为该批次的数据基线（不等于暂存确认的交付范围）
        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        for rec in records:
            rec["batch_code"] = batch_code
        self._write_jsonl(output_file, records)

        total = len(records)
        sizes = list(chunk_sizes) if chunk_sizes else None
        if sizes:
            if sum(sizes) != total:
                return {"error": f"分片大小之和 {sum(sizes)} 与批次记录数 {total} 不一致"}
        else:
            base, rem = divmod(total, chunk_count)
            sizes = [base + (1 if i < rem else 0) for i in range(chunk_count)]
        if chunk_seconds is None:
            chunk_seconds = [0] * len(sizes)
        if len(chunk_seconds) != len(sizes):
            return {"error": "预计耗时列表长度必须与分片数一致"}

        verdict_svc = self._safe_import(
            "app.services.channel_verdict", "ChannelVerdictService"
        )()
        chunks: list[dict[str, Any]] = []
        offset = 0
        for index, size in enumerate(sizes):
            line_start, line_end = offset + 1, offset + size
            verdict = verdict_svc.issue_verdict(
                batch_code, channel_id, int(chunk_seconds[index]),
                chunk_index=index, now=now,
            )
            chunks.append({
                "chunk_index": index,
                "line_start": line_start,
                "line_end": line_end,
                "record_count": size,
                "estimated_seconds": int(chunk_seconds[index]),
                "status": CHUNK_STATUS_WAITING,
                "verdict_id": verdict["verdict_id"],
                "verdict_code": verdict["verdict_code"],
                "rule_version": verdict["rule_version"],
                "deadline_seconds": verdict["deadline_seconds"],
                "delivered": False,
                "result": None,
                "remark": "判定单有效，等待发送" if verdict["valid"]
                else "判定单失效：" + "；".join(verdict["reasons"]),
            })
            offset += size

        plan = {
            "batch_code": batch_code,
            "batch_id": batch_id,
            "channel_id": channel_id,
            "total_records": total,
            "chunks": chunks,
            "created_at": now,
            "updated_at": now,
        }
        self._save_plan(plan)

        # 批次进入传输中，水位初始为 0（尚无真正交付的片）
        self.run_action(batch_id, "启动传输")
        batch_entry["批次水位"] = "0"
        self._write_checkpoint(f"dispatch_{batch_code}", {
            "batch_code": batch_code,
            "channel_id": channel_id,
            "chunk_count": len(chunks),
            "delivered_chunks": 0,
            "watermark": 0,
            "prepared_at": now,
        })
        return {"batch_code": batch_code, "plan": plan}

    def get_dispatch_plan(self, batch_code: str) -> dict[str, Any]:
        """回查批次调度计划与各分片状态（含每片绑定的判定单编号与规则版本）。"""
        return self._load_plan(batch_code)

    def _chunk_records(self, batch_code: str, chunk: dict[str, Any]) -> list[dict[str, Any]]:
        records = self._read_jsonl(self._data_dir("batches") / f"{batch_code}.jsonl")
        start, end = chunk["line_start"], chunk["line_end"]
        return [dict(rec) for rec in records[start - 1:end]]

    def _append_chunk_event(self, plan: dict[str, Any], event: dict[str, Any]) -> None:
        event["at"] = time.time()
        self._write_jsonl(
            self._data_dir("dispatches") / f"{plan['batch_code']}_chunk_events.jsonl", [event]
        )

    def _delivered_count(self, plan: dict[str, Any]) -> int:
        return sum(c["record_count"] for c in plan["chunks"] if c["delivered"])

    def _refresh_watermark(self, plan: dict[str, Any]) -> int:
        delivered = self._delivered_count(plan)
        batch_entry = store.find(self.MODULE, plan["batch_id"])
        if batch_entry is not None:
            batch_entry["批次水位"] = str(delivered)
        self._write_checkpoint(f"dispatch_{plan['batch_code']}", {
            "batch_code": plan["batch_code"],
            "channel_id": plan["channel_id"],
            "chunk_count": len(plan["chunks"]),
            "delivered_chunks": sum(1 for c in plan["chunks"] if c["delivered"]),
            "watermark": delivered,
        })
        return delivered

    def send_chunk(
        self, batch_code: str, chunk_index: int, *, now: float | None = None
    ) -> dict[str, Any]:
        """提交并发送一个分片：发送前按通道当前时限重新判定，观察实际发送或等待状态。

        - 当前规则下判定有效：分片真正交付，写入只含已交付片的交付产物，状态置“已发送”；
        - 当前规则下判定失效（如时限缩短后装不下预计耗时）：分片留在队列“等待中”，
          不产生交付记录，同时出具新的失效判定单供回查；
        - 已暂停/已超时的片须先走恢复流程，已发送的片不允许重复发送。
        """
        now = time.time() if now is None else now
        plan = self._load_plan(batch_code)
        chunk = next((c for c in plan["chunks"] if c["chunk_index"] == chunk_index), None)
        if chunk is None:
            return {"error": f"批次 {batch_code} 不存在分片 {chunk_index}"}
        if chunk["status"] == CHUNK_STATUS_SENT:
            return {"error": f"分片 {chunk_index} 已发送交付，不能重复发送", "chunk": chunk}
        if chunk["status"] in RESUMABLE_STATUSES:
            return {
                "error": f"分片 {chunk_index} 处于{chunk['status']}，须先恢复到等待位置才能再发送",
                "chunk": chunk,
            }

        # 发送前按当前实际时限重新判定：通道规则变更会在这张新判定单上体现
        verdict_svc = self._safe_import(
            "app.services.channel_verdict", "ChannelVerdictService"
        )()
        verdict = verdict_svc.issue_verdict(
            batch_code, plan["channel_id"], chunk["estimated_seconds"],
            chunk_index=chunk_index, now=now,
        )
        chunk["verdict_id"] = verdict["verdict_id"]
        chunk["verdict_code"] = verdict["verdict_code"]
        chunk["rule_version"] = verdict["rule_version"]
        chunk["deadline_seconds"] = verdict["deadline_seconds"]

        if not verdict["valid"]:
            chunk["status"] = CHUNK_STATUS_WAITING
            chunk["remark"] = "当前规则判定失效，继续排队：" + "；".join(verdict["reasons"])
            self._save_plan(plan)
            self._append_chunk_event(plan, {
                "action": "send_blocked", "chunk_index": chunk_index,
                "verdict_code": verdict["verdict_code"],
                "rule_version": verdict["rule_version"],
                "deadline_seconds": verdict["deadline_seconds"],
            })
            return {"batch_code": batch_code, "chunk": chunk, "verdict": verdict,
                    "state": "等待中", "delivered": False}

        # 判定有效：真正交付。只把已交付片追加到交付产物，暂存侧只确认这里的记录
        chunk_records = self._chunk_records(batch_code, chunk)
        for rec in chunk_records:
            rec["chunk_index"] = chunk_index
            rec["verdict_code"] = verdict["verdict_code"]
            rec["rule_version"] = verdict["rule_version"]
            rec["delivered_at"] = now
        self._write_jsonl(self._delivered_path(batch_code), chunk_records)

        chunk["status"] = CHUNK_STATUS_SENT
        chunk["delivered"] = True
        chunk["result"] = {
            "state": "已发送",
            "verdict_code": verdict["verdict_code"],
            "rule_version": verdict["rule_version"],
            "deadline_seconds": verdict["deadline_seconds"],
            "estimated_seconds": chunk["estimated_seconds"],
            "records_delivered": len(chunk_records),
            "delivered_at": now,
        }
        chunk["remark"] = f"已按规则版本{verdict['rule_version']}交付 {len(chunk_records)} 条"
        self._save_plan(plan)
        self._append_chunk_event(plan, {
            "action": "send", "chunk_index": chunk_index,
            "verdict_code": verdict["verdict_code"],
            "rule_version": verdict["rule_version"],
            "records_delivered": len(chunk_records),
        })
        watermark = self._refresh_watermark(plan)

        all_done = all(c["status"] == CHUNK_STATUS_SENT for c in plan["chunks"])
        if all_done:
            batch_entry = self.get_entry(plan["batch_id"])
            if batch_entry is not None:
                self.run_action(plan["batch_id"], "标记完成")
                batch_entry["批次水位"] = str(watermark)
        return {"batch_code": batch_code, "chunk": chunk, "verdict": verdict,
                "state": "已发送", "delivered": True, "watermark": watermark}

    def pause_chunk(self, batch_code: str, chunk_index: int, reason: str = "") -> dict[str, Any]:
        """暂停排队中的分片：保留其在计划中的发送位置，并登记续传断点。"""
        plan = self._load_plan(batch_code)
        chunk = next((c for c in plan["chunks"] if c["chunk_index"] == chunk_index), None)
        if chunk is None:
            return {"error": f"批次 {batch_code} 不存在分片 {chunk_index}"}
        if chunk["status"] != CHUNK_STATUS_WAITING:
            return {"error": f"只有等待中的分片可暂停，当前状态为{chunk['status']}", "chunk": chunk}

        chunk["status"] = CHUNK_STATUS_PAUSED
        chunk["remark"] = f"暂停保留续传位置：{reason or '人工暂停'}"
        self._save_plan(plan)
        self._append_chunk_event(plan, {
            "action": "pause", "chunk_index": chunk_index, "reason": reason,
        })
        resume = self._register_resume_point(plan, chunk, "暂停中断", reason)
        return {"batch_code": batch_code, "chunk": chunk, "resume_point": resume}

    def timeout_chunk(self, batch_code: str, chunk_index: int, reason: str = "") -> dict[str, Any]:
        """将分片标记为发送超时：保留发送位置并登记超时类续传断点，已交付的片不受影响。"""
        plan = self._load_plan(batch_code)
        chunk = next((c for c in plan["chunks"] if c["chunk_index"] == chunk_index), None)
        if chunk is None:
            return {"error": f"批次 {batch_code} 不存在分片 {chunk_index}"}
        if chunk["status"] == CHUNK_STATUS_SENT:
            return {"error": "已交付的分片不能再标记超时", "chunk": chunk}

        chunk["status"] = CHUNK_STATUS_TIMEOUT
        chunk["delivered"] = False
        chunk["remark"] = f"超时保留续传位置：{reason or '发送超过通道时限'}"
        self._save_plan(plan)
        self._append_chunk_event(plan, {
            "action": "timeout", "chunk_index": chunk_index, "reason": reason,
        })
        resume = self._register_resume_point(plan, chunk, "超时中断", reason)
        return {"batch_code": batch_code, "chunk": chunk, "resume_point": resume}

    def resume_chunk(
        self, batch_code: str, chunk_index: int, *, now: float | None = None
    ) -> dict[str, Any]:
        """恢复暂停或超时的分片：从保留的发送位置重新进入队列，并按当前规则重新判定发送。"""
        plan = self._load_plan(batch_code)
        chunk = next((c for c in plan["chunks"] if c["chunk_index"] == chunk_index), None)
        if chunk is None:
            return {"error": f"批次 {batch_code} 不存在分片 {chunk_index}"}
        if chunk["status"] not in RESUMABLE_STATUSES:
            return {"error": f"分片当前状态为{chunk['status']}，无需恢复", "chunk": chunk}

        previous_status = chunk["status"]
        chunk["status"] = CHUNK_STATUS_WAITING
        chunk["remark"] = f"已从{previous_status}恢复到原发送位置，等待重新判定"
        self._save_plan(plan)
        self._append_chunk_event(plan, {
            "action": "resume", "chunk_index": chunk_index, "from_status": previous_status,
        })
        # 恢复后立即按当前实际时限重新判定并尝试发送
        send_result = self.send_chunk(batch_code, chunk_index, now=now)
        send_result["resumed_from"] = previous_status
        return send_result

    def _register_resume_point(
        self, plan: dict[str, Any], chunk: dict[str, Any],
        point_type: str, reason: str,
    ) -> dict[str, Any]:
        """在断点续传模块登记一条与分片绑定的续传断点，恢复位置即分片在计划中的序号。"""
        try:
            rp_svc = self._safe_import(
                "app.services.resume_point", "ResumePointService"
            )()
            completed = sum(1 for c in plan["chunks"] if c["status"] == CHUNK_STATUS_SENT)
            pending = len(plan["chunks"]) - completed
            resume = rp_svc.create_dispatch_resume(
                plan["batch_code"], chunk["chunk_index"],
                completed, pending, point_type, reason,
            )
            return resume
        except Exception as exc:  # 断点登记失败不应阻断分片状态流转
            return {"error": str(exc)}

    def sync_delivery_to_staging_and_reconcile(self, batch_code: str) -> dict[str, Any]:
        """把真正交付的分片范围同步给暂存落地和到达对账，两侧均以判定单与交付产物为准核对。"""
        plan = self._load_plan(batch_code)
        expected = plan["total_records"]
        delivered = self._delivered_count(plan)
        result: dict[str, Any] = {
            "batch_code": batch_code,
            "expected": expected,
            "delivered": delivered,
            "chunks": [
                {
                    "chunk_index": c["chunk_index"],
                    "status": c["status"],
                    "verdict_code": c["verdict_code"],
                    "rule_version": c["rule_version"],
                    "delivered": c["delivered"],
                }
                for c in plan["chunks"]
            ],
        }
        try:
            sa_svc = self._safe_import(
                "app.services.staging_area", "StagingAreaService"
            )()
            result["auto_staging"] = sa_svc.land_delivered_chunks(batch_code)
        except Exception as exc:
            result["auto_staging"] = {"error": str(exc)}
        try:
            ar_svc = self._safe_import(
                "app.services.arrival_reconcile", "ArrivalReconcileService"
            )()
            recon = ar_svc.create_reconciliation(batch_code, expected)
            result["auto_reconcile"] = ar_svc.reconcile_by_verdict(recon["recon_id"])
        except Exception as exc:
            result["auto_reconcile"] = {"error": str(exc)}
        return result
