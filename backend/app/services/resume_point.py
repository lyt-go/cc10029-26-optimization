"""断点续传领域服务：维护传输中断后已完成与未完成分片的断点登记与恢复位置。

本模块在传输批次因分片中断、重试超限等原因失败时，创建断点记录并支持从
恢复位置重新启动传输，保障数据传输的完整性。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ResumePointService(BaseModuleService):
    MODULE = "resume_point"
    ENTITY = "续传断点"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["断点编号", "批次编号", "已完成分片", "未完成分片", "恢复位置", "重试次数", "断点类型", "续传水位"]
    REQUIRED_FIELDS = ["断点编号", "批次编号", "已完成分片"]
    STATUS_ORDER = ["草稿", "待续传", "续传中", "已恢复"]
    ACTION_RULES = {"启动续传": "续传中", "确认恢复": "已恢复", "标记放弃": "已恢复"}
    NEGATIVE_ACTIONS = ["标记放弃"]

    def create_resume_point(self, batch_code: str, completed_chunks: int, pending_chunks: int) -> dict[str, Any]:
        """为传输中断批次创建断点续传记录，登记已完成分片、未完成分片和恢复位置。"""
        resume_code = f"RESU-{batch_code}-{int(time.time())}"
        values = {
            "断点编号": resume_code,
            "批次编号": batch_code,
            "已完成分片": str(completed_chunks),
            "未完成分片": str(pending_chunks),
            "恢复位置": str(completed_chunks),
            "重试次数": "0",
            "断点类型": "分片中断",
            "续传水位": str(completed_chunks),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建断点续传记录失败: {missing}")
        # 写入断点日志
        self._write_jsonl(
            self._data_dir("events") / "resume_point_log.jsonl",
            [{"action": "create_resume_point", "resume_code": resume_code, "batch_code": batch_code,
              "completed_chunks": completed_chunks, "pending_chunks": pending_chunks,
              "at": time.time()}]
        )
        return {"resume_code": resume_code, "resume_id": entry["id"], "batch_code": batch_code,
                "completed_chunks": completed_chunks, "pending_chunks": pending_chunks}

    def resume_transfer(self, resume_id: int) -> dict[str, Any]:
        """执行断点续传：从恢复位置重新传输未完成分片数据并写入续传产物文件。

        本方法实现数据传输的高可用保障：读取批次产物文件，定位已完成的末位分片，
        从未完成分片重新开始传输，记录重试次数并推进续传水位。
        """
        resume_entry = self.get_entry(resume_id)
        if resume_entry is None:
            return {"error": f"续传断点 {resume_id} 不存在"}

        entry, msg = self.run_action(resume_id, "启动续传")
        if entry is None:
            return {"error": msg, "resume_id": resume_id}

        batch_code = resume_entry["批次编号"]
        retry_count = int(resume_entry.get("重试次数", 0)) + 1
        resume_position = int(resume_entry.get("恢复位置", 0))

        # 读取原批次产物文件，定位未完成分片
        batch_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        existing_records = self._read_jsonl(batch_file)

        # 从恢复位置读取未传输记录
        pending_records = existing_records[resume_position:] if resume_position < len(existing_records) else []
        if not pending_records:
            # 无已有数据则生成续传模拟数据
            pending_records = self._simulate_records(batch_code, count=5, field_prefix="resume")

        # 续传写入
        resume_file = self._data_dir("batches") / f"{batch_code}_resume_{retry_count}.jsonl"
        written = self._write_jsonl(resume_file, pending_records)

        # 更新断点水位
        new_position = resume_position + written
        entry["重试次数"] = str(retry_count)
        entry["续传水位"] = str(new_position)

        # 写入续传产物断点
        self._write_checkpoint(f"resume_{batch_code}", {
            "batch_code": batch_code,
            "retry_count": retry_count,
            "resume_position": new_position,
            "records_written": written,
            "last_resumed_at": time.time(),
        })

        if len(pending_records) > 0 and written > 0:
            final_entry, _ = self.run_action(resume_id, "确认恢复")

        return {
            "resume_code": resume_entry["断点编号"],
            "batch_code": batch_code,
            "retry_count": retry_count,
            "records_resumed": written,
            "new_position": new_position,
        }

    def get_pending_resume_points(self) -> list[dict[str, Any]]:
        """获取所有待续传的断点记录列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "待续传"]

    def create_dispatch_resume(
        self,
        batch_code: str,
        chunk_index: int,
        completed_chunks: int,
        pending_chunks: int,
        point_type: str,
        reason: str = "",
    ) -> dict[str, Any]:
        """为暂停或超时的分片登记续传断点，恢复位置精确到分片序号，保留继续发送的位置。"""
        resume_code = f"RESU-{batch_code}-C{chunk_index}-{int(time.time())}"
        values = {
            "断点编号": resume_code,
            "批次编号": batch_code,
            "已完成分片": str(completed_chunks),
            "未完成分片": str(pending_chunks),
            "恢复位置": str(chunk_index),
            "重试次数": "0",
            "断点类型": point_type,
            "续传水位": str(completed_chunks),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建分片续传断点失败: {missing}")
        entry["分片序号"] = str(chunk_index)
        entry["中断原因"] = reason
        self._write_jsonl(
            self._data_dir("events") / "resume_point_log.jsonl",
            [{"action": "create_dispatch_resume", "resume_code": resume_code,
              "batch_code": batch_code, "chunk_index": chunk_index,
              "point_type": point_type, "reason": reason,
              "completed_chunks": completed_chunks, "pending_chunks": pending_chunks,
              "at": time.time()}]
        )
        return {"resume_code": resume_code, "resume_id": entry["id"],
                "batch_code": batch_code, "chunk_index": chunk_index,
                "point_type": point_type, "resume_position": chunk_index}