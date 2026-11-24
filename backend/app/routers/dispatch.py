"""通道判定驱动的批次分片调度接口。

与通用资源路由并列注册，承载发送前判定、分片发送/暂停/超时/恢复、
交付范围同步落地与按判定单对账等跨模块编排动作。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.channel_config import ChannelConfigService
from app.services.channel_verdict import ChannelVerdictService
from app.services.transfer_batch import TransferBatchService

router = APIRouter(prefix="/api/dispatch", tags=["通道判定与分片调度"])


class PreparePayload(BaseModel):
    batch_id: int
    channel_id: int
    chunk_count: int = Field(default=2, ge=1, le=100)
    chunk_seconds: list[int] | None = None
    chunk_sizes: list[int] | None = None


class ChunkPayload(BaseModel):
    batch_code: str
    chunk_index: int = Field(ge=0)
    reason: str = ""


class TimeoutPayload(BaseModel):
    timeout_seconds: int = Field(gt=0)


def _unwrap(result: dict[str, Any]) -> dict[str, Any]:
    """统一把领域服务返回中的 error 转成 400，成功结果原样返回。"""
    if "error" in result:
        raise HTTPException(status_code=400, detail=str(result["error"]))
    return result


@router.post("/prepare")
def prepare_dispatch(payload: PreparePayload) -> dict[str, Any]:
    """发送前准备：按通道实际时限逐片判定，生成有效通道判定单与调度计划。"""
    svc = TransferBatchService()
    result = svc.prepare_dispatch(
        payload.batch_id, payload.channel_id,
        chunk_count=payload.chunk_count,
        chunk_seconds=payload.chunk_seconds,
        chunk_sizes=payload.chunk_sizes,
    )
    return _unwrap(result)


@router.get("/plan/{batch_code}")
def get_plan(batch_code: str) -> dict[str, Any]:
    """回查批次调度计划：每片的判定单编号、生效时限、规则版本与发送状态。"""
    try:
        return TransferBatchService().get_dispatch_plan(batch_code)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/chunk/send")
def send_chunk(payload: ChunkPayload) -> dict[str, Any]:
    """提交并发送分片：按当前通道时限重新判定，返回实际发送或排队等待状态。"""
    return _unwrap(TransferBatchService().send_chunk(
        payload.batch_code, payload.chunk_index))


@router.post("/chunk/pause")
def pause_chunk(payload: ChunkPayload) -> dict[str, Any]:
    """暂停分片，保留其继续发送的位置并登记续传断点。"""
    return _unwrap(TransferBatchService().pause_chunk(
        payload.batch_code, payload.chunk_index, payload.reason))


@router.post("/chunk/timeout")
def timeout_chunk(payload: ChunkPayload) -> dict[str, Any]:
    """标记分片超时，保留发送位置并登记超时类续传断点。"""
    return _unwrap(TransferBatchService().timeout_chunk(
        payload.batch_code, payload.chunk_index, payload.reason))


@router.post("/chunk/resume")
def resume_chunk(payload: ChunkPayload) -> dict[str, Any]:
    """恢复暂停或超时的分片，从保留位置重新判定并继续发送。"""
    return _unwrap(TransferBatchService().resume_chunk(
        payload.batch_code, payload.chunk_index))


@router.post("/sync/{batch_code}")
def sync_delivery(batch_code: str) -> dict[str, Any]:
    """把真正交付的分片范围同步给暂存落地与到达对账核对。"""
    return _unwrap(TransferBatchService().sync_delivery_to_staging_and_reconcile(batch_code))


@router.get("/verdicts/{batch_code}")
def list_verdicts(batch_code: str) -> dict[str, Any]:
    """回查批次全部有效通道判定单（含失效判定与原因、时限、规则版本）。"""
    svc = ChannelVerdictService()
    return {
        "batch_code": batch_code,
        "verdicts": svc.list_for_batch(batch_code),
        "artifacts": svc.read_verdict_artifacts(batch_code),
    }


@router.post("/channels/{channel_id}/timeout")
def update_channel_timeout(channel_id: int, payload: TimeoutPayload) -> dict[str, Any]:
    """变更通道时限并推进规则版本（如 90 秒缩为 45 秒），历史判定单保持不变。"""
    return _unwrap(ChannelConfigService().update_timeout(
        channel_id, payload.timeout_seconds))
