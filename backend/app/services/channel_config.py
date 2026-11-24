"""通道配置领域服务：维护传输通道的协议、并发连接数、重试间隔与加密方式配置。

本模块管理数据交换通道的配置参数，支持按源端目标端配对注册通道，
通道配置变更时写入配置版本快照。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ChannelConfigService(BaseModuleService):
    MODULE = "channel_config"
    ENTITY = "传输通道"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["通道编号", "传输协议", "并发连接数", "重试间隔", "超时秒数", "加密方式", "通道类型", "配置版本"]
    REQUIRED_FIELDS = ["通道编号", "传输协议", "并发连接数"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    def create_channel(self, protocol: str, connections: int, channel_type: str = "源端通道") -> dict[str, Any]:
        """创建传输通道配置：定义传输协议、并发连接数、重试间隔和加密方式。"""
        channel_code = f"CHAN-{protocol}-{int(time.time())}"
        values = {
            "通道编号": channel_code,
            "传输协议": protocol,
            "并发连接数": str(connections),
            "重试间隔": "30",
            "超时秒数": "300",
            "加密方式": "AES256",
            "通道类型": channel_type,
            "配置版本": "v1.0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建传输通道失败: {missing}")
        # 写入通道配置快照
        self._write_jsonl(
            self._data_dir("channels") / f"{channel_code}_config.jsonl",
            [{"action": "create_channel", "channel_code": channel_code, "protocol": protocol,
              "connections": connections, "type": channel_type, "at": time.time()}]
        )
        return entry

    def validate_channel(self, channel_id: int) -> dict[str, Any]:
        """校验传输通道配置的有效性：验证协议格式、连接数范围和加密方式合法性。"""
        channel = self.get_entry(channel_id)
        if channel is None:
            return {"error": f"传输通道 {channel_id} 不存在"}

        protocol = channel.get("传输协议", "")
        connections = int(channel.get("并发连接数", 0))
        encryption = channel.get("加密方式", "")
        channel_type = channel.get("通道类型", "")

        valid_protocols = ["HTTPS", "TCP", "gRPC", "WebSocket"]
        valid_encryptions = ["AES256", "TLS1.3", "AES128"]
        valid_types = ["源端通道", "目标端通道"]

        errors = []
        if protocol not in valid_protocols:
            errors.append(f"不支持的传输协议: {protocol}")
        if connections < 1 or connections > 100:
            errors.append(f"并发连接数超出范围 (1-100): {connections}")
        if encryption not in valid_encryptions:
            errors.append(f"不支持的加密方式: {encryption}")
        if channel_type not in valid_types:
            errors.append(f"不支持的通道类型: {channel_type}")

        valid = len(errors) == 0
        if valid:
            self.run_action(channel_id, "确认生效")
        else:
            self.run_action(channel_id, "提交校验")

        # 写入校验结果到产物文件
        verify_file = self._data_dir("channels") / f"{channel['通道编号']}_verify.jsonl"
        self._write_jsonl(verify_file, [{
            "channel_code": channel["通道编号"],
            "valid": valid,
            "errors": errors,
            "protocol": protocol,
            "connections": connections,
            "encryption": encryption,
            "verified_at": time.time(),
        }])

        return {
            "channel_code": channel["通道编号"],
            "valid": valid,
            "errors": errors,
        }

    def get_active_channels(self) -> list[dict[str, Any]]:
        """获取所有已生效的传输通道配置列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]

    def update_timeout(self, channel_id: int, timeout_seconds: int) -> dict[str, Any]:
        """变更通道时限并推进配置版本，供下一张判定单按新实际时限出具。

        已出具的历史判定单不受影响（不可变），变更后新发送的分片重新判定时会读到
        新时限与新版本，从而在判定单上留下规则版本轨迹。
        """
        channel = self.get_entry(channel_id)
        if channel is None:
            return {"error": f"传输通道 {channel_id} 不存在"}
        if timeout_seconds <= 0:
            return {"error": f"通道时限必须为正整数秒，收到 {timeout_seconds}"}

        old_timeout = channel.get("超时秒数")
        old_version = str(channel.get("配置版本", "v1.0"))
        try:
            major = int(old_version.lstrip("vV").split(".")[0])
        except ValueError:
            major = 1
        new_version = f"v{major + 1}.0"
        channel["超时秒数"] = str(timeout_seconds)
        channel["配置版本"] = new_version

        record = {
            "action": "update_timeout",
            "channel_code": channel["通道编号"],
            "old_timeout": old_timeout,
            "new_timeout": timeout_seconds,
            "old_version": old_version,
            "new_version": new_version,
            "at": time.time(),
        }
        self._write_jsonl(
            self._data_dir("channels") / f"{channel['通道编号']}_config.jsonl", [record]
        )
        return {
            "channel_code": channel["通道编号"],
            "old_timeout": old_timeout,
            "timeout_seconds": timeout_seconds,
            "rule_version": new_version,
        }