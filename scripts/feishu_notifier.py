#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书通知模块 - 用于任务进度推送
"""

import json, requests, time, os
from datetime import datetime
from typing import Optional


class FeishuNotifier:
    """飞书群消息通知器"""

    # 默认配置
    DEFAULT_APP_ID = "cli_a947bdb243a19ccc"
    DEFAULT_APP_SECRET = "dYlOLoRwDE5zKVptcKCmXbAAC26REjEK"
    DEFAULT_CHAT_ID = "oc_dc2f188c805062233726079221cfebb1"
    TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    MSG_URL = "https://open.feishu.cn/open-apis/im/v1/messages"
    # 飞书限制: 100次/分钟, 5次/秒
    MIN_INTERVAL = 0.25

    def __init__(
        self,
        app_id: str = None,
        app_secret: str = None,
        chat_id: str = None,
        task_name: str = "未命名任务",
        enabled: bool = True,
    ):
        self.app_id = app_id or self.DEFAULT_APP_ID
        self.app_secret = app_secret or self.DEFAULT_APP_SECRET
        self.chat_id = chat_id or self.DEFAULT_CHAT_ID
        self.task_name = task_name
        self.enabled = enabled
        self._token = None
        self._token_expire = 0
        self._last_send_time = 0

    def _get_token(self) -> str:
        """获取或刷新 tenant_access_token"""
        if self._token and time.time() < self._token_expire:
            return self._token

        try:
            resp = requests.post(
                self.TOKEN_URL,
                json={"app_id": self.app_id, "app_secret": self.app_secret},
                timeout=10,
            )
            data = resp.json()
            if data.get("code") != 0:
                print(f"[通知] 获取token失败: {data.get('msg')}")
                return None
            self._token = data["tenant_access_token"]
            self._token_expire = time.time() + data.get("expire", 7200) - 60
            return self._token
        except Exception as e:
            print(f"[通知] 获取token异常: {e}")
            return None

    def send(self, msg_type: str = "text", content: dict = None, **kwargs) -> bool:
        """发送消息到飞书群

        Args:
            msg_type: 消息类型，支持 text / interactive / post
            content: 消息内容字典
            **kwargs: 额外参数

        Returns:
            bool: 是否发送成功
        """
        if not self.enabled:
            return False

        # 频率限制
        elapsed = time.time() - self._last_send_time
        if elapsed < self.MIN_INTERVAL:
            time.sleep(self.MIN_INTERVAL - elapsed)

        token = self._get_token()
        if not token:
            return False

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        payload = {
            "receive_id": self.chat_id,
            "msg_type": msg_type,
            "content": json.dumps(content, ensure_ascii=False) if isinstance(content, dict) else content,
        }

        try:
            resp = requests.post(
                self.MSG_URL,
                headers=headers,
                params={"receive_id_type": "chat_id"},
                json=payload,
                timeout=10,
            )
            result = resp.json()
            if result.get("code") != 0:
                print(f"[通知] 发送失败: {result.get('msg')}")
                return False
            self._last_send_time = time.time()
            return True
        except Exception as e:
            print(f"[通知] 发送异常: {e}")
            return False

    def send_text(self, text: str) -> bool:
        """发送纯文本消息"""
        return self.send("text", {"text": text})

    def send_card(self, title: str, elements: list) -> bool:
        """发送交互式卡片消息

        Args:
            title: 卡片标题
            elements: 卡片元素列表，每个元素为 dict
                文本元素: {"tag": "markdown", "content": "**加粗**文本"}
                分割线: {"tag": "hr"}
                字段组: {"tag": "column_set", "flex_mode": "bisect", "background_style": "default", "columns": [...]}
        """
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue",
            },
            "elements": elements,
        }
        return self.send("interactive", card)

    # ========== 预设通知模板 ==========

    def notify_start(self, total: int, description: str = "") -> bool:
        """任务开始通知"""
        now = datetime.now().strftime("%H:%M:%S")
        text = f"[{self.task_name}] 任务开始\n时间: {now}\n总任务数: {total}"
        if description:
            text += f"\n说明: {description}"
        return self.send_card(
            f"任务开始 - {self.task_name}",
            [
                {"tag": "markdown", "content": f"**任务名称:** {self.task_name}"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**开始时间:** {now}"},
                {"tag": "markdown", "content": f"**总任务数:** {total}"},
                {"tag": "markdown", "content": f"**说明:** {description or '无'}"},
            ],
        )

    def notify_progress(
        self,
        current: int,
        total: int,
        success: int,
        failed: int,
        skipped: int = 0,
        elapsed_min: float = 0,
        eta_min: float = None,
        current_item: str = "",
    ) -> bool:
        """阶段进度通知"""
        now = datetime.now().strftime("%H:%M:%S")
        pct = current / total * 100 if total else 0
        eta_str = f"约{eta_min:.0f}分钟" if eta_min else "计算中..."

        # 进度条（20格）
        filled = int(pct / 5)
        bar = "█" * filled + "░" * (20 - filled)

        return self.send_card(
            f"进度报告 - {self.task_name}",
            [
                {"tag": "markdown", "content": f"**{bar} {pct:.1f}%**"},
                {"tag": "hr"},
                {"tag": "column_set", "flex_mode": "bisect", "background_style": "default",
                 "columns": [
                     {"tag": "column", "width": "weighted", "weight": 1,
                      "elements": [{"tag": "markdown", "content": f"**已完成:** {current}/{total}"}]},
                     {"tag": "column", "width": "weighted", "weight": 1,
                      "elements": [{"tag": "markdown", "content": f"**成功:** {success}"}]},
                 ]},
                {"tag": "column_set", "flex_mode": "bisect", "background_style": "default",
                 "columns": [
                     {"tag": "column", "width": "weighted", "weight": 1,
                      "elements": [{"tag": "markdown", "content": f"**失败:** {failed}"}]},
                     {"tag": "column", "width": "weighted", "weight": 1,
                      "elements": [{"tag": "markdown", "content": f"**跳过:** {skipped}"}]},
                 ]},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**已耗时:** {elapsed_min:.1f}分钟  |  **预计剩余:** {eta_str}"},
                {"tag": "markdown", "content": f"**当前处理:** {current_item or '-'}"},
                {"tag": "markdown", "content": f"<font color='grey'>更新时间: {now}</font>"},
            ],
        )

    def notify_error(
        self,
        item: str,
        error: str,
        is_critical: bool = False,
        consecutive_count: int = 1,
    ) -> bool:
        """异常告警通知"""
        now = datetime.now().strftime("%H:%M:%S")
        level = "⚠️ 严重" if is_critical else "ℹ️ 警告"

        template_color = "red" if is_critical else "orange"

        elements = [
            {"tag": "markdown", "content": f"**级别:** {level}"},
            {"tag": "hr"},
            {"tag": "markdown", "content": f"**任务项:** {item}"},
            {"tag": "markdown", "content": f"**错误信息:** {error[:200]}"},
        ]
        if consecutive_count > 1:
            elements.append(
                {"tag": "markdown", "content": f"**连续失败:** {consecutive_count}次"}
            )
        elements.append({"tag": "markdown", "content": f"<font color='grey'>{now}</font>"})

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"异常告警 - {self.task_name}"},
                "template": template_color,
            },
            "elements": elements,
        }
        return self.send("interactive", card)

    def notify_complete(
        self,
        total: int,
        success: int,
        failed: int,
        skipped: int,
        elapsed_min: float,
        output_path: str = "",
        extra_info: str = "",
    ) -> bool:
        """任务完成汇总通知"""
        now = datetime.now().strftime("%H:%M:%S")
        template_color = "green" if failed == 0 else ("orange" if failed < total * 0.1 else "red")

        elements = [
            {"tag": "markdown", "content": f"**任务:** {self.task_name}"},
            {"tag": "hr"},
            {"tag": "column_set", "flex_mode": "bisect", "background_style": "default",
             "columns": [
                 {"tag": "column", "width": "weighted", "weight": 1,
                  "elements": [{"tag": "markdown", "content": f"**总计:** {total}"}]},
                 {"tag": "column", "width": "weighted", "weight": 1,
                  "elements": [{"tag": "markdown", "content": f"**成功:** {success}"}]},
             ]},
            {"tag": "column_set", "flex_mode": "bisect", "background_style": "default",
             "columns": [
                 {"tag": "column", "width": "weighted", "weight": 1,
                  "elements": [{"tag": "markdown", "content": f"**失败:** {failed}"}]},
                 {"tag": "column", "width": "weighted", "weight": 1,
                  "elements": [{"tag": "markdown", "content": f"**跳过:** {skipped}"}]},
             ]},
            {"tag": "hr"},
            {"tag": "markdown", "content": f"**总耗时:** {elapsed_min:.1f}分钟"},
        ]
        if output_path:
            elements.append({"tag": "markdown", "content": f"**输出目录:** `{output_path}`"})
        if extra_info:
            elements.append({"tag": "markdown", "content": extra_info})
        elements.append({"tag": "markdown", "content": f"<font color='grey'>完成时间: {now}</font>"})

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"任务完成 - {self.task_name}"},
                "template": template_color,
            },
            "elements": elements,
        }
        return self.send("interactive", card)


if __name__ == "__main__":
    # 测试
    n = FeishuNotifier(task_name="测试通知")
    print("发送测试消息...")
    n.send_text("飞书通知模块测试成功。")
    print("完成")
