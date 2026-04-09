#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书指令监听服务（子进程 WebSocket 模式）

通过飞书官方 SDK 的 WebSocket 长连接，接收卡片按钮回调和群消息。
由于 Jupyter 环境中 asyncio 事件循环冲突，采用子进程方式启动监听服务。
灵犀通过轮询指令文件获取用户指令，实现飞书双向交互。

使用方式：
    from feishu_listener import send_and_wait, start_listener, wait_for_reply, stop_listener
    from feishu_notifier import FeishuNotifier

    notifier = FeishuNotifier(task_name="排非清单提取")
    cmd = send_and_wait(
        notifier,
        "测试结果 - 请确认",
        elements=[...],
        buttons=[
            {"label": "确认全量执行", "action": "confirm"},
            {"label": "修改后重试", "action": "retry"},
        ],
        timeout=300,
    )
    if cmd and cmd["action"] == "confirm":
        # 执行全量
        ...

架构：
    飞书卡片按钮/群消息 → 飞书 WebSocket → 子进程 SDK → 写入指令文件
    灵犀 wait_for_reply() → 轮询指令文件 → 返回指令

前提条件：
    - 飞书开放平台已订阅回调：事件与回调 → 回调配置 → 卡片回传交互（长连接）
    - 飞书开放平台已订阅事件：事件与回调 → 事件配置 → 接收消息（长连接）
    - 飞书开放平台已添加应用能力：机器人
"""

import json
import os
import sys
import time
import subprocess
import shutil
import signal
from typing import Optional

# 指令文件目录
CMD_FILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".feishu_cmd")

# 从 feishu_notifier 导入凭证
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 预设指令映射
ACTION_MAP = {
    "confirm": "confirm",
    "retry": "retry",
    "cancel": "cancel",
    "pause": "pause",
    "resume": "resume",
}

# 全局状态
_ws_process: Optional[subprocess.Popen] = None


def _get_credentials():
    """获取飞书应用凭证"""
    try:
        from feishu_notifier import FeishuNotifier
        n = FeishuNotifier()
        return n.app_id, n.app_secret
    except Exception as e:
        print(f"[飞书监听] 获取凭证失败: {e}")
        return None, None


def _ensure_dir():
    """确保指令目录存在"""
    os.makedirs(CMD_FILE_DIR, exist_ok=True)


def _write_command(action: str, value: str = "", extra: dict = None):
    """写入指令到文件"""
    _ensure_dir()
    cmd = {"action": action, "value": value, "time": time.time()}
    if extra:
        cmd.update(extra)
    filepath = os.path.join(CMD_FILE_DIR, f"cmd_{int(time.time() * 1000)}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(cmd, f, ensure_ascii=False)
    print(f"[飞书监听] 指令已写入: {action} = {value}")


def _read_latest_command(after_time: float = None) -> Optional[dict]:
    """读取最新的指令"""
    if not os.path.exists(CMD_FILE_DIR):
        return None
    cmd_files = sorted(
        [f for f in os.listdir(CMD_FILE_DIR) if f.endswith(".json")],
        key=lambda f: os.path.getmtime(os.path.join(CMD_FILE_DIR, f)),
        reverse=True,
    )
    if not cmd_files:
        return None
    filepath = os.path.join(CMD_FILE_DIR, cmd_files[0])
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            cmd = json.load(f)
        if after_time and cmd.get("time", 0) <= after_time:
            return None
        return cmd
    except Exception:
        return None


def _cleanup_old_commands(keep_count: int = 10):
    """清理旧指令文件"""
    if not os.path.exists(CMD_FILE_DIR):
        return
    cmd_files = sorted(
        [f for f in os.listdir(CMD_FILE_DIR) if f.endswith(".json")],
        key=lambda f: os.path.getmtime(os.path.join(CMD_FILE_DIR, f)),
    )
    for f in cmd_files[:-keep_count]:
        try:
            os.remove(os.path.join(CMD_FILE_DIR, f))
        except Exception:
            pass


def _parse_action(text: str) -> tuple:
    """解析文本为 (action, value)"""
    text_lower = text.lower()
    for keyword, action in ACTION_MAP.items():
        if keyword in text_lower:
            return (action, text)
    return ("custom", text)


def _get_listener_script_path():
    """获取监听子进程脚本路径"""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ".feishu_ws_listener.py")


def _generate_listener_script(app_id: str, app_secret: str):
    """生成独立的 WebSocket 监听脚本"""
    script = f"""#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 飞书 WebSocket 监听子进程（自动生成，请勿手动修改）

import json
import os
import time
import sys

sys.path.insert(0, r"{os.path.dirname(os.path.abspath(__file__))}")

import lark_oapi as lark
from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTrigger, P2CardActionTriggerResponse

APP_ID = "{app_id}"
APP_SECRET = "{app_secret}"
CMD_DIR = r"{CMD_FILE_DIR}"

os.makedirs(CMD_DIR, exist_ok=True)

def write_command(action, value="", extra=None):
    cmd = {{"action": action, "value": value, "time": time.time()}}
    if extra:
        cmd.update(extra)
    filepath = os.path.join(CMD_DIR, f"cmd_{{int(time.time() * 1000)}}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(cmd, f, ensure_ascii=False)
    print(f"[WS] 指令写入: {{action}} = {{value}}", flush=True)

def card_handler(data):
    try:
        operator = data.event.operator
        action_obj = data.event.action
        value = action_obj.value if action_obj and action_obj.value else {{}}
        cmd_action = value.get("action", "confirm")
        cmd_label = value.get("label", cmd_action)
        extra = {{}}
        if operator:
            extra["open_id"] = operator.open_id or ""
            extra["user_id"] = operator.user_id or ""
        write_command(cmd_action, cmd_label, extra)
        print(f"[WS] 卡片回调: {{cmd_label}} -> {{cmd_action}}", flush=True)
    except Exception as e:
        print(f"[WS] 卡片回调异常: {{e}}", flush=True)
        import traceback
        traceback.print_exc()
    return P2CardActionTriggerResponse()

def message_handler(data):
    try:
        msg = data.event.message
        if msg and msg.message_type == "text":
            content = json.loads(msg.content).get("text", "").strip() if msg.content else ""
            if content:
                action_map = {{"确认": "confirm", "重试": "retry", "取消": "cancel", "暂停": "pause", "继续": "resume"}}
                action = "custom"
                for k, v in action_map.items():
                    if k in content:
                        action = v
                        break
                write_command(action, content)
                print(f"[WS] 消息: {{content}} -> {{action}}", flush=True)
    except Exception as e:
        print(f"[WS] 消息处理异常: {{e}}", flush=True)

event_handler = (
    lark.EventDispatcherHandler.builder("", "")
    .register_p2_card_action_trigger(card_handler)
    .register_p2_im_message_receive_v1(message_handler)
    .build()
)

client = lark.ws.Client(APP_ID, APP_SECRET, event_handler=event_handler, log_level=lark.LogLevel.INFO)

print("[WS] 飞书 WebSocket 监听服务启动中...", flush=True)
client.start()
"""
    script_path = _get_listener_script_path()
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(script)
    return script_path


# ========== 公开 API ==========

def start_listener() -> bool:
    """启动飞书指令监听服务（子进程 WebSocket 模式）

    通过启动独立子进程运行飞书 SDK WebSocket 客户端，
    接收卡片按钮回调和群消息，将指令写入本地文件。

    Returns:
        bool: 是否启动成功

    Note:
        - 子进程为 daemon 模式，跟随主进程生命周期
        - 首次调用会生成监听脚本并启动子进程，后续调用跳过（幂等）
        - 前提：飞书开放平台已配置长连接方式的事件/回调订阅
    """
    global _ws_process

    if _ws_process is not None and _ws_process.poll() is None:
        print("[飞书监听] 监听服务已在运行")
        return True

    app_id, app_secret = _get_credentials()
    if not app_id or not app_secret:
        print("[飞书监听] 无法获取应用凭证，启动失败")
        return False

    # 生成监听脚本
    script_path = _generate_listener_script(app_id, app_secret)

    # 启动子进程
    _ws_process = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # 等待连接建立
    for i in range(15):
        time.sleep(1)
        if _ws_process.poll() is not None:
            # 进程已退出，读取错误信息
            output = _ws_process.stdout.read()
            print(f"[飞书监听] 子进程异常退出: {output[:500]}")
            _ws_process = None
            return False

    print("[飞书监听] WebSocket 监听服务已启动（子进程）")
    return True


def wait_for_reply(
    timeout: int = 300,
    poll_interval: float = 2.0,
    expected_actions: list = None,
) -> Optional[dict]:
    """阻塞等待用户在飞书中的回复（按钮点击或消息）

    Args:
        timeout: 超时时间（秒），默认 300 秒（5 分钟）
        poll_interval: 轮询间隔（秒），默认 2 秒
        expected_actions: 期望的指令列表，如 ["confirm", "retry", "cancel"]

    Returns:
        dict: {"action": "confirm", "value": "...", "time": ...}
        None: 超时未收到指令
    """
    start_time_epoch = time.time()
    print(f"[飞书监听] 等待飞书回复 (超时: {timeout}s)...")

    while True:
        elapsed = time.time() - start_time_epoch
        if elapsed >= timeout:
            print(f"[飞书监听] 等待超时 ({timeout}s)")
            return None

        cmd = _read_latest_command(after_time=start_time_epoch)
        if cmd is not None:
            action = cmd.get("action", "")
            if expected_actions is None or action in expected_actions:
                print(f"[飞书监听] 收到指令: {action}")
                _cleanup_old_commands()
                return cmd

        time.sleep(poll_interval)


def stop_listener():
    """停止监听服务"""
    global _ws_process
    if _ws_process is not None:
        try:
            _ws_process.terminate()
            _ws_process.wait(timeout=5)
        except Exception:
            try:
                _ws_process.kill()
            except Exception:
                pass
        _ws_process = None
        print("[飞书监听] 服务已停止")


def send_and_wait(
    notifier,
    title: str,
    elements: list,
    buttons: list = None,
    timeout: int = 300,
    expected_actions: list = None,
) -> Optional[dict]:
    """发送带按钮的飞书卡片并等待用户回复

    一站式方法：启动监听 + 发送通知 + 等待回复。

    Args:
        notifier: FeishuNotifier 实例
        title: 卡片标题
        elements: 卡片内容元素列表（不含按钮）
        buttons: 按钮配置列表，每个为 {"label": "...", "action": "..."}
            默认: 确认全量执行 / 修改后重试 / 取消任务
        timeout: 等待超时（秒）
        expected_actions: 期望的指令列表

    Returns:
        dict: 用户指令 {"action": "...", "value": "...", "time": ...}
        None: 超时
    """
    # 启动监听
    start_listener()

    # 构建按钮
    if buttons is None:
        buttons = [
            {"label": "确认全量执行", "action": "confirm"},
            {"label": "修改后重试", "action": "retry"},
            {"label": "取消任务", "action": "cancel"},
        ]

    button_elements = []
    for btn in buttons:
        button_elements.append({
            "tag": "button",
            "text": {"tag": "plain_text", "content": btn["label"]},
            "type": "primary" if btn["action"] == "confirm" else ("danger" if btn["action"] == "cancel" else "default"),
            "value": {
                "action": btn["action"],
                "label": btn["label"],
            },
        })

    # 添加按钮到卡片
    card_elements = list(elements) + [{
        "tag": "action",
        "actions": button_elements,
    }]

    # 发送卡片
    notifier.send_card(title, card_elements)

    # 等待回复
    return wait_for_reply(timeout=timeout, expected_actions=expected_actions)


if __name__ == "__main__":
    print("飞书指令监听服务（请通过 send_and_wait 或 start_listener 启动）")
