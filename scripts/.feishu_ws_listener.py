
import asyncio
import json
import os
import time
import sys

sys.path.insert(0, r"C:\Users\黑面书生\AppData\Roaming\WPS 灵犀\serverdir\skills\law-pdf-extract\scripts")

import lark_oapi as lark
from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTrigger, P2CardActionTriggerResponse

APP_ID = "cli_a947bdb243a19ccc"
APP_SECRET = "dYlOLoRwDE5zKVptcKCmXbAAC26REjEK"
CMD_DIR = os.path.join(r"C:\Users\黑面书生\AppData\Roaming\WPS 灵犀\serverdir\skills\law-pdf-extract\scripts", ".feishu_cmd")

os.makedirs(CMD_DIR, exist_ok=True)

def write_command(action, value="", extra=None):
    cmd = {"action": action, "value": value, "time": time.time()}
    if extra:
        cmd.update(extra)
    filepath = os.path.join(CMD_DIR, f"cmd_{int(time.time() * 1000)}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(cmd, f, ensure_ascii=False)
    print(f"[WS] 指令写入: {action} = {value}", flush=True)

def card_handler(data):
    try:
        operator = data.event.operator
        action_obj = data.event.action
        value = action_obj.value if action_obj and action_obj.value else {}
        cmd_action = value.get("action", "confirm")
        cmd_label = value.get("label", cmd_action)

        extra = {}
        if operator:
            extra["open_id"] = operator.open_id or ""
            extra["user_id"] = operator.user_id or ""

        write_command(cmd_action, cmd_label, extra)
        print(f"[WS] 卡片回调: {cmd_label} -> {cmd_action}", flush=True)
    except Exception as e:
        print(f"[WS] 卡片回调异常: {e}", flush=True)
        import traceback
        traceback.print_exc()
    return P2CardActionTriggerResponse()

def message_handler(data):
    try:
        msg = data.event.message
        if msg and msg.message_type == "text":
            content = json.loads(msg.content).get("text", "").strip() if msg.content else ""
            if content:
                action_map = {"确认": "confirm", "重试": "retry", "取消": "cancel", "暂停": "pause", "继续": "resume"}
                action = "custom"
                for k, v in action_map.items():
                    if k in content:
                        action = v
                        break
                write_command(action, content)
                print(f"[WS] 消息: {content} -> {action}", flush=True)
    except Exception as e:
        print(f"[WS] 消息处理异常: {e}", flush=True)

event_handler = (
    lark.EventDispatcherHandler.builder("", "")
    .register_p2_card_action_trigger(card_handler)
    .register_p2_im_message_receive_v1(message_handler)
    .build()
)

client = lark.ws.Client(APP_ID, APP_SECRET, event_handler=event_handler, log_level=lark.LogLevel.INFO)

print("[WS] 飞书 WebSocket 监听服务启动中...", flush=True)
client.start()
