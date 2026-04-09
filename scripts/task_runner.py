#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务运行器 - 通用的批量任务执行框架

特性:
- 自动断点续跑（进度持久化）
- 失败不阻塞，自动跳过并记录
- 飞书实时进度推送
- 连续失败熔断告警
- 任务结束汇总报告
"""

import os, json, time, sys, traceback, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable, Optional, Any, List, Dict
from dataclasses import dataclass, field, asdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from api_client import APIClient, APIConfig, CircuitBreakerOpen, TaskTimeout
from feishu_notifier import FeishuNotifier


@dataclass
class TaskItem:
    """单个任务项"""
    task_id: str = ""
    label: str = ""
    group: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class TaskResult:
    """任务结果"""
    task_id: str = ""
    status: str = "pending"
    error: str = ""
    output: Any = None
    elapsed: float = 0.0
    attempts: int = 0


@dataclass
class RunConfig:
    """运行配置"""
    resume: bool = True
    retry_failed: bool = False
    notify_enabled: bool = True
    notify_interval: int = 10
    notify_interval_sec: float = 300.0
    output_dir: str = ""
    progress_file: str = ""
    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    feishu_chat_id: str = ""
    task_name: str = "批量任务"
    max_workers: int = 5


class TaskRunner:
    """通用批量任务运行器"""

    def __init__(self, api_config, run_config=None, task_processor=None):
        self.api_config = api_config
        self.config = run_config or RunConfig()
        self.task_processor = task_processor
        self.api = APIClient(api_config)
        self.notifier = FeishuNotifier(
            app_id=self.config.feishu_app_id or None,
            app_secret=self.config.feishu_app_secret or None,
            chat_id=self.config.feishu_chat_id or None,
            task_name=self.config.task_name,
            enabled=self.config.notify_enabled,
        )
        self._progress = {}
        self._lock = threading.Lock()
        self._results = []
        self._start_time = 0
        self._last_notify_time = 0

    def load_progress(self):
        pf = self.config.progress_file
        if not pf and self.config.output_dir:
            pf = os.path.join(self.config.output_dir, "_progress.json")
        if not pf or not os.path.exists(pf):
            return {}
        try:
            with open(pf, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def save_progress(self):
        pf = self.config.progress_file
        if not pf and self.config.output_dir:
            pf = os.path.join(self.config.output_dir, "_progress.json")
        if not pf:
            return
        os.makedirs(os.path.dirname(pf), exist_ok=True)
        # 合并历史进度，避免单次运行只写入本轮任务而覆盖已完成的条目
        merged = dict(self._progress.get("results", {}))
        for r in self._results:
            merged[r.task_id] = {"status": r.status, "error": r.error, "elapsed": r.elapsed}
        self._progress["results"] = merged
        data = {
            "task_name": self.config.task_name,
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "results": merged,
        }
        with open(pf, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _should_skip(self, task_id):
        if not self.config.resume:
            return False
        existing = self._progress.get("results", {}).get(task_id, {})
        return existing.get("status", "") in ("success", "skipped")

    def _notify_progress(self, current, total, stats, item_label=""):
        with self._lock:
            if not self.config.notify_enabled:
                return
            now = time.time()
            if current > 0 and current < total:
                if current % self.config.notify_interval != 0:
                    if now - self._last_notify_time < self.config.notify_interval_sec:
                        return
            elapsed = (now - self._start_time) / 60
            eta = None
            if current > 0:
                avg = (now - self._start_time) / current
                eta = avg * (total - current) / 60
            self.notifier.notify_progress(
                current=current, total=total,
                success=stats["success"], failed=stats["failed"], skipped=stats["skipped"],
                elapsed_min=elapsed, eta_min=eta, current_item=item_label,
            )
            self._last_notify_time = now

    def run(self, tasks):
        self._start_time = time.time()
        self._last_notify_time = 0
        total = len(tasks)
        if total == 0:
            print("任务列表为空", flush=True)
            return {"total": 0}
        
        if self.config.output_dir:
            os.makedirs(self.config.output_dir, exist_ok=True)
            
        self._progress = self.load_progress()
        stats = {"success": 0, "failed": 0, "skipped": 0}
        resumed = 0
        
        # Calculate initial stats
        for task in tasks:
            existing = self._progress.get("results", {}).get(task.task_id, {})
            status = existing.get("status", "")
            if status == "success":
                stats["success"] += 1
            elif status == "skipped":
                stats["skipped"] += 1
        resumed = stats["success"] + stats["skipped"]

        def process_single_task(task):
            task_label = task.label or task.task_id
            
            with self._lock:
                if self._should_skip(task.task_id):
                    return None
                    
                if self.config.retry_failed:
                    existing = self._progress.get("results", {}).get(task.task_id, {})
                    if existing.get("status") != "failed":
                        return None

            result = TaskResult(task_id=task.task_id)
            task_start = time.time()
            
            try:
                if self.task_processor:
                    output = self.task_processor(task, self.api)
                    result.status = "success"
                    result.output = output
                    with self._lock:
                        stats["success"] += 1
                        done = stats["success"] + stats["failed"]
                        print(f"[{done}/{total - resumed}] {task_label} [OK]", flush=True)
                else:
                    result.status = "skipped"
                    with self._lock:
                        stats["skipped"] += 1
            except (CircuitBreakerOpen, TaskTimeout) as e:
                result.status = "failed"
                result.error = str(e)
                with self._lock:
                    stats["failed"] += 1
                    done = stats["success"] + stats["failed"]
                    print(f"[{done}/{total - resumed}] {task_label} [FAIL] {e}", flush=True)
            except Exception as e:
                result.status = "failed"
                result.error = str(e)[:500]
                with self._lock:
                    stats["failed"] += 1
                    done = stats["success"] + stats["failed"]
                    print(f"[{done}/{total - resumed}] {task_label} [FAIL] {e}", flush=True)
                    
            result.elapsed = time.time() - task_start
            
            with self._lock:
                self._results.append(result)
                self.save_progress()
                self._notify_progress(current=stats["success"] + stats["failed"], total=total, stats=stats, item_label=task_label)
                
            return result

        self.notifier.notify_start(total=total, description=f"跳过已完成 {resumed}/{total}" if resumed else "")
        hdr = "=" * 60
        print(hdr, flush=True)
        print(f"任务: {self.config.task_name}", flush=True)
        print(f"总数: {total} | 已完成: {resumed} | 待处理: {total - resumed} | 并发度: {self.config.max_workers}", flush=True)
        print(hdr, flush=True)
        
        api_stats_at_start = self.api.stats

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = []
            for task in tasks:
                if not self._should_skip(task.task_id):
                    futures.append(executor.submit(process_single_task, task))
            for future in as_completed(futures):
                pass

        with self._lock:
            self.save_progress()
            elapsed = (time.time() - self._start_time) / 60
            api_stats = self.api.stats
            api_calls = api_stats["total_calls"] - api_stats_at_start["total_calls"]
            self.notifier.notify_complete(total=total, success=stats["success"], failed=stats["failed"], skipped=stats["skipped"], elapsed_min=elapsed, output_path=self.config.output_dir or "", extra_info=f"**API 调用次数:** {api_calls}")
            
            print(hdr, flush=True)
            print(f"完成! 成功={stats['success']} 失败={stats['failed']} 跳过={stats['skipped']}", flush=True)
            print(f"耗时: {elapsed:.1f}分钟", flush=True)
            print(hdr, flush=True)
            
            failed_ids = [r.task_id for r in self._results if r.status == "failed"]
            return {"total": total, "success": stats["success"], "failed": stats["failed"], "skipped": stats["skipped"], "elapsed_min": elapsed, "failed_ids": failed_ids}
