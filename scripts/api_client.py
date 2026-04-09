#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API 调用封装 - 统一的 LLM API 客户端

特性:
- 指数退避重试
- 单次调用超时控制
- 单任务总超时熔断
- 连续失败熔断
- 请求频率限制
"""

import time, requests, json, threading
from typing import Optional, Callable
from dataclasses import dataclass, field


@dataclass
class APIConfig:
    """API 配置"""
    url: str = ""
    key: str = ""
    model: str = ""
    max_retries: int = 3
    retry_base_delay: float = 5.0          # 首次重试等待秒数
    single_timeout: float = 60.0           # 单次请求超时（秒）
    task_timeout: float = 300.0            # 单任务总超时（含重试）
    interval: float = 2.0                  # 请求间隔（秒）
    max_consecutive_failures: int = 5      # 连续失败熔断阈值
    max_tokens: int = 4096
    temperature: float = 0.01
    verify_ssl: bool = False


@dataclass
class APIResult:
    """API 调用结果"""
    success: bool = False
    content: str = ""
    error: str = ""
    attempts: int = 0
    elapsed: float = 0.0


class RateLimitExceeded(Exception):
    """频率限制异常"""
    pass


class TaskTimeout(Exception):
    """单任务超时异常"""
    pass


class CircuitBreakerOpen(Exception):
    """熔断器打开异常"""
    pass


class APIClient:
    """统一的 LLM API 客户端"""

    def __init__(self, config: APIConfig):
        self.config = config
        self._consecutive_failures = 0
        self._breaker_open_until = 0
        self._last_request_time = 0
        self._total_calls = 0
        self._total_failures = 0
        self._lock = threading.Lock()

    def call(self, prompt: str, system: str = None, on_retry: Callable = None) -> APIResult:
        """调用 API，带重试和熔断

        Args:
            prompt: 用户消息
            system: 系统消息（可选）
            on_retry: 重试回调 fn(attempt, error, wait_seconds)

        Returns:
            APIResult

        Raises:
            CircuitBreakerOpen: 连续失败过多，熔断器打开
            TaskTimeout: 单任务总超时
        """
        # 检查熔断器
        self._check_circuit_breaker()

        start_time = time.time()
        cfg = self.config
        last_error = ""

        for attempt in range(cfg.max_retries):
            # 检查任务总超时
            elapsed = time.time() - start_time
            if elapsed >= cfg.task_timeout:
                self._record_failure()
                raise TaskTimeout(
                    f"单任务总超时 ({cfg.task_timeout}s)，已尝试 {attempt + 1} 次"
                )

            # 频率限制
            self._rate_limit()

            try:
                result = self._single_call(prompt, system)
                self._record_success()
                return result

            except RateLimitExceeded as e:
                last_error = str(e)
                wait = cfg.retry_base_delay * (2 ** attempt)
                if on_retry:
                    on_retry(attempt, f"限流", wait)
                time.sleep(wait)

            except requests.exceptions.Timeout:
                last_error = f"请求超时 ({cfg.single_timeout}s)"
                wait = cfg.retry_base_delay * (2 ** attempt)
                if on_retry:
                    on_retry(attempt, last_error, wait)
                time.sleep(wait)

            except requests.exceptions.ConnectionError as e:
                last_error = f"连接错误: {str(e)[:100]}"
                wait = cfg.retry_base_delay * (2 ** attempt)
                if on_retry:
                    on_retry(attempt, last_error, wait)
                time.sleep(wait)

            except Exception as e:
                last_error = str(e)
                wait = cfg.retry_base_delay * (2 ** attempt)
                if on_retry:
                    on_retry(attempt, last_error, wait)
                time.sleep(wait)

        # 所有重试失败
        self._record_failure()
        return APIResult(
            success=False,
            error=f"连续失败 {cfg.max_retries} 次，最后错误: {last_error}",
            attempts=cfg.max_retries,
            elapsed=time.time() - start_time,
        )

    def _single_call(self, prompt: str, system: str = None) -> APIResult:
        """单次 API 调用"""
        cfg = self.config
        start = time.time()

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg.key}",
        }

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        data = {
            "model": cfg.model,
            "messages": messages,
            "temperature": cfg.temperature,
            "max_tokens": cfg.max_tokens,
            "stream": False,
        }

        resp = requests.post(
            cfg.url,
            headers=headers,
            json=data,
            timeout=cfg.single_timeout,
            verify=cfg.verify_ssl,
        )
        resp.raise_for_status()

        result = resp.json()

        # 检查 API 层面错误
        if "error" in result:
            error_msg = result["error"].get("message", str(result["error"]))
            if "rate" in error_msg.lower() or "429" in str(resp.status_code):
                raise RateLimitExceeded(error_msg)
            raise RuntimeError(f"API错误: {error_msg}")

        if "choices" not in result or not result["choices"]:
            raise RuntimeError("API无choices返回")

        content = result["choices"][0]["message"]["content"].strip()
        self._total_calls += 1

        return APIResult(
            success=True,
            content=content,
            attempts=1,
            elapsed=time.time() - start,
        )

    def _rate_limit(self):
        """频率限制"""
        with self._lock:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.config.interval:
                time.sleep(self.config.interval - elapsed)
            self._last_request_time = time.time()

    def _check_circuit_breaker(self):
        """检查熔断器"""
        with self._lock:
            if time.time() < self._breaker_open_until:
                remaining = self._breaker_open_until - time.time()
                raise CircuitBreakerOpen(
                    f"熔断器打开，需等待 {remaining:.0f}s "
                    f"(连续失败 {self._consecutive_failures} 次)"
                )

    def _record_success(self):
        """记录成功"""
        with self._lock:
            self._consecutive_failures = 0
            self._breaker_open_until = 0
            self._total_calls += 1

    def _record_failure(self):
        """记录失败"""
        with self._lock:
            self._consecutive_failures += 1
            self._total_failures += 1
            if self._consecutive_failures >= self.config.max_consecutive_failures:
                # 熔断 60 秒
                self._breaker_open_until = time.time() + 60

    def reset_circuit_breaker(self):
        """手动重置熔断器"""
        with self._lock:
            self._consecutive_failures = 0
            self._breaker_open_until = 0

    @property
    def stats(self) -> dict:
        """统计信息"""
        with self._lock:
            return {
                "total_calls": self._total_calls,
                "total_failures": self._total_failures,
                "consecutive_failures": self._consecutive_failures,
                "breaker_open": time.time() < self._breaker_open_until,
            }
