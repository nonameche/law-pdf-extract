#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""输出文件排版模块

排版规则（基于实际使用反馈）：
1. 每个字段（要点/定性/处理/法律依据）之间用空行分隔
2. 法律依据按【】分割，每条独立一行，条目间只换行（无空行）
3. 连续多余空行合并
4. 去除首尾空白
"""

import re


def format_output(raw_text: str) -> str:
    """对提取结果进行排版

    Args:
        raw_text: API 返回的原始文本或拼接后的内容

    Returns:
        排版后的文本
    """
    text = raw_text.strip()
    if not text:
        return ""

    lines = text.split('\n')
    formatted_lines = []
    in_law = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # 检测法律依据字段
        if re.match(r'^法律依据[：:]', stripped):
            in_law = True
            formatted_lines.append('')
            formatted_lines.append('法律依据：')
            content_after = re.sub(r'^法律依据[：:]\s*', '', stripped)
            if content_after:
                stripped = content_after
            else:
                continue

        # 检测其他字段（要点/定性/处理）
        elif re.match(r'^(要点|定性|处理)[：:]', stripped):
            in_law = False
            formatted_lines.append('')
            formatted_lines.append(stripped)
            continue

        # 法律依据部分：按【】分割，每条独立一行
        if in_law:
            items = re.split(r'(?=【)', stripped)
            for item in items:
                item = item.strip()
                if item:
                    formatted_lines.append(item)
        else:
            formatted_lines.append(stripped)

    result = '\n'.join(formatted_lines)
    # 合并连续空行（最多保留一个）
    result = re.sub(r'\n{3,}', '\n\n', result)
    return result.strip() + '\n'


def split_by_brackets(text: str) -> list:
    """将文本按【】分割成独立的法律依据条目

    Args:
        text: 包含多条法律依据的文本

    Returns:
        法律依据条目列表
    """
    items = re.split(r'(?=【)', text)
    return [item.strip() for item in items if item.strip().startswith('【')]
