#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高精度PDF目录识别脚本
策略：
1. 全文扫描所有包含省名的页面，提取上下文
2. 用启发式规则判断：文件开头/结尾/中间引用
3. 按省份汇总start/end事件，交叉验证生成目录
"""

import fitz
import json
import re
import os
from collections import defaultdict

PDF_PATH = r"E:\工作\启科律行\知识库\量刑\20250901全国各省市量刑指导意见实施细则汇编(OCR).pdf"
OUTPUT_DIR = r"E:\工作\启科律行\知识库\量刑\代码"

PROVINCES = [
    "北京", "天津", "上海", "重庆",
    "河北", "山西", "辽宁", "吉林", "黑龙江",
    "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "海南",
    "四川", "贵州", "云南", "陕西", "甘肃", "青海",
    "内蒙古", "广西", "西藏", "宁夏", "新疆",
]


def scan_all_province_pages(doc):
    """全文扫描，提取所有包含省名的页面"""
    total = doc.page_count
    results = []

    for page_num in range(total):
        page = doc[page_num]
        text = page.get_text()
        page_no = page_num + 1

        for prov in PROVINCES:
            if prov not in text:
                continue

            # 找到省名在文本中的位置
            idx = text.find(prov)
            # 提取前后上下文
            context_start = max(0, idx - 200)
            context_end = min(len(text), idx + len(prov) + 400)
            context = text[context_start:context_end].strip()

            results.append({
                "page": page_no,
                "province": prov,
                "context": context,
            })

    return results


def classify_page_type(item):
    """
    判断页面中省名出现的上下文类型：
    - file_start: 印发通知页（文件开头）
    - file_end: 印发表样页（文件末尾）
    - title_page: 文件正文标题页
    - body_ref: 正文中间引用其他省的文件
    """
    ctx = item["context"]

    # ===== 1. 印发表样页（文件末尾）=====
    # 特征：XX省高级人民法院办公室 + 年月日印发
    if re.search(r"(人民法院|人民检察院)办公室", ctx) and \
       re.search(r"\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日\s*印发", ctx):
        return "file_end"

    # 特征：抄送 + 印发
    if "抄送" in ctx and "印发" in ctx:
        return "file_end"

    # ===== 2. 印发通知页（文件开头）=====
    # 特征：关于印发 + 实施细则/意见
    if re.search(r"关于印发.*实施细则|关于印发.*量刑指导意见|关于印发.*意见", ctx):
        return "file_start"

    # 特征：现将 + 印发 + 执行
    if "现将" in ctx and ("印发" in ctx or "执行" in ctx) and \
       re.search(r"(高级人民法院|人民检察院)", ctx):
        return "file_start"

    # ===== 3. 正文标题页 =====
    # 特征：实施细则/指导意见出现在页面开头位置，且含省名
    # 但不是file_start（没有"关于印发"等通知用语）
    ctx_first_100 = ctx[:150] if len(ctx) > 150 else ctx
    if ("实施细则" in ctx_first_100 or "量刑指导意见" in ctx_first_100) and \
       "关于印发" not in ctx and "抄送" not in ctx:
        return "title_page"

    # ===== 4. 其他（正文引用等）=====
    return "body_ref"


if __name__ == "__main__":
    print("打开PDF...")
    doc = fitz.open(PDF_PATH)
    total = doc.page_count
    print(f"PDF 共 {total} 页\n")

    # 第一步：扫描
    print("第一步：扫描所有包含省名的页面...")
    raw = scan_all_province_pages(doc)
    print(f"  找到 {len(raw)} 个含省名的页面")

    # 第二步：分类
    print("第二步：分类页面类型...")
    for item in raw:
        item["type"] = classify_page_type(item)

    # 统计
    type_counts = defaultdict(int)
    for item in raw:
        type_counts[item["type"]] += 1
    for t, c in sorted(type_counts.items()):
        print(f"  {t}: {c}")

    # 第三步：按省份输出
    print("\n" + "=" * 75)
    print("各省份文件边界识别结果")
    print("=" * 75)

    province_events = defaultdict(list)
    for item in raw:
        province_events[item["province"]].append(item)

    # 收集所有start和end事件
    all_file_events = []

    for prov in PROVINCES:
        events = province_events.get(prov, [])
        if not events:
            print(f"\n【{prov}】无匹配")
            continue

        starts = [e for e in events if e["type"] in ("file_start", "title_page")]
        ends = [e for e in events if e["type"] == "file_end"]
        body_refs = [e for e in events if e["type"] == "body_ref"]

        print(f"\n【{prov}】")

        for e in sorted(starts, key=lambda x: x["page"]):
            print(f"  [START] P{e['page']:>4} | {e['context'][:100].replace(chr(10), ' | ')}")
            all_file_events.append({"province": prov, "page": e["page"], "event": "start", "type": e["type"]})

        for e in sorted(ends, key=lambda x: x["page"]):
            print(f"  [END]   P{e['page']:>4} | {e['context'][:100].replace(chr(10), ' | ')}")
            all_file_events.append({"province": prov, "page": e["page"], "event": "end"})

        if body_refs:
            print(f"  [REF]   {len(body_refs)} 个正文引用（已忽略）")

    # 第四步：全局排序，展示文件边界时间线
    print("\n" + "=" * 75)
    print("全局文件边界时间线（按页码排序）")
    print("=" * 75)

    all_file_events.sort(key=lambda x: x["page"])
    for e in all_file_events:
        print(f"  P{e['page']:>4} [{e['event']:<5}] {e['province']}")

    # 保存
    output = {
        "total_matches": len(raw),
        "events": all_file_events,
        "raw": raw,
    }
    output_path = os.path.join(OUTPUT_DIR, "precise_events.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存至: {output_path}")

    doc.close()
