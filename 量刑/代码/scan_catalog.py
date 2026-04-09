#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OCR PDF 目录提取脚本
目标：从 3265 页的 OCR PDF 中，逐页扫描识别文件标题，
      构建完整的文件级目录（各省市量刑实施细则 + 最高法/最高检文件）
"""

import fitz
import json
import re
import os
from collections import defaultdict

PDF_PATH = r"E:\工作\启科律行\知识库\量刑\20250901全国各省市量刑指导意见实施细则汇编(OCR).pdf"
OUTPUT_DIR = r"E:\工作\启科律行\知识库\量刑\代码"

# ===== 标题识别规则 =====

# 1. 最高法/最高检/国家机构关键词
AUTHORITY_KEYWORDS = [
    "最高人民法院",
    "最高人民检察院",
    "最高人民法院、最高人民检察院",
    "最高人民检察院、最高人民法院",
    "公安部",
    "司法部",
    "国家监委",
    "全国人大常委会",
    "全国人民代表大会",
    "中央政法委",
    "最高法",
    "最高检",
]

# 2. 省级法院/检察院
PROVINCES = [
    "北京", "天津", "上海", "重庆",
    "河北", "山西", "辽宁", "吉林", "黑龙江",
    "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "海南",
    "四川", "贵州", "云南", "陕西", "甘肃", "青海",
    "内蒙古", "广西", "西藏", "宁夏", "新疆",
]

# 省级法院/检察院的完整名称模式
PROVINCE_COURT_PATTERN = re.compile(
    r"(" + "|".join(PROVINCES) + r")"
    r"(?:省|市|自治区)?"
    r"(?:高级人民法院|人民检察院|高级人民法院、人民检察院|人民检察院、高级人民法院)"
)

# 3. 文件类型关键词（标题的结尾部分）
DOC_TYPE_KEYWORDS = [
    "意见",
    "通知",
    "规定",
    "解释",
    "批复",
    "答复",
    "纪要",
    "细则",
    "实施细则",
    "实施办法",
    "实施意见",
    "标准",
    "补充规定",
    "若干问题的解释",
    "若干问题的意见",
    "若干问题的规定",
    "适用法律若干问题的解释",
    "适用法律若干问题的意见",
    "适用法律若干问题的规定",
    "座谈会纪要",
    "量刑指导意见",
    "量刑指导意见（试行）",
    "量刑规范",
    "量刑规范的实施细则",
    "量刑规范化实施细则",
]

# 4. 发文字号模式（如 法发〔2021〕21号、高检发释字〔2022〕1号）
DOC_NUMBER_PATTERN = re.compile(
    r"[（(]\s*(?:法发|法释|高检发|高法发|公通字|公法|高检研|国发|司发|"
    r"刑他字|法〔|检〔|公〔|刑〔|司〔)"
    r"[^）)]*[）)]\s*"
    r"(?:\d{4}\s*年?\s*\d*\s*号?)?"
)

# 5. 标题核心模式：机构 + 关于 + 内容 + 文件类型
TITLE_CORE_PATTERN = re.compile(
    r"(.{2,30}?)(?:印发|关于|印发《)(.{2,80}?)(?:》?的(?:通知|意见|批复|规定|解释|办法|细则|标准))"
)

# 6. 简单标题模式：机构 + 文件名
TITLE_SIMPLE_PATTERN = re.compile(
    r"(.{2,40}?(?:意见|实施细则|实施办法|实施意见|指导意见|规定|解释|通知|批复|办法|细则|标准))"
)

def is_title_line(text):
    """判断一行文本是否可能是标题"""
    text = text.strip()
    if not text or len(text) < 5:
        return False
    # 标题通常不会太长
    if len(text) > 120:
        return False
    # 标题通常不包含句号
    if "。" in text:
        return False
    # 排除正文特征
    if re.match(r"^\s*第[一二三四五六七八九十百千\d]+条", text):
        return False
    if re.match(r"^\s*\d+[.、]\s", text):
        return False
    if text.startswith("（") and text.endswith("）"):
        return False
    return True


def extract_title_from_page(text, page_no):
    """从页面文本中提取文件标题"""
    lines = text.split("\n")
    titles = []

    for i, line in enumerate(lines):
        line = line.strip()
        if not is_title_line(line):
            continue

        # 策略1：匹配"XX省高级人民法院..."模式
        match_court = PROVINCE_COURT_PATTERN.search(line)
        if match_court:
            # 取匹配到的完整标题行
            title = line.strip()
            # 尝试合并下一行（标题可能跨行）
            if i + 1 < len(lines) and is_title_line(lines[i+1]):
                next_line = lines[i+1].strip()
                if len(next_line) < 80 and not next_line[0].isdigit():
                    title = title + next_line
            titles.append({
                "page": page_no,
                "title": title,
                "type": "province_court",
                "confidence": "high",
            })
            continue

        # 策略2：匹配"最高人民法院/最高人民检察院..."模式
        for auth in AUTHORITY_KEYWORDS:
            if auth in line:
                title = line.strip()
                # 合并下一行
                if i + 1 < len(lines) and is_title_line(lines[i+1]):
                    next_line = lines[i+1].strip()
                    if len(next_line) < 80 and not next_line[0].isdigit():
                        title = title + next_line
                titles.append({
                    "page": page_no,
                    "title": title,
                    "type": "national",
                    "confidence": "high",
                })
                break

        # 策略3：匹配发文字号（单独成行时，可能标题在上方）
        if not titles or titles[-1]["page"] != page_no:
            match_num = DOC_NUMBER_PATTERN.search(line)
            if match_num and len(line) < 60:
                # 发文字号行，向上查找标题
                for j in range(max(0, i-3), i):
                    up_line = lines[j].strip()
                    if is_title_line(up_line) and len(up_line) > 10:
                        # 检查是否已记录过
                        if not any(t["title"] == up_line for t in titles):
                            titles.append({
                                "page": page_no,
                                "title": up_line,
                                "type": "doc_number_ref",
                                "confidence": "medium",
                            })

    return titles


def classify_province(title):
    """从标题中识别省份"""
    for prov in PROVINCES:
        if prov in title:
            return prov
    return None


def scan_pdf(pdf_path):
    """扫描PDF，提取所有文件标题"""
    doc = fitz.open(pdf_path)
    total_pages = doc.page_count
    print(f"PDF 共 {total_pages} 页，开始扫描...")

    all_titles = []

    for page_num in range(total_pages):
        page = doc[page_num]
        text = page.get_text()
        page_no = page_num + 1

        titles = extract_title_from_page(text, page_no)
        all_titles.extend(titles)

        if (page_num + 1) % 200 == 0:
            print(f"  已扫描 {page_num + 1}/{total_pages} 页，已发现 {len(all_titles)} 个标题...")

    doc.close()
    print(f"  扫描完成，共发现 {len(all_titles)} 个标题候选")

    return all_titles


def build_catalog(all_titles):
    """构建目录，去重并归类"""
    # 去重：相同标题只保留第一个（页码最小的）
    seen = {}
    unique_titles = []
    for t in all_titles:
        title = t["title"]
        if title not in seen:
            seen[title] = True
            unique_titles.append(t)

    # 按页码排序
    unique_titles.sort(key=lambda x: x["page"])

    # 分类
    catalog = {
        "national": [],      # 最高法/最高检
        "provinces": defaultdict(list),  # 各省
        "other": [],         # 其他
    }

    for t in unique_titles:
        prov = classify_province(t["title"])
        if prov:
            catalog["provinces"][prov].append(t)
        elif any(auth in t["title"] for auth in AUTHORITY_KEYWORDS):
            catalog["national"].append(t)
        else:
            catalog["other"].append(t)

    return catalog, unique_titles


def print_catalog(catalog):
    """打印目录摘要"""
    print("\n" + "=" * 70)
    print("目录摘要")
    print("=" * 70)

    print(f"\n--- 最高法/最高检文件 ({len(catalog['national'])} 个) ---")
    for t in catalog["national"][:20]:
        print(f"  P{t['page']:>4} | {t['title'][:80]}")
    if len(catalog["national"]) > 20:
        print(f"  ... 还有 {len(catalog['national']) - 20} 个")

    print(f"\n--- 各省市文件 ---")
    for prov in sorted(catalog["provinces"].keys()):
        items = catalog["provinces"][prov]
        print(f"\n  【{prov}】{len(items)} 个文件")
        for t in items[:5]:
            print(f"    P{t['page']:>4} | {t['title'][:80]}")
        if len(items) > 5:
            print(f"    ... 还有 {len(items) - 5} 个")

    if catalog["other"]:
        print(f"\n--- 其他 ({len(catalog['other'])} 个) ---")
        for t in catalog["other"][:10]:
            print(f"  P{t['page']:>4} | {t['title'][:80]}")

    total = len(catalog["national"]) + sum(len(v) for v in catalog["provinces"].values()) + len(catalog["other"])
    print(f"\n总计: {total} 个文件")


if __name__ == "__main__":
    all_titles = scan_pdf(PDF_PATH)
    catalog, unique_titles = build_catalog(all_titles)
    print_catalog(catalog)

    # 保存结果
    output = {
        "total_titles": len(unique_titles),
        "catalog": {
            "national": catalog["national"],
            "provinces": {k: v for k, v in catalog["provinces"].items()},
            "other": catalog["other"],
        },
        "raw_titles": unique_titles,
    }

    output_path = os.path.join(OUTPUT_DIR, "catalog.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n目录已保存至: {output_path}")
