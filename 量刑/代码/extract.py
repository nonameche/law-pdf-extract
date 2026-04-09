# -*- coding: utf-8 -*-
"""量刑指导意见全量提取脚本 v8.3

基于全量诊断结果重写，修复以下问题：
1. 区域定位：区分目录行和正文标题，跳过正文引用
2. 终点定位：验证匹配位置是否为行首章节标题
3. 罪名列表从22个扩充到35+个，覆盖所有省份
4. 罪名定位增加OCR错字修复和"X. 罪名"格式
5. 好差文本去重基于编辑距离（阈值0.8）
6. 跳过逻辑精确化，只跳过章节标题级内容
"""

import json, os, re, sys, time, logging, asyncio, aiohttp, fitz
from datetime import datetime
from difflib import SequenceMatcher

# ====== 配置 ======
API_BASE = "https://ark.cn-beijing.volces.com/api/v3"
API_KEY = "3cb4b1df-6c2c-4ee8-bfd3-b4d441b1b263"
MODEL = "doubao-seed-2-0-lite-260215"
MAX_CONCURRENT = 5
REQ_INTERVAL = 2.0
MAX_RETRIES = 3
PDF_PATH = r"E:\工作\启科律行\知识库\量刑\20250901全国各省市量刑指导意见实施细则汇编(OCR).pdf"
CATALOG = r"E:\工作\启科律行\知识库\量刑\代码\final_catalog_v7.json"
OUTPUT = r"E:\工作\启科律行\知识库\量刑\输出"
PROGRESS = r"E:\工作\启科律行\知识库\量刑\代码\extract_progress.json"
LOG_FILE = r"E:\工作\启科律行\知识库\量刑\代码\extract.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("extract")

# ====== 常量 ======

NL = "\n"
FZ_L = "\uff08"  # （
FZ_R = "\uff09"  # ）

CN = [
    "零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十",
    "十一", "十二", "十三", "十四", "十五", "十六", "十七", "十八",
    "十九", "二十", "二十一", "二十二", "二十三", "二十四", "二十五",
    "二十六", "二十七", "二十八", "二十九", "三十",
    "三十一", "三十二", "三十三", "三十四", "三十五",
]

# 扩充罪名列表 v8.3（35个）
KNOWN_CRIMES = [
    # 原始22个
    "交通肇事罪", "危险驾驶罪", "非法吸收公众存款罪",
    "集资诈骗罪", "信用卡诈骗罪", "合同诈骗罪",
    "故意伤害罪", "强奸罪", "非法拘禁罪",
    "抢劫罪", "盗窃罪", "诈骗罪",
    "抢夺罪", "职务侵占罪", "敲诈勒索罪",
    "妨害公务罪", "聚众斗殴罪", "寻衅滋事罪",
    "掩饰、隐瞒犯罪所得、犯罪所得收益罪",
    "走私、贩卖、运输、制造毒品罪",
    "非法持有毒品罪", "容留他人吸毒罪",
    "引诱、容留、介绍卖淫罪",
    # 新增13个
    "开设赌场罪",
    "帮助信息网络犯罪活动罪",
    "拒不执行判决、裁定罪",
    "侵犯公民个人信息罪",
    "非法经营罪",
    "组织卖淫罪",
    "猥亵儿童罪",
    "污染环境罪",
    "故意毁坏财物罪",
    "走私、贩卖、运输、制造毒品",
    "掩饰、隐瞒犯罪所得罪",
    "非法持有毒品",
    "容留他人吸毒",
]

# OCR错字映射表（罪名模糊匹配用）
CRIME_OCR_FIXES = {
    "交通空导罪": "交通肇事罪", "交通学事罪": "交通肇事罪",
    "交通晕事罪": "交通肇事罪", "交通事罪": "交通肇事罪",
    "交通量事罪": "交通肇事罪", "交通签事罪": "交通肇事罪",
    "危险当驶罪": "危险驾驶罪", "危险驾段罪": "危险驾驶罪",
    "危险驾歌罪": "危险驾驶罪", "后险驾驶罪": "危险驾驶罪",
    "后险中罪": "危险驾驶罪",
    "非法尚禁罪": "非法拘禁罪", "非法拘崇罪": "非法拘禁罪",
    "诈编罪": "诈骗罪", "诈确罪": "诈骗罪", "诈翳罪": "诈骗罪",
    "诈膈罪": "诈骗罪", "诈痛罪": "诈骗罪", "诈贻罪": "诈骗罪",
    "诈单察罪": "诈骗罪",
    "益窃罪": "盗窃罪", "益访罪": "盗窃罪", "益场罪": "盗窃罪",
    "窃罪": "盗窃罪",
    "抢奇罪": "抢夺罪", "抢存罪": "抢夺罪", "抢动罪": "抢劫罪",
    "强好罪": "强奸罪", "鱼好罪": "强奸罪",
    "敲诈勤索罪": "敲诈勒索罪", "敲诈勒素罪": "敲诈勒索罪",
    "鼓许勒素罪": "敲诈勒索罪",
    "聚众斗政罪": "聚众斗殴罪", "聚众斗酸罪": "聚众斗殴罪",
    "竖众斗罪": "聚众斗殴罪", "蒙众斗政罪": "聚众斗殴罪",
    "乘众斗酸罪": "聚众斗殴罪",
    "寻蚌滋事罪": "寻衅滋事罪", "导衅滋事罪": "寻衅滋事罪",
    "子鲜滋事罪": "寻衅滋事罪",
    "合同诈编罪": "合同诈骗罪", "合同许骗罪": "合同诈骗罪",
    "合同诈霸罪": "合同诈骗罪",
    "信用卡诈端罪": "信用卡诈骗罪", "信用卡诈罪": "信用卡诈骗罪",
    "信用卡详骗罪": "信用卡诈骗罪", "信用下辞骗罪": "信用卡诈骗罪",
    "集资诈编罪": "集资诈骗罪",
    "职劳侵占罪": "职务侵占罪", "职旁夜古罪": "职务侵占罪",
    "坊害公务罪": "妨害公务罪", "妨害么务罪": "妨害公务罪",
    "开设酱场罪": "开设赌场罪", "开设籍场罪": "开设赌场罪",
    "狼亵儿童罪": "猥亵儿童罪", "狼袭儿量罪": "猥亵儿童罪",
    "很热儿鱼罪": "猥亵儿童罪", "狼良儿童罪": "猥亵儿童罪",
    "狼表儿量罪": "猥亵儿童罪",
    "组织章浮罪": "组织卖淫罪",
    "引诱、答馏、介绍实淫罪": "引诱、容留、介绍卖淫罪",
    "引诱、容留、介绍卖浮罪": "引诱、容留、介绍卖淫罪",
    "容苗他人吸毒罪": "容留他人吸毒罪",
    "容留做人吸盘罪": "容留他人吸毒罪",
    "3E法经营罪": "非法经营罪", "故意伤善罪": "故意伤害罪",
    "走私、贩买、运输、制语盘品罪": "走私、贩卖、运输、制造毒品罪",
    "走私、败实、还箱、制造毒品罪": "走私、贩卖、运输、制造毒品罪",
    "无私、败卖、还输、制语垂品罪": "走私、贩卖、运输、制造毒品罪",
    "走私、贩买、运输、制语盘品罪": "走私、贩卖、运输、制造毒品罪",
}

# 差文本特征词
BAD_WORDS = [
    "量州", "人氏", "坩白", "十十五", "很所", "退晤", "器押", "秀地",
    "目百", "地以", "期定", "可法", "十自", "犯非",
    "王观恶性", "人当危险", "干要", "香任", "衍州", "拘符", "乱点", "基泪",
    "伯事故", "仿事故", "外下", "叫以", "卜列", "全再和罪", "很册很赔", "很册",
    "人氏法院", "可法机关", "法防",
]

# 量刑情节章节标题正则
SECTION_PATTERNS = [
    r"[三四五][\u3001\uff0e.]\s*常见量刑情节的适用",
    r"[三四五][\u3001\uff0e.]\s*常见量刑情节",
    r"常见量刑情节的适用",
    r"[\d一二三四五六七八九十]+\s*[.、．:：]\s*常见量刑情节",
    r"常见量刑情节",
]

# 量刑情节"对于"关键词
S_START_KW = [
    "对于未成年人犯罪", "对于未成年人犯",
    "对于已满七十五", "对于已满六十五", "对于年满六十五",
    "对于尚未完全丧失", "对于又聋又哑",
    "对于防卫过当", "对于预备犯", "对于未遂犯",
    "对于从犯", "对于自首", "对于立功",
    "对于又聋又哑", "对于盲人", "对于聋哑",
    "对于精神病人", "对于累犯", "对于犯罪未遂",
    "对于中止犯", "对于胁从犯", "对于教唆犯",
]

# 量刑情节特征词（判断编号后是否为量刑情节）
SENTENCING_FEATURES = [
    "未成年人", "老年人", "精神病", "聋", "哑", "盲",
    "防卫过当", "避险过当", "预备犯", "未遂犯", "中止犯",
    "从犯", "自首", "坦白", "立功",
    "累犯", "前科", "退赃", "退赔", "赔偿", "谅解",
    "被害人过错", "又聋又哑", "盲人犯罪", "精神病人",
    "认罪认罚", "刑事和解", "羁押期间",
    "教唆犯", "胁从犯", "犯罪未遂", "犯罪预备",
    "年满六十五", "年满七十五", "已满十四",
    "已满十六", "已满十二", "已满七十五",
]

# 应跳过的标题（章节标题级，非量刑情节条目）
SKIP_TITLES = [
    "量刑步骤", "确定宣告刑", "适用缓刑", "适用罚金",
    "宣告刑", "量刑起点", "基准刑", "计算单位", "通用原则",
    "常见犯罪的量刑", "常见罪名", "常见犯罪",
    "量刑的指导原则", "量刑的基本方法",
    "基准刑的确定", "量刑方法", "常见量刑情节",
]

# 罪名章节标题（用于定位区域终点）
CRIME_SECTION_PATTERNS = [
    r"[一二三四五六七八九十][\u3001\uff0e.]\s*常见犯罪",
    r"常见犯罪的量刑", "常见犯罪量刑", "常见犯罪的量用",
    r"[\d]+[.、．]\s*常见犯罪", "具体罪名", "个罪量刑",
    r"\d+[.、．:：]\s*常见罪名",
]

# LLM提示词
CRIME_PROMPT = """从OCR文本中提取"{crime_name}"的好文本。
规则：
1. 只提取好文本，忽略差文本（OCR乱码）
2. 只提取属于该罪名本身的量刑规定，不提取交叉引用
3. 不提取罚金、缓刑内容
4. 按编号子条目拆分，用"==="分隔
5. 每项第一行输出[DESC:简化描述]，描述10字以内
6. 忠于原文不改写
7. 无法识别则输出[NONE]

简化示例：
"法定刑在三年以下有期徒刑..." → "三年以下"
"其他可以增加刑罚量的情形" → "其他情形" """


# ====== 工具函数 ======

def _is_bad(s):
    return sum(1 for w in BAD_WORDS if w in s) >= 2


def _cn_idx(cn):
    try:
        return int(cn) - 1
    except (ValueError, TypeError):
        for i, c in enumerate(CN[1:]):
            if c == cn:
                return i
        return 99


def _is_sentencing_feat(text):
    """判断编号后面是否为量刑情节条目"""
    if "对于" in text:
        return True
    return any(feat in text for feat in SENTENCING_FEATURES)


def _is_toc_line(line):
    """判断是否是目录行"""
    if line.count(".") >= 5:
        return True
    if re.match(r'.+?\.{2,}\s*\d{1,3}\s*$', line.strip()):
        return True
    return False


def _is_skip_title(text):
    """判断是否是章节标题（非量刑情节）"""
    clean = text.replace(" ", "").replace("\n", "")
    return any(s in clean for s in SKIP_TITLES)


def _is_line_start_title(ft, pos):
    """检查pos处是否为行首的章节标题。
    
    排除正文中的引用（如"参照《关于常见犯罪的量刑指导意见》"），
    只匹配真正以编号开头的章节标题行。
    """
    line_start = ft.rfind(NL, 0, pos)
    line_prefix = ft[line_start + 1:pos].strip() if line_start >= 0 else ft[:pos].strip()
    if line_prefix and len(line_prefix) > 2:
        return False
    line_end = ft.find(NL, pos)
    line = ft[line_start + 1:line_end] if line_start >= 0 else ft[:line_end]
    return bool(re.match(r'\s*[一二三四五六七八九十\d]+[\u3001\uff0e.、]', line))


def _clean_name(name):
    """清理名称"""
    name = re.sub(r"[，。、\s]+$", "", name)
    name = re.sub(r"[，。、\s]{2,}", " ", name)
    return name.strip()


def _similar(a, b, threshold=0.8):
    """判断两个名称是否相似（用于好差文本去重）"""
    if not a or not b:
        return False
    if a == b:
        return True
    return SequenceMatcher(None, a, b).ratio() >= threshold


def _fix_crime_name(name):
    """尝试修复OCR错字罪名"""
    name = name.strip()
    if name in CRIME_OCR_FIXES:
        return CRIME_OCR_FIXES[name]
    for crime in KNOWN_CRIMES:
        if crime in name:
            return crime
    clean = re.sub(r'^[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*', '', name)
    clean = re.sub(r'^\d{1,2}[.、．:：]\s*', '', clean)
    clean = re.sub(r'^\d+\.\d+\s*', '', clean)
    for crime in KNOWN_CRIMES:
        if crime in clean:
            return crime
    if clean in CRIME_OCR_FIXES:
        return CRIME_OCR_FIXES[clean]
    return None


def load_json(p):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(p, d):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)


def sanitize(n):
    return re.sub(r'[<>:"/\\|?*]', "", n).strip()[:200]


def prov_name(info):
    return re.sub(
        r"[（(][一二三四五六七八九十\d]+[）)]", "",
        info.get("province", ""),
    ).strip()


def file_id(info):
    return f"{info.get('province', '')}_P{info.get('start', '')}-{info.get('end', '')}"


def pdf_text(ps, pe):
    doc = fitz.open(PDF_PATH)
    t = ""
    for p in range(ps, pe + 1):
        t += doc[p - 1].get_text() + NL
    doc.close()
    return t


# ====== 区域定位 ======

def _find_section_start(ft, patterns):
    """定位量刑情节章节的起点。
    
    v8.3改进：
    - 只匹配行首标题（排除正文引用）
    - 跳过目录行（含大量点号）
    - 跳过"标题后面全是子标题"的目录式标题
    """
    for pat in patterns:
        for m in re.finditer(pat, ft):
            pos = m.start()
            if not _is_line_start_title(ft, pos):
                continue
            ls = ft.rfind(NL, 0, pos)
            le = ft.find(NL, pos)
            line = ft[ls + 1:le] if ls >= 0 else ft[:le]
            if _is_toc_line(line):
                continue
            # 验证后面有正文内容，而非仅子标题列表
            after = ft[le + 1:le + 2000] if le >= 0 else ""
            cl = after.split(NL)
            sub_count = 0
            non_title = False
            for c in cl[:10]:
                c = c.strip()
                if not c:
                    continue
                if re.match(r'\d+\.\d+', c):
                    sub_count += 1
                elif len(c) > 20 and not re.match(r'\d', c):
                    non_title = True
                    break
            if sub_count >= 3 and not non_title:
                continue
            return pos
    return None


def _find_region_end(ft, rs):
    """定位量刑情节区域的终点。
    
    v8.3改进：
    - 只匹配行首章节标题（排除正文引用）
    """
    re_ = None
    for pat in CRIME_SECTION_PATTERNS:
        for m in re.finditer(pat, ft[rs + 200:]):
            pos = rs + 200 + m.start()
            if _is_line_start_title(ft, pos):
                ls = ft.rfind(NL, 0, pos)
                re_ = ls + 1 if ls >= 0 else pos
                break
        if re_:
            break

    # 兜底：第一个已知罪名的标题位置
    if re_ is None:
        best = len(ft)
        for crime in ["交通肇事罪", "危险驾驶罪", "故意伤害罪", "盗窃罪"]:
            idx = ft.find(crime, rs + 200)
            if 0 < idx < best:
                bef = ft[max(0, idx - 30):idx]
                if re.search(r'[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*$', bef) or \
                   re.search(r'\d{1,2}[.、．]\s*$', bef) or \
                   re.search(r'\d+\.\d+\s*$', bef):
                    best = idx
        if best < len(ft):
            ls = ft.rfind(NL, 0, best)
            re_ = ls + 1 if ls >= 0 else best

    if re_ is None:
        re_ = min(len(ft), rs + 20000)

    return re_


# ====== 量刑情节提取 ======

def extract_sentencing(ft):
    """通用量刑情节提取器 v8.3"""
    rs = _find_section_start(ft, SECTION_PATTERNS)

    if rs is None:
        # 关键词兜底
        for kw in S_START_KW:
            idx = ft.find(kw)
            if idx > 0:
                rs = max(0, idx - 200)
                break
    if rs is None:
        return []

    re_ = _find_region_end(ft, rs)
    reg = ft[rs:re_]

    # 收集编号位置
    nps = []
    for cn in CN[1:36]:
        for fmt in [FZ_L + cn + FZ_R, "(" + cn + ")"]:
            idx = 0
            while True:
                idx = reg.find(fmt, idx)
                if idx < 0:
                    break
                nps.append((idx, cn, "A"))
                idx += len(fmt)
    for m in re.finditer(r'(?:^|\n)\s*(\d{1,2})\s*[.、．]\s*', reg):
        nps.append((m.start(), m.group(1), "B"))
    for m in re.finditer(r'(?:^|\n)\s*(\d+\.\d+(?:\.\d+)?)\s+', reg):
        nps.append((m.start(), m.group(1), "C"))
    nps.sort()

    # 过滤：去重(5字符内) + 排除目录行
    filtered = []
    for pos, num, style in nps:
        if filtered and pos - filtered[-1][0] < 5:
            continue
        le = reg.find(NL, pos)
        line = reg[pos:le] if le >= 0 else reg[pos:pos + 100]
        if _is_toc_line(line):
            continue
        filtered.append((pos, num, style))
    if not filtered:
        return []

    # 切分并分类
    items = []
    seq = 0
    for i, (pos, num, style) in enumerate(filtered):
        end = filtered[i + 1][0] if i + 1 < len(filtered) else len(reg)
        sec = reg[pos:end].strip()
        if len(sec) < 15:
            continue
        bad = _is_bad(sec)

        # 去掉编号前缀
        an = re.sub(r"^[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*", "", sec)
        an = re.sub(r"^\d{1,2}[.、．]\s*", "", an)
        an = re.sub(r"^\d+\.\d+(?:\.\d+)?\s+", "", an)
        an = an.strip()
        first_line = an.split(NL)[0].strip() if an else ""

        if _is_skip_title(first_line) or _is_skip_title(an[:80]):
            continue
        if not first_line or len(first_line) <= 2:
            continue

        # 提取名称
        name = None
        di = an.find("对于")
        if di < 0:
            ni = an.find(NL + "对于")
            if ni >= 0:
                di = ni + 1
        if di >= 0:
            bef = an[:di].strip().replace(NL, " ").strip()
            dl = an[di:].split(NL)[0].strip()
            if bef and len(bef) > 1 and not _is_skip_title(bef):
                name = _clean_name(bef[:30])
            else:
                n = re.sub(r"^对于", "", dl)
                n = re.sub(r"[，。、]$", "", n).strip()
                name = _clean_name(n[:30]) if n else None
        else:
            if _is_sentencing_feat(an[:300]):
                clean = re.sub(r'[.。．]{2,}\s*\d*\s*$', '', first_line)
                clean = re.sub(r'^\d+\s+', '', clean)
                clean = re.sub(r'^\d+\.\d+\s+', '', clean)
                name = _clean_name(clean[:30]) if len(clean) > 1 else None
            else:
                continue

        if name is None or len(name) < 2:
            continue
        if "退赃、退赔" in name or "退赃退赔" in name:
            continue
        seq += 1
        items.append({"num": str(seq), "name": name, "content": sec, "is_bad": bad})

    # 去重（编辑距离0.8阈值）
    deduped = []
    for item in items:
        dup_idx = None
        for j, existing in enumerate(deduped):
            if _similar(item["name"], existing["name"], 0.8):
                dup_idx = j
                break
        if dup_idx is not None:
            existing = deduped[dup_idx]
            if not item["is_bad"] and existing["is_bad"]:
                deduped[dup_idx] = item
            elif not item["is_bad"] and not existing["is_bad"]:
                if len(item["content"]) > len(existing["content"]):
                    deduped[dup_idx] = item
        else:
            deduped.append(item)

    deduped.sort(key=lambda x: _cn_idx(x["num"]))
    return deduped


# ====== 罪名定位 ======

def find_crimes(ft):
    """查找文本中包含的所有罪名及其位置。v8.3"""
    found = []

    for crime in KNOWN_CRIMES:
        search_formats = [
            FZ_L + "一" + FZ_R + crime,
            FZ_L + "一" + FZ_R + NL + crime,
            "(" + "一" + ")" + crime,
            "(" + "一" + ")" + NL + crime,
            FZ_L + "一" + FZ_R + NL + NL + crime,
            "(" + "一" + ")" + NL + NL + crime,
            FZ_L + crime,
            "(" + crime,
            r"1[.、．]\s*" + crime,
            crime,
        ]
        for fmt in search_formats:
            idx = ft.find(fmt)
            if idx < 0:
                continue
            if idx > 0 and ft[idx - 1] not in (NL, ' ', '\t'):
                continue
            ls = ft.rfind(NL, 0, idx)
            pos = ls + 1 if ls >= 0 else idx
            existing_crimes = [f["crime"] for f in found]
            if crime not in existing_crimes:
                found.append({"pos": pos, "crime": crime})
            break

    # OCR错字修复
    for ocr_name, correct_name in CRIME_OCR_FIXES.items():
        if correct_name in [f["crime"] for f in found]:
            continue
        idx = ft.find(ocr_name)
        if idx > 0:
            ls = ft.rfind(NL, 0, idx)
            pos = ls + 1 if ls >= 0 else idx
            found.append({"pos": pos, "crime": correct_name})

    # "X. 罪名"格式的未知罪名（北京/四川/天津风格）
    for m in re.finditer(r'(?:^|\n)\s*(\d{1,2}[.、．]\s*)([\u4e00-\u9fff]{2,15}罪)\s', ft):
        pos = m.start()
        crime_candidate = m.group(2)
        existing = [f["crime"] for f in found]
        if crime_candidate not in existing:
            if "犯罪" in crime_candidate and len(crime_candidate) <= 8:
                continue
            fixed = _fix_crime_name(crime_candidate)
            if fixed and fixed not in existing:
                found.append({"pos": pos, "crime": fixed})
            elif crime_candidate not in existing:
                found.append({"pos": pos, "crime": crime_candidate})

    found.sort(key=lambda x: x["pos"])
    return found


# ====== API 客户端 ======

async def _api_call(client, prompt, sys_msg="你是专业的法律文书分析助手。"):
    url = API_BASE + "/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {API_KEY}"}
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": sys_msg},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
    }
    for attempt in range(MAX_RETRIES):
        try:
            async with client.post(url, json=payload, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 429:
                    wait = 2 ** attempt * 5
                    log.warning(f"    限流，等待 {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if resp.status != 200:
                    text = await resp.text()
                    log.error(f"    API {resp.status}: {text[:200]}")
                    return None
                data = await resp.json()
                return data["choices"][0]["message"]["content"]
        except Exception as e:
            log.error(f"    API异常 (重试 {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)
    return None


async def extract_crime(client, ft, crime_positions, crime):
    """提取单个罪名的内容"""
    ci = None
    for i, cp in enumerate(crime_positions):
        if cp["crime"] == crime:
            ci = i
            break
    if ci is None:
        return None
    pos = crime_positions[ci]["pos"]
    nxt = crime_positions[ci + 1]["pos"] if ci + 1 < len(crime_positions) else len(ft)
    chunk = ft[pos:nxt]
    if len(chunk) < 10:
        return None
    chunk = chunk[:6000]
    prompt = CRIME_PROMPT.format(crime_name=crime) + f"\n\n文本：\n{chunk}"
    return await _api_call(client, prompt)


# ====== 输出格式化 ======

def _fmt_sentencing_item(item):
    lines = [f"# {item['name']}", ""]
    content = item["content"]
    content = re.sub(r"^[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*", "", content)
    content = re.sub(r"^\d{1,2}[.、．]\s*", "", content)
    content = re.sub(r"^\d+\.\d+(?:\.\d+)?\s+", "", content)
    lines.append(content.strip())
    lines.append("")
    if item["is_bad"]:
        lines.append("> OCR质量较差，以下为原始差文本，仅供参考")
        lines.append("")
    return "\n".join(lines)


def _fmt_crime(name, result):
    if not result or "[NONE]" in result:
        return f"# {name}\n\n> 未提取到有效内容\n"
    parts = [f"# {name}", ""]
    for seg in result.split("==="):
        seg = seg.strip()
        if not seg:
            continue
        if seg.startswith("[DESC:"):
            lines = seg.split("\n")
            desc = lines[0].replace("[DESC:", "").rstrip("]")
            body = "\n".join(lines[1:]).strip()
            parts.append(f"## {desc}")
            if body:
                parts.append("")
                parts.append(body)
        else:
            parts.append(seg)
        parts.append("")
    return "\n".join(parts)


# ====== 处理单个文件 ======

async def process_one(client, info, semaphore):
    async with semaphore:
        t0 = time.time()
        pid = file_id(info)
        pname = prov_name(info)
        ps, pe = info["start"], info["end"]
        log.info(f"[{pid}] 读取 P{ps}-{pe}")

        ft = pdf_text(ps, pe)

        # 1) 量刑情节
        s_items = extract_sentencing(ft)
        sc = len(s_items)
        log.info(f"[{pid}] 情节={sc}")

        # 2) 罪名
        crime_positions = find_crimes(ft)
        cc = len(crime_positions)
        log.info(f"[{pid}] 罪名={cc}")

        # 3) LLM提取罪名内容
        crime_results = {}
        if cc > 0:
            for i, cp in enumerate(crime_positions):
                crime = cp["crime"]
                log.info(f"[{pid}] 罪名 ({i + 1}/{cc}) {crime}")
                result = await extract_crime(client, ft, crime_positions, crime)
                if result:
                    crime_results[crime] = result
                await asyncio.sleep(REQ_INTERVAL)

        # 4) 写入文件
        pdir = os.path.join(OUTPUT, sanitize(pname))
        os.makedirs(pdir, exist_ok=True)

        for item in s_items:
            fn = sanitize(item["name"]) + ".md"
            with open(os.path.join(pdir, fn), "w", encoding="utf-8") as f:
                f.write(_fmt_sentencing_item(item))

        for crime, result in crime_results.items():
            fn = sanitize(crime) + ".md"
            with open(os.path.join(pdir, fn), "w", encoding="utf-8") as f:
                f.write(_fmt_crime(crime, result))

        dt = time.time() - t0
        log.info(f"[{pid}] 完成: {pname} | 情节={sc} 罪名={len(crime_results)}/{cc} | {dt:.0f}s")
        return {"province": pname, "s": sc, "c": len(crime_results), "c_total": cc, "t": round(dt)}


# ====== 主流程 ======

async def main():
    cat = load_json(CATALOG)
    files = cat.get("全国性文件", []) + cat.get("省市级文件", [])
    log.info(f"总任务: {len(files)} 个文件")

    prog = load_json(PROGRESS) if os.path.exists(PROGRESS) else {"done": [], "stats": {}}
    done_ids = set(prog.get("done", []))
    remaining = [f for f in files if file_id(f) not in done_ids]
    log.info(f"已完成: {len(done_ids)}, 剩余: {len(remaining)}")

    if not remaining:
        log.info("全部完成！")
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession() as client:
        for i, info in enumerate(remaining):
            pid = file_id(info)
            log.info(f"\n[{i + 1}/{len(remaining)}] {pid}")
            try:
                result = await process_one(client, info, semaphore)
                prog["done"].append(pid)
                prog["stats"][pid] = result
                save_json(PROGRESS, prog)
            except Exception as e:
                log.error(f"[{pid}] 失败: {e}")
                import traceback
                traceback.print_exc()

    log.info("\n全量提取完成！")
    for pid, st in prog["stats"].items():
        c_total = st.get("c_total", st["c"])
        log.info(f"  {st['province']:10s} | 情节={st['s']} 罪名={st['c']}/{c_total} | {st['t']}s")


if __name__ == "__main__":
    asyncio.run(main())