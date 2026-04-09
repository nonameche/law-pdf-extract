# -*- coding: utf-8 -*-
"""
量刑指导意见全量提取脚本 v9.4

严格按照 提取规则_v3.md 规范输出。

v9.4 修复:
- 量刑情节提取支持阿拉伯数字编号 (1. 2. ... 26.)
- 罪名子条目切分支持 "第X个量刑幅度" 格式
- 排除量刑指导原则区域的误识别为罪名
"""

import json, os, re, sys, time, logging
import fitz
from difflib import SequenceMatcher

# ====== 配置 ======
PDF_PATH = r"E:\工作\启科律行\知识库\量刑\20250901全国各省市量刑指导意见实施细则汇编(OCR).pdf"
CATALOG = r"E:\工作\启科律行\知识库\量刑\代码\final_catalog_v7.json"
BASE_DIR = r"E:\工作\启科律行\知识库\量刑"
OUTPUT = os.path.join(BASE_DIR, "输出")
TEST_DIR = os.path.join(BASE_DIR, "测试")
PROGRESS = r"E:\工作\启科律行\知识库\量刑\代码\extract_progress_v9.json"
LOG_FILE = r"E:\工作\启科律行\知识库\量刑\代码\extract_v9.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("extract_v9")

# ====== 常量 ======
NL = "\n"
FZ_L = "\uff08"
FZ_R = "\uff09"

CN = [
    "零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十",
    "十一", "十二", "十三", "十四", "十五", "十六", "十七", "十八",
    "十九", "二十", "二十一", "二十二", "二十三", "二十四", "二十五",
    "二十六", "二十七", "二十八", "二十九", "三十",
    "三十一", "三十二", "三十三", "三十四", "三十五",
]

BAD_OCR_PROVINCES = {"黑龙江", "山东"}

BAD_WORDS = [
    "娴定", "置告", "根括", "符殊", "娘高", "豆告", "预行",
    "甲处", "究除", "处词", "个得", "个满", "开词", "造用",
    "刑期个", "为牛", "半牛", "十牛", "二牛", "五牛",
    "法空", "单外", "羽金", "低害", "补会", "信犯",
    "申判", "独仕", "考愿", "硼定", "犯非", "非贡",
    "香任", "被需", "该解", "矛店", "目百", "觅得",
    "坛白", "很册", "退晤", "十自", "十十", "人氏",
    "量州", "可法", "十十五", "十白", "全再和罪",
    "很册很赔", "很所", "坩白", "器押", "秀地",
    "乱点", "基泪", "仿事故", "叫以", "卜列",
    "以乃", "检烈", "法降", "亡省", "昌级", "决防",
    "祭院", "印足", "昌关", "陈布", "忠列",
    "才懈能", "秀现", "才力", "赐偿", "教翻",
    "问颞", "扬失", "人当危险", "王观恶性",
]

PROVINCE_LEGAL_BASIS = {
    "全国（一）": ("最高人民法院 最高人民检察院《关于常见犯罪的量刑指导意见（试行）》", "法发〔2017〕7号"),
    "全国（二）": ("最高人民法院 最高人民检察院《关于常见犯罪的量刑指导意见（二）（试行）》", "法发〔2017〕9号"),
    "安徽（一）": ("安徽省《关于二十三种常见犯罪量刑规范的实施细则（试行）》", "皖高法〔2022〕1号"),
    "北京（一）": ("北京市《关于常见犯罪的量刑指导意见》实施细则（试行）", "待识别"),
    "重庆（一）": ("重庆市《关于常见犯罪的量刑指导意见（试行）》实施细则", "待识别"),
    "福建（一）": ("福建省《关于常见犯罪的量刑指导意见》实施细则", "待识别"),
    "福建（二）": ("福建省《关于常见犯罪的量刑指导意见（二）（试行）》实施细则", "闽高法〔2025〕21号"),
    "甘肃（一）": ("甘肃省《关于常见犯罪的量刑指导意见（试行）》实施细则", "甘高法发〔2022〕19号"),
    "广东（一）": ("广东省《关于实施修订后的〈关于常见犯罪的量刑指导意见〉实施细则》的通知", "粤高法发〔2017〕6号"),
    "广东（二）": ("广东省《关于常见犯罪的量刑指导意见（二）》实施细则（试行）", "粤高法发〔2017〕7号"),
    "广西（一）": ("广西壮族自治区《关于常见犯罪的量刑指导意见》实施细则（试行）", "桂高法会〔2021〕12号"),
    "广西（二）": ("广西壮族自治区《关于常见犯罪的量刑指导意见（二）（试行）》实施细则", "待识别"),
    "贵州（一）": ("贵州省《关于常见犯罪的量刑指导意见（试行）》实施细则", "黔高法〔2022〕142号"),
    "海南（一）": ("海南省《关于常见犯罪的量刑指导意见》实施细则（试行）", "琼高法联〔2021〕10号"),
    "河北（一）": ("河北省《关于常见犯罪的量刑指导意见（试行）》实施细则", "待识别"),
    "河南（一）": ("河南省《关于常见犯罪的量刑指导意见（试行）》实施细则", "豫高法〔2024〕158号"),
    "黑龙江（一）": ("黑龙江省《关于常见犯罪的量刑指导意见》实施细则", "待识别"),
    "湖北（一）": ("湖北省《关于常见犯罪的量刑指导意见》实施细则", "待识别"),
    "湖南（一）": ("湖南省《关于常见犯罪的量刑指导意见（试行）》实施细则", "湘高法发〔2023〕4号"),
    "吉林（一）": ("吉林省《关于常见犯罪的量刑指导意见（试行）》实施细则", "吉高法〔2023〕154号"),
    "吉林（二）": ("吉林省《关于常见犯罪的量刑指导意见（二）（试行）》实施细则", "吉高法〔2024〕186号"),
    "江苏（一）": ("江苏省《关于常见犯罪的量刑指导意见（试行）》实施细则", "苏高法〔2023〕114号"),
    "江西（一）": ("江西省《关于常见犯罪的量刑指导意见（试行）》实施细则", "赣高法〔2022〕115号"),
    "江西（二）": ("江西省《关于常见犯罪的量刑指导意见（二）（试行）》实施细则", "赣高法〔2025〕20号"),
    "辽宁（一）": ("辽宁省《关于常见犯罪的量刑指导意见（试行）》实施细则", "待识别"),
    "辽宁（二）": ("辽宁省《关于常见犯罪的量刑指导意见（二）（试行）》实施细则", "待识别"),
    "辽宁（三）": ("辽宁省《关于常见犯罪的量刑指导意见》实施细则（三）", "待识别"),
    "内蒙古（一）": ("内蒙古自治区《关于常见犯罪的量刑指导意见（试行）》实施细则", "待识别"),
    "内蒙古（二）": ("内蒙古自治区《关于常见犯罪的量刑指导意见（二）（试行）》实施细则", "内高法〔2025〕70号"),
    "宁夏（一）": ("宁夏回族自治区《关于常见犯罪的量刑指导意见（试行）》实施细则", "宁高法〔2022〕85号"),
    "青海（一）": ("青海省《关于常见犯罪的量刑指导意见》实施细则", "待识别"),
    "山东（一）": ("山东省《关于常见犯罪的量刑指导意见（试行）》实施细则", "待识别"),
    "山西（一）": ("山西省《关于常见犯罪的量刑指导意见（试行）》实施细则", "待识别"),
    "陕西（一）": ("陕西省《关于常见犯罪的量刑指导意见（试行）》实施细则", "待识别"),
    "上海（一）": ("上海市《关于常见犯罪的量刑指导意见》实施细则", "待识别"),
    "上海（二）": ("上海市《关于常见犯罪的量刑指导意见（二）（试行）》实施细则", "待识别"),
    "四川（一）": ("四川省《关于常见犯罪的量刑指导意见（试行）》实施细则", "待识别"),
    "天津（一）": ("天津市《关于常见犯罪的量刑指导意见》实施细则", "津高法发〔2021〕6号"),
    "西藏（一）": ("西藏自治区《关于常见犯罪的量刑指导意见（试行）》实施细则", "藏高法发〔2022〕14号"),
    "新疆（一）": ("新疆维吾尔自治区《关于常见犯罪的量刑指导意见》实施细则", "待识别"),
    "新疆（二）": ("新疆维吾尔自治区《关于常见犯罪的量刑指导意见（二）（试行）》实施细则", "新高法发〔2025〕4号"),
    "云南（一）": ("云南省《关于常见犯罪的量刑指导意见》实施细则", "云高法〔2018〕86号"),
    "浙江（一）": ("浙江省《关于常见犯罪的量刑指导意见（试行）》实施细则", "浙高法审〔2022〕1号"),
}

CRIME_OCR_FIXES = {
    "交通空导罪": "交通肇事罪", "交通学事罪": "交通肇事罪",
    "交通晕事罪": "交通肇事罪", "交通事罪": "交通肇事罪",
    "交通量事罪": "交通肇事罪", "交通签事罪": "交通肇事罪",
    "危险当驶罪": "危险驾驶罪", "危险驾段罪": "危险驾驶罪",
    "危险驾歌罪": "危险驾驶罪", "后险驾驶罪": "危险驾驶罪",
    "非法尚禁罪": "非法拘禁罪", "非法拘崇罪": "非法拘禁罪",
    "诈编罪": "诈骗罪", "诈确罪": "诈骗罪", "诈翳罪": "诈骗罪",
    "诈膈罪": "诈骗罪", "诈痛罪": "诈骗罪", "诈贻罪": "诈骗罪",
    "益窃罪": "盗窃罪", "益访罪": "盗窃罪", "益场罪": "盗窃罪",
    "抢奇罪": "抢夺罪", "抢存罪": "抢夺罪", "抢动罪": "抢劫罪",
    "强好罪": "强奸罪", "鱼好罪": "强奸罪",
    "敲诈勤索罪": "敲诈勒索罪", "敲诈勒素罪": "敲诈勒索罪",
    "聚众斗政罪": "聚众斗殴罪", "聚众斗酸罪": "聚众斗殴罪",
    "寻蚌滋事罪": "寻衅滋事罪", "导衅滋事罪": "寻衅滋事罪",
    "合同诈编罪": "合同诈骗罪", "合同许骗罪": "合同诈骗罪",
    "信用卡诈端罪": "信用卡诈骗罪", "信用卡详骗罪": "信用卡诈骗罪",
    "集资诈编罪": "集资诈骗罪",
    "职劳侵占罪": "职务侵占罪", "职旁夜古罪": "职务侵占罪",
    "坊害公务罪": "妨害公务罪", "妨害么务罪": "妨害公务罪",
    "开设酱场罪": "开设赌场罪", "开设籍场罪": "开设赌场罪",
    "狼亵儿童罪": "猥亵儿童罪", "狼袭儿量罪": "猥亵儿童罪",
    "组织章浮罪": "组织卖淫罪",
    "容苗他人吸毒罪": "容留他人吸毒罪",
    "3E法经营罪": "非法经营罪", "故意伤善罪": "故意伤害罪",
}

KNOWN_CRIMES = [
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
    "开设赌场罪", "帮助信息网络犯罪活动罪",
    "拒不执行判决、裁定罪", "侵犯公民个人信息罪",
    "非法经营罪", "组织卖淫罪", "猥亵儿童罪",
    "污染环境罪", "故意毁坏财物罪",
]

# 量刑情节区域标题
SECTION_PATTERNS = [
    r"常见量刑情节的适用",
    r"[三四五六七八九十][\u3001.]\s*常见量刑情节",
    r"\d+[.、]\s*常见量刑情节",
]

# 量刑情节区域结束标志（罪名区域开始）
CRIME_SECTION_PATTERNS = [
    r"[四五][\u3001.]\s*常见犯罪",
    r"[四五]、常见犯罪",
    r"常见犯罪的量刑",
    r"常见犯罪量刑",
    r"常见犯罪的量[刑刑用]",
    r"\d+[.、]\s*常见犯罪",
    r"具体罪名",
    r"个罪量刑",
]

# 量刑情节识别特征词
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

# 量刑情节关键词（匹配到的段落开头）
SENTENCING_STARTERS = [
    "对于未成年人犯罪", "对于未成年人犯",
    "对于已满七十五", "对于已满六十五", "对于年满六十五",
    "对于尚未完全丧失", "对于又聋又哑",
    "对于防卫过当", "对于预备犯", "对于未遂犯",
    "对于从犯", "对于自首", "对于立功",
    "对于又聋又哑", "对于盲人", "对于聋哑",
    "对于精神病人", "对于累犯", "对于犯罪未遂",
    "对于中止犯", "对于胁从犯", "对于教唆犯",
    "对于退赃", "对于坦白",
    "对于当庭自愿认罪", "对于被害人", "对于当事人",
    "对于犯罪嫌疑人", "对于被告人认罪",
    "对于有前科", "对于犯罪对象",
    "对于在重大自然灾害",
]

# 应跳过的标题
SKIP_TITLES = [
    "量刑步骤", "确定宣告刑", "适用缓刑", "适用罚金",
    "宣告刑", "计算单位", "通用原则",
    "常见犯罪的量刑", "常见罪名", "常见犯罪",
    "量刑的指导原则", "量刑的基本方法",
    "基准刑的确定", "量刑方法",
    "常见量刑情节",
    "量刑时要充分考虑", "量刑应当",
    "量刑既要考虑",
]

# ====== 工具函数 ======

def _is_toc_line(line):
    if line.count(".") >= 5:
        return True
    return bool(re.match(r'.+?\.{2,}\s*\d{1,3}\s*$', line.strip()))


def _is_line_start_title(ft, pos):
    line_start = ft.rfind(NL, 0, pos)
    line_prefix = ft[line_start + 1:pos].strip() if line_start >= 0 else ft[:pos].strip()
    if line_prefix and len(line_prefix) > 2:
        return False
    line_end = ft.find(NL, pos)
    line = ft[line_start + 1:line_end] if line_start >= 0 else ft[:line_end]
    return bool(re.match(r'\s*[一二三四五六七八九十\d]+[\u3001\uff0e.、]', line))


def _is_skip_title(text):
    clean = text.replace(" ", "").replace(NL, "")
    return any(s in clean for s in SKIP_TITLES)


def _is_sentencing_feat(text):
    if "对于" in text:
        return True
    return any(feat in text for feat in SENTENCING_FEATURES)


def _fix_crime_name(name):
    if name in CRIME_OCR_FIXES:
        return CRIME_OCR_FIXES[name]
    for crime in KNOWN_CRIMES:
        if crime in name:
            return crime
    return None


def _clean_filename(s):
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _cn_idx(cn):
    try:
        return int(cn) - 1
    except (ValueError, TypeError):
        for i, c in enumerate(CN[1:]):
            if c == cn:
                return i
        return 99


def _text_quality(text):
    score = 0
    score += len(re.findall(r'[\uff0c\u3002\u3001\uff1b\uff1a]', text)) * 2
    score += len(re.findall(r'[\uff08\uff09]', text)) * 2
    for w in BAD_WORDS:
        if w in text:
            score -= 3
    score -= len(re.findall(r'[,.]', text))
    return score


# ====== PDF读取（block级双层文本过滤）======

def pdf_text(ps, pe):
    doc = fitz.open(PDF_PATH)
    pages = []
    for p in range(ps, pe + 1):
        page = doc[p - 1]
        blocks = page.get_text("dict")["blocks"]
        entries = []
        for b in blocks:
            if "lines" not in b:
                continue
            text = "".join(span["text"] for line in b["lines"] for span in line["spans"])
            text = text.strip()
            if not text or len(text) < 3:
                continue
            y0 = b["bbox"][1]
            x0 = b["bbox"][0]
            q = _text_quality(text)
            entries.append((y0, x0, text, q))
        entries.sort()
        good = []
        i = 0
        while i < len(entries):
            y, x, text, q = entries[i]
            group = [(y, x, text, q)]
            j = i + 1
            while j < len(entries) and entries[j][0] - y < 5:
                group.append(entries[j])
                j += 1
            if len(group) == 1:
                good.append(text)
            else:
                best = max(group, key=lambda g: g[3])
                good.append(best[2])
            i = j
        pages.append(NL.join(good))
    doc.close()
    return NL.join(pages)


# ====== 区域定位 ======

def _find_section_start(ft, patterns):
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
            return pos
    # fallback: 搜索第一个量刑情节关键词
    for kw in SENTENCING_STARTERS:
        idx = ft.find(kw)
        if idx > 200:
            # 往前找编号/标题
            ls = ft.rfind(NL, 0, idx)
            prefix = ft[max(0, idx - 200):idx]
            if re.search(r'\d{1,2}[.、．]\s*$', prefix.split(NL)[-1] if NL in prefix else prefix):
                return max(0, idx - 200)
            return idx
    return None


def _find_region_end(ft, rs):
    """找量刑情节区域的结束位置（罪名区域开始）"""
    for pat in CRIME_SECTION_PATTERNS:
        for m in re.finditer(pat, ft[rs + 100:]):
            pos = rs + 100 + m.start()
            if _is_line_start_title(ft, pos):
                ls = ft.rfind(NL, 0, pos)
                return ls + 1 if ls >= 0 else pos
    # fallback: 搜索第一个已知罪名出现
    best = len(ft)
    for crime in ["交通肇事罪", "危险驾驶罪", "故意伤害罪", "盗窃罪", "非法吸收公众存款罪"]:
        idx = ft.find(crime, rs + 100)
        if 0 < idx < best:
            bef = ft[max(0, idx - 30):idx]
            if re.search(r'[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*$', bef) or \
               re.search(r'\d{1,2}[.、．:：)]\s*$', bef):
                best = idx
    if best < len(ft):
        ls = ft.rfind(NL, 0, best)
        return ls + 1 if ls >= 0 else best
    return min(len(ft), rs + 20000)


def _find_principle_end(ft):
    """找量刑指导原则的结束位置"""
    for pat in SECTION_PATTERNS:
        idx = ft.find("常见量刑情节")
        if idx > 0:
            return idx
    for kw in SENTENCING_STARTERS:
        idx = ft.find(kw)
        if idx > 200:
            return idx
    return None


# ====== 量刑情节提取 ======

def extract_sentencing(ft):
    """提取常见量刑情节区域的所有条目"""
    rs = _find_section_start(ft, SECTION_PATTERNS)
    if rs is None:
        return []

    re_ = _find_region_end(ft, rs)
    reg = ft[rs:re_]
    if len(reg) < 50:
        return []

    # 收集所有编号位置
    nps = []  # (pos, num_text, style)

    # 中文编号：（一）（二）...
    for cn in CN[1:36]:
        for fmt in [FZ_L + cn + FZ_R, "(" + cn + ")"]:
            idx = 0
            while True:
                idx = reg.find(fmt, idx)
                if idx < 0:
                    break
                nps.append((idx, cn, "zh"))
                idx += len(fmt)

    # 阿拉伯数字编号：1. 2. ... 26.
    for m in re.finditer(r'(?:^|\n)\s*(\d{1,2})\s*[.、．:：]\s*', reg):
        nps.append((m.start(), m.group(1), "ar"))

    # 层级编号：4.1 4.2
    for m in re.finditer(r'(?:^|\n)\s*(\d+\.\d+(?:\.\d+)?)\s+', reg):
        nps.append((m.start(), m.group(1), "sub"))

    nps.sort()

    # 去重（5字符内的多个编号只保留第一个）
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

    # 切分条目
    items = []
    seq = 0
    for i, (pos, num, style) in enumerate(filtered):
        end = filtered[i + 1][0] if i + 1 < len(filtered) else len(reg)
        sec = reg[pos:end].strip()
        if len(sec) < 15:
            continue

        # 去掉编号前缀，获取正文
        an = re.sub(r"^[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*", "", sec)
        an = re.sub(r"^\d{1,2}[.、．:：]\s*", "", an)
        an = re.sub(r"^\d+\.\d+(?:\.\d+)?\s+", "", an)
        an = an.strip()
        first_line = an.split(NL)[0].strip() if an else ""

        # 跳过非量刑情节的标题
        if _is_skip_title(first_line) or _is_skip_title(an[:80]):
            continue

        if not first_line or len(first_line) <= 2:
            continue

        # 提取情节名称
        name = _extract_sentencing_name(an, first_line)

        if name is None or len(name) < 2:
            continue

        seq += 1
        items.append({"num": str(seq), "name": name, "content": sec})

    return _dedup_sentencing(items)


def _extract_sentencing_name(an, first_line):
    """从正文中提取量刑情节名称"""
    # 策略1：找"对于...的"模式
    di = an.find("对于")
    if di < 0:
        ni = an.find(NL + "对于")
        if ni >= 0:
            di = ni + 1

    if di >= 0:
        bef = an[:di].strip().replace(NL, " ").strip()
        dl = an[di:].split(NL)[0].strip()

        if bef and len(bef) > 1 and not _is_skip_title(bef):
            # 如 "1.对于..." → bef可能是 "1"
            if re.match(r'^\d{1,2}$', bef):
                name = re.sub(r"[，。、\s]+$", "", dl[:30])
            else:
                name = re.sub(r"[，。、\s]+$", "", bef[:30])
        else:
            n = re.sub(r"^对于", "", dl)
            n = re.sub(r"[，。、]$", "", n).strip()
            name = re.sub(r"[，。、\s]+$", "", n[:30]) if n else None
    elif _is_sentencing_feat(an[:300]):
        # 没有"对于"但有量刑特征词
        clean = re.sub(r'[.。．]{2,}\s*\d*\s*$', '', first_line)
        clean = re.sub(r'^\d+\s+', '', clean)
        clean = re.sub(r'^\d+\.\d+\s+', '', clean)
        name = re.sub(r"[，。、\s]+$", "", clean[:30]) if len(clean) > 1 else None
    else:
        return None

    return name


def _dedup_sentencing(items):
    """去重并清理名称"""
    deduped = []
    for item in items:
        dup = None
        for j, ex in enumerate(deduped):
            if not ex.get("name") or not item["name"]:
                continue
            if ex["name"] == item["name"]:
                dup = j
                break
            if SequenceMatcher(None, ex["name"], item["name"]).ratio() >= 0.8:
                dup = j
                break
        if dup is not None:
            if len(item["content"]) > len(deduped[dup]["content"]):
                deduped[dup] = item
        else:
            deduped.append(item)

    # 清理过长名称
    for item in deduped:
        if len(item["name"]) > 15:
            short = re.split(r'[，。、,.]', item["name"])[0].strip()
            if len(short) >= 2:
                item["name"] = short

    deduped.sort(key=lambda x: _cn_idx(x["num"]))
    return deduped


# ====== 罪名定位 ======

def find_crimes(ft):
    """定位全文中的所有罪名"""
    # 先确定量刑情节区域，罪名只在该区域之后搜索
    principle_end = _find_principle_end(ft)
    search_start = principle_end if principle_end else 0

    found = []
    existing_names = []

    # 策略1: 直接搜索已知罪名
    for crime in KNOWN_CRIMES:
        if crime in existing_names:
            continue
        idx = search_start
        candidates = []
        while True:
            idx = ft.find(crime, idx)
            if idx < 0 or idx < search_start:
                break
            bef = ft[max(search_start, idx - 60):idx]
            ok = False
            if re.search(r'[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*$', bef):
                ok = True
            elif re.search(r'\d{1,2}[.、．:：)]\s*$', bef):
                ok = True
            elif bef.rstrip().endswith("构成"):
                ok = True
            elif idx <= search_start + 5 or ft[idx - 1] in (NL, ' ', '\t'):
                after = ft[idx + len(crime):idx + len(crime) + 5]
                if after.startswith(NL) or not after.strip():
                    ok = True
            if ok:
                ls = ft.rfind(NL, 0, idx)
                pos = ls + 1 if ls >= 0 else idx
                candidates.append((pos, crime))
            idx += len(crime)

        if candidates:
            found.append({"pos": candidates[0][0], "crime": crime})
            existing_names.append(crime)

    # 策略2: OCR错字修复
    for ocr_name, correct_name in CRIME_OCR_FIXES.items():
        if correct_name in existing_names:
            continue
        idx = ft.find(ocr_name, search_start)
        if idx >= search_start:
            ls = ft.rfind(NL, 0, idx)
            pos = ls + 1 if ls >= 0 else idx
            found.append({"pos": pos, "crime": correct_name})
            existing_names.append(correct_name)

    # 策略3: 正则搜索 "X. 罪名"
    for m in re.finditer(r'(?:^|\n)\s*(\d{1,2}[.、．:：)]\s*)([\u4e00-\u9fff]{2,15}罪)', ft[search_start:]):
        crime_cand = m.group(2)
        abs_pos = search_start + m.start()
        if crime_cand in existing_names:
            continue
        fixed = _fix_crime_name(crime_cand)
        if fixed and fixed not in existing_names:
            found.append({"pos": abs_pos, "crime": fixed})
            existing_names.append(fixed)
        elif crime_cand not in existing_names and not ("犯罪" in crime_cand and len(crime_cand) <= 8):
            found.append({"pos": abs_pos, "crime": crime_cand})
            existing_names.append(crime_cand)

    # 过滤假罪名
    filtered = []
    for item in found:
        c = item["crime"]
        if c.startswith("构成"):
            continue
        if "本实施细则" in c or "本细则" in c:
            continue
        if "规范上列" in c or "仅规范" in c:
            continue
        filtered.append(item)
    filtered.sort(key=lambda x: x["pos"])
    return filtered


# ====== 罪名条目切分 ======

def split_crime_items(ft, crime_info, crimes_list):
    """将一个罪名的文本按子条目拆分"""
    ci = {item["crime"]: i for i, item in enumerate(crimes_list)}
    if crime_info["crime"] not in ci:
        return []
    pos = ci[crime_info["crime"]]
    nxt = crimes_list[pos + 1]["pos"] if pos + 1 < len(crimes_list) else len(ft)
    chunk = ft[crime_info["pos"]:nxt]
    if len(chunk) < 20:
        return []

    # 去掉罪名的标题行（第一行的编号+罪名）
    first_nl = chunk.find(NL)
    if first_nl > 0 and first_nl < 200:
        first_line = chunk[:first_nl].strip()
        if re.search(r'[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*\S+罪', first_line) or \
           re.search(r'\d{1,2}[.、．]\s*\S+罪', first_line):
            chunk = chunk[first_nl:].strip()

    # 收集所有编号位置
    sub_pat = re.compile(
        r'(?:^|\n)'          # 行首
        r'\s*'
        r'(?:'
        r'[\uff08(][一二三四五六七八九十]+[\uff09)]'   # （一）
        r'|'
        r'第[一二三四五六七八九十]+个量刑幅度'         # 第一个量刑幅度
        r'|'
        r'\d{1,2}[.、．)\uff09]'                       # 1. 或 1）
        r'|'
        r'\d+\.\d+'                                     # 4.1
        r')'
        r'\s+'
    )

    matches = list(sub_pat.finditer(chunk))
    if not matches:
        return [{"desc": "情节一般", "content": chunk.strip()}]

    sub_starts = [m.end() for m in matches]
    if not sub_starts:
        return [{"desc": "情节一般", "content": chunk.strip()}]

    sub_items = []
    for i, start in enumerate(sub_starts):
        end = sub_starts[i + 1] if i + 1 < len(sub_starts) else len(chunk)
        sec = chunk[start:end].strip()
        if len(sec) < 10:
            continue

        # 提取子条目描述
        first_line = sec.split(NL)[0].strip()
        first_line = re.sub(r'^[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*', '', first_line)
        first_line = re.sub(r'^第[一二三四五六七八九十]+个量刑幅度\s*', '', first_line)
        first_line = re.sub(r'^\d{1,2}[.、．)\uff09]\s*', '', first_line)
        first_line = re.sub(r'^\d+\.\d+\s+', '', first_line)
        first_line = first_line.strip()

        # 检查是否包含"量刑幅度"等关键词
        desc = _extract_crime_desc(first_line, sec, i)

        sub_items.append({"desc": desc, "content": sec})

    return sub_items


def _extract_crime_desc(first_line, sec, idx):
    """提取罪名子条目的描述"""
    # 优先匹配量刑幅度描述
    amp_patterns = [
        r'(\S{2,30}(?:量刑幅度|起点))',
        r'(\S{2,30}(?:数额较大|数额巨大|数额特别巨大))',
        r'(\S{2,30}(?:情节一般|情节严重|情节特别严重|情节较轻))',
        r'(\S{2,30}(?:致人重伤|致人死亡|致人轻伤))',
        r'(\S{2,30}(?:犯罪情节一般|有其他严重情节|有其他特别严重情节))',
    ]
    for pat in amp_patterns:
        m = re.search(pat, sec[:200])
        if m:
            desc = m.group(1).strip()
            # 去掉编号
            desc = re.sub(r'^[\uff08(][一二三四五六七八九十\d]+[\uff09)]\s*', '', desc)
            desc = re.sub(r'^\d{1,2}[.、．)\uff09]\s*', '', desc)
            desc = re.sub(r'^\d+\.\d+\s+', '', desc)
            desc = desc.strip()
            if 2 <= len(desc) <= 30:
                return desc

    # fallback
    desc = re.split(r'[，。,.;；]', first_line)[0].strip()
    if not desc or len(desc) < 2:
        desc = "条目" + str(idx + 1)
    return desc


# ====== 输出文件 ======

def write_sentencing_file(output_dir, province_name, item, legal_basis_text):
    filename = "量刑【" + item["name"] + "】" + province_name + ".md"
    filepath = os.path.join(output_dir, _clean_filename(filename))
    content = item["content"].strip()
    if _is_bad(content):
        content = content + "\n[OCR无法识别部分已省略]"
    content = content + "\n法律依据：" + legal_basis_text
    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    return filepath


def write_crime_file(output_dir, province_name, crime_name, sub_item, legal_basis_text):
    desc = sub_item["desc"]
    filename = "量刑【" + crime_name + "】" + province_name + "（" + desc + "）.md"
    filepath = os.path.join(output_dir, _clean_filename(filename))
    content = sub_item["content"].strip()
    if _is_bad(content):
        content = content + "\n[OCR无法识别部分已省略]"
    content = content + "\n法律依据：" + legal_basis_text
    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    return filepath


def _is_bad(s):
    return sum(1 for w in BAD_WORDS if w in s) >= 2


# ====== 进度管理 ======

def load_progress():
    if os.path.exists(PROGRESS):
        with open(PROGRESS, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"done": [], "stats": {}}


def save_progress(prog):
    with open(PROGRESS, "w", encoding="utf-8") as f:
        json.dump(prog, f, ensure_ascii=False, indent=2)


# ====== 省份名解析 ======

def get_province_output_name(raw_province):
    m = re.match(r'(.+?)[（(](\d+)[）)]', raw_province)
    base = m.group(1) if m else raw_province
    base = base.strip()
    if base == "全国":
        cn_map = {"1": "一", "2": "二", "3": "三", "4": "四", "5": "五"}
        file_num = m.group(2) if m else "1"
        return "全国（" + cn_map.get(file_num, file_num) + "）"
    auto_map = {
        "广西": "广西壮族自治区",
        "内蒙古": "内蒙古自治区",
        "宁夏": "宁夏回族自治区",
        "西藏": "西藏自治区",
        "新疆": "新疆维吾尔自治区",
    }
    if base in auto_map:
        return auto_map[base]
    if base in ("北京", "天津", "上海", "重庆"):
        return base + "市"
    return base + "省"


def get_legal_basis_key(raw_province):
    m = re.match(r'(.+?)[（(](\d+)[）)]', raw_province)
    if m:
        base = m.group(1)
        cn_map = {"1": "一", "2": "二", "3": "三", "4": "四", "5": "五"}
        cn = cn_map.get(m.group(2), m.group(2))
        return base + "（" + cn + "）"
    return raw_province + "（一）"


# ====== 单省份处理 ======

def process_one(info, output_base):
    t0 = time.time()
    raw_prov = info.get("province", "")
    ps, pe = info["start"], info["end"]
    fid = raw_prov + "_P" + str(ps) + "-" + str(pe)
    prov_output = get_province_output_name(raw_prov)
    log.info("[%s] %s P%d-%d", fid, prov_output, ps, pe)

    basis_key = get_legal_basis_key(raw_prov)
    basis = PROVINCE_LEGAL_BASIS.get(basis_key, (prov_output + "量刑指导意见实施细则", "待识别"))
    legal_text = basis[0] + "（" + basis[1] + "）"

    ft = pdf_text(ps, pe)

    out_dir = os.path.join(output_base, prov_output)
    os.makedirs(out_dir, exist_ok=True)
    file_count = 0

    # 提取量刑情节
    s_items = extract_sentencing(ft)
    s_count = len(s_items)
    log.info("  量刑情节: %d 个", s_count)
    for item in s_items:
        fp = write_sentencing_file(out_dir, prov_output, item, legal_text)
        file_count += 1
        log.info("    + %s", os.path.basename(fp))

    # 提取罪名
    crimes = find_crimes(ft)
    c_count = len(crimes)
    log.info("  罪名: %d 个", c_count)
    for crime_entry in crimes:
        sub_items = split_crime_items(ft, crime_entry, crimes)
        crime_name = crime_entry["crime"]
        log.info("    %s: %d 个子条目", crime_name, len(sub_items))
        for sub in sub_items:
            fp = write_crime_file(out_dir, prov_output, crime_name, sub, legal_text)
            file_count += 1
            log.info("    + %s", os.path.basename(fp))

    dt = time.time() - t0
    log.info("  完成: %d 个文件, %ds", file_count, int(dt))
    return {"province": prov_output, "files": file_count, "sentencing": s_count, "crimes": c_count, "time": int(dt)}


# ====== 主流程 ======

def main(test_mode=False, test_provinces=None):
    with open(CATALOG, "r", encoding="utf-8") as f:
        cat = json.load(f)

    all_files = cat.get("全国性文件", []) + cat.get("省市级文件", [])
    log.info("总任务: %d 个文件", len(all_files))

    prog = load_progress()
    done_ids = set(prog.get("done", []))

    if test_mode and test_provinces:
        targets = []
        for info in all_files:
            prov = info.get("province", "")
            for tp in test_provinces:
                if tp in prov:
                    fid = prov + "_P" + str(info['start']) + "-" + str(info['end'])
                    if fid not in done_ids:
                        targets.append(info)
                    break
        remaining = targets
        output_base = TEST_DIR
        log.info("测试模式: %d 个文件 -> %s", len(remaining), output_base)
    else:
        remaining = [
            f for f in all_files
            if f"{f.get('province', '')}_P{f['start']}-{f['end']}" not in done_ids
        ]
        output_base = OUTPUT
        log.info("全量模式: 已完成 %d, 剩余 %d", len(done_ids), len(remaining))

    if not remaining:
        log.info("全部完成！")
        return

    for i, info in enumerate(remaining):
        raw = info.get("province", "")
        fid = raw + "_P" + str(info['start']) + "-" + str(info['end'])
        log.info("=" * 60)
        log.info("[%d/%d] %s", i + 1, len(remaining), fid)
        try:
            result = process_one(info, output_base)
            prog["done"].append(fid)
            prog["stats"][fid] = result
            save_progress(prog)
        except Exception as e:
            log.error("  失败: %s", e)
            import traceback
            traceback.print_exc()

    log.info("=" * 60)
    log.info("提取完成！统计:")
    for fid, st in prog["stats"].items():
        log.info("  %-20s | 文件=%3d 情节=%2d 罪名=%2d | %ds",
                 st['province'], st['files'], st['sentencing'], st['crimes'], st['time'])


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="量刑指导意见全量提取 v9.4")
    parser.add_argument("--test", action="store_true", help="测试模式")
    parser.add_argument("--provinces", nargs="+", default=None, help="测试省份列表")
    args = parser.parse_args()

    if args.test:
        if not args.provinces:
            args.provinces = ["辽宁", "黑龙江"]
        main(test_mode=True, test_provinces=args.provinces)
    else:
        main(test_mode=False)