#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
法律 PDF 提取 - 主入口
用法:
  python run_task.py config.json              # 执行任务
  python run_task.py config.json --dry-run    # 预览任务列表
  python run_task.py config.json --retry-failed  # 重试失败任务
"""

import os, sys, json, re, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api_client import APIConfig, APIClient, TaskTimeout, CircuitBreakerOpen
from task_runner import TaskRunner, TaskItem, RunConfig
from feishu_notifier import FeishuNotifier
import pdfplumber


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    required = ["api", "output"]
    for key in required:
        if key not in config:
            raise ValueError(f"配置缺少必填项: {key}")

    if not config["api"].get("url") or not config["api"].get("key"):
        raise ValueError("api.url 和 api.key 为必填项")

    return config


def smart_truncate(text: str, max_length: int) -> str:
    if len(text) <= max_length:
        return text
    truncate_points = ['\n\n', '\n', '。', '；', '！', '？', '.', '!', '?']
    best_point = max_length
    for pt in truncate_points:
        pos = text.rfind(pt, max_length - 500, max_length)
        if pos != -1:
            best_point = pos + len(pt)
            break
    return text[:best_point]

def extract_pdf_pages(pdf_path: str, start_page: int, end_page: int) -> str:
    texts = []
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        for pn in range(start_page, min(end_page + 1, total + 1)):
            text = pdf.pages[pn - 1].extract_text()
            if text:
                texts.append(text)
    return "\n".join(texts)

def extract_pdf_tables(pdf_path: str, start_page: int, end_page: int) -> str:
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        for pn in range(start_page, min(end_page + 1, total + 1)):
            page_tables = pdf.pages[pn - 1].extract_tables()
            for table in page_tables:
                if table:
                    markdown_table = []
                    for i, row in enumerate(table):
                        clean_row = [str(cell).replace("\\n", " ") if cell else "" for cell in row]
                        markdown_table.append("| " + " | ".join(clean_row) + " |")
                        if i == 0:
                            markdown_table.append("|" + "|".join(["---"] * len(clean_row)) + "|")
                    tables.append("\n".join(markdown_table))
    return "\n\n".join(tables)

def build_task_items(config: dict) -> list:
    inp = config.get("input", {})
    data_source = inp.get("data_source", "json")
    tasks = []

    if data_source == "toc":
        data_path = inp.get("data_path", "")
        with open(data_path, "r", encoding="utf-8") as f:
            toc = json.load(f)

        for idx, item in enumerate(toc):
            num = str(item[0])
            title = item[1].strip().lstrip("[").rstrip("]")
            sp = item[2]
            ep = toc[idx + 1][2] if idx + 1 < len(toc) else sp + 5
            next_num = str(toc[idx + 1][0]) if idx + 1 < len(toc) else None

            tasks.append(TaskItem(
                task_id=f"{num}_{title[:20]}",
                label=f"第{num}条 [{title[:25]}]",
                metadata={
                    "type": "interp",
                    "num": num,
                    "title": title,
                    "start_page": sp,
                    "end_page": ep,
                    "next_num": next_num,
                },
            ))

    elif data_source == "list":
        for item in inp.get("task_list", []):
            tasks.append(TaskItem(
                task_id=item["id"],
                label=item.get("label", item["id"]),
                metadata={"type": "custom", "pages": item.get("pages", []), **item},
            ))

    elif data_source == "table":
        for item in inp.get("task_list", []):
            tasks.append(TaskItem(
                task_id=item.get("id", str(len(tasks))),
                label=item.get("label", "表格内容"),
                metadata={"type": "table", "pages": item.get("pages", []), **item},
            ))

    else:
        data_path = inp.get("data_path", "")
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    tasks.append(TaskItem(
                        task_id=item, label=item,
                        metadata={"type": "json_list", "raw": item},
                    ))
                elif isinstance(item, dict):
                    task_id = (
                        item.get("task_id")
                        or item.get("id")
                        or item.get("label", "")
                    )
                    if not task_id:
                        point = item.get("point", "")
                        evidence = item.get("evidence", "")
                        task_id = f"{evidence}｜{point}" if evidence and point else str(item.get("num", idx))
                    label = item.get("label", "") or task_id
                    tasks.append(TaskItem(
                        task_id=task_id, label=label,
                        metadata={"type": "json_dict", **item},
                    ))

        elif isinstance(data, dict):
            for key in ["yaodians", "items", "tasks", "entries"]:
                if key in data:
                    for item in data[key]:
                        sub_items = item.get("sub_items", [])
                        evidence = item.get("evidence", "")
                        yaodian_name = item.get("yaodian_name", item.get("name", ""))

                        if sub_items:
                            for sub in sub_items:
                                sub_name = re.sub(
                                    r"^[（\uff08]([一二三四五六七八九十]+)[）\uff09]", "", sub
                                ).strip()
                                task_id = f"{evidence}|{yaodian_name}|{sub}"
                                tasks.append(TaskItem(
                                    task_id=task_id, label=f"{yaodian_name} -> {sub}",
                                    group=evidence,
                                    metadata={
                                        "type": "evidence_sub", "evidence": evidence,
                                        "yaodian_name": yaodian_name, "sub_item": sub,
                                        "sub_name": sub_name,
                                    },
                                ))
                        else:
                            task_id = f"{evidence}|{yaodian_name}"
                            tasks.append(TaskItem(
                                task_id=task_id, label=yaodian_name, group=evidence,
                                metadata={
                                    "type": "evidence", "evidence": evidence,
                                    "yaodian_name": yaodian_name,
                                },
                            ))
                    break

    return tasks


def create_processor(config: dict):
    inp = config.get("input", {})
    pdf_path = inp.get("pdf_path", "")
    page_ranges = inp.get("page_ranges", {})
    output_dir = config.get("output", {}).get("dir", "")
    prompt_cfg = config.get("prompt", {})
    system_prompt = prompt_cfg.get("system", "")
    template = prompt_cfg.get("template", "")
    chunk_size = prompt_cfg.get("chunk_size", 20000)

    os.makedirs(output_dir, exist_ok=True)
    output_config = config.get("output", {})
    chapter_cache = {}

    def processor(task: TaskItem, api: APIClient):
        meta = task.metadata
        task_type = meta.get("type", "")

        if task_type == "interp":
            return _process_interp(task, api, pdf_path, system_prompt, template,
                                   output_dir, chunk_size, output_config)

        elif task_type in ("evidence", "evidence_sub"):
            group = task.group or meta.get("evidence", "")
            if group not in chapter_cache and group in page_ranges:
                sp, ep = page_ranges[group]
                chapter_cache[group] = extract_pdf_pages(pdf_path, sp, ep)
                print(f"    加载 {group} 正文 P{sp}-P{ep} ({len(chapter_cache[group])}字)", flush=True)

            chapter_text = chapter_cache.get(group, "")
            if not chapter_text:
                raise RuntimeError(f"未找到分组 [{group}] 的页码范围")

            return _process_evidence(task, api, chapter_text, system_prompt,
                                     template, meta, output_config)

        elif task_type == "custom" or task_type == "table":
            pages = meta.get("pages", [])

            if task_type == "table":
                text = extract_pdf_tables(pdf_path, pages[0], pages[-1]) if pages else ""
            else:
                text = extract_pdf_pages(pdf_path, pages[0], pages[-1]) if pages else ""
                text = smart_truncate(text, chunk_size)

            prompt = template.replace("{text}", text).replace("{label}", task.label)

            if task_type == "table" and not system_prompt and not template:
                filepath = os.path.join(output_dir, f"{task.task_id}.md")
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(text)
                return {"content": text, "file": filepath}

            result = api.call(prompt, system=system_prompt)
            if not result.success:
                raise RuntimeError(result.error)
            filepath = os.path.join(output_dir, f"{task.task_id}.md")
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(result.content)
            return {"content": result.content, "file": filepath}

        elif task_type == "json_dict":
            evidence = meta.get("evidence", "")
            if evidence and evidence in page_ranges:
                group = evidence
                if group not in chapter_cache:
                    sp, ep = page_ranges[group]
                    chapter_cache[group] = extract_pdf_pages(pdf_path, sp, ep)
                    print(f"    加载 {group} 正文 P{sp}-P{ep} ({len(chapter_cache[group])}字)", flush=True)
                chapter_text = chapter_cache.get(group, "")
                if not chapter_text:
                    raise RuntimeError(f"未找到分组 [{group}] 的页码范围")
                result = _process_evidence(task, api, chapter_text, system_prompt,
                                           template, meta, output_config)
            else:
                chapter_text = meta.get("text", meta.get("raw", meta.get("content", "")))
                if chapter_text:
                    result = _process_evidence(task, api, chapter_text, system_prompt,
                                               template, meta, output_config)
                else:
                    prompt = template
                    for key, val in meta.items():
                        if isinstance(val, str):
                            prompt = prompt.replace(f"{{{key}}}", val)
                    result = api.call(prompt, system=system_prompt)
                    if not result.success:
                        raise RuntimeError(result.error)
                    filepath = os.path.join(output_dir, f"{task.task_id}.md")
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(result.content)
                    result = {"content": result.content, "file": filepath, "chars": len(result.content)}
            return result

        elif task_type == "json_list":
            prompt = template.replace("{text}", meta["raw"]).replace("{label}", task.label)
            result = api.call(prompt, system=system_prompt)
            if not result.success:
                raise RuntimeError(result.error)
            filepath = os.path.join(output_dir, f"{task.task_id}.md")
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(result.content)
            return {"content": result.content, "file": filepath}

        else:
            prompt = template.replace("{label}", task.label)
            result = api.call(prompt, system=system_prompt)
            if not result.success:
                raise RuntimeError(result.error)
            filepath = os.path.join(output_dir, f"{task.task_id}.md")
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(result.content)
            return {"content": result.content, "file": filepath}

    return processor


PROJECT_SUBDIRS = ["规则", "输入", "代码", "测试", "输出"]


def init_project(project_dir: str, project_name: str = "") -> dict:
    subdirs = {}
    for name in PROJECT_SUBDIRS:
        p = os.path.join(project_dir, name)
        os.makedirs(p, exist_ok=True)
        subdirs[name] = p
    return subdirs


def place_input_files(project_dir: dict, files: list, target_subdir: str = "输入") -> list:
    import shutil
    dest_dir = project_dir.get(target_subdir, project_dir.get("输入", ""))
    if not dest_dir:
        return []
    placed = []
    for src in files:
        if isinstance(src, str) and os.path.exists(src):
            filename = os.path.basename(src)
            dest = os.path.join(dest_dir, filename)
            if os.path.exists(dest) and os.path.abspath(src) != os.path.abspath(dest):
                base, ext = os.path.splitext(filename)
                counter = 1
                while os.path.exists(dest):
                    dest = os.path.join(dest_dir, f"{base}_{counter}{ext}")
                    counter += 1
            shutil.copy2(src, dest)
            placed.append(dest)
    return placed


def save_rule_doc(project_dir: dict, content: str, filename: str = "提取规则.md") -> str:
    rule_dir = project_dir.get("规则", "")
    if not rule_dir:
        return ""
    filepath = os.path.join(rule_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    return filepath


def save_config(project_dir: dict, config: dict, filename: str = "config.json") -> str:
    code_dir = project_dir.get("代码", "")
    if not code_dir:
        return ""
    filepath = os.path.join(code_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    return filepath


def save_run_script(project_dir: dict, config: dict, filename: str = "run.py") -> str:
    code_dir = project_dir.get("代码", "")
    if not code_dir:
        return ""
    import base64
    config_bytes = json.dumps(config, ensure_ascii=False).encode("utf-8")
    config_b64 = base64.b64encode(config_bytes).decode("ascii")
    lines = [
        "#!/usr/bin/env python3",
        "# -*- coding: utf-8 -*-",
        '"""自动生成的提取脚本 - 运行方式: python run.py"""',
        "import sys, json, base64",
        'sys.path.insert(0, r"C:\\Users\\黑面书生\\AppData\\Roaming\\WPS 灵犀\\serverdir\\skills\\law-pdf-extract\\scripts")',
        "from run_task import run_from_dict",
        "",
        '_CONFIG_B64 = "' + config_b64 + '"',
        'CONFIG = json.loads(base64.b64decode(_CONFIG_B64).decode("utf-8"))',
        "",
        'if __name__ == "__main__":',
        "    import argparse",
        "    parser = argparse.ArgumentParser()",
        '    parser.add_argument("--dry-run", action="store_true")',
        '    parser.add_argument("--retry-failed", action="store_true")',
        "    args = parser.parse_args()",
        "    result = run_from_dict(CONFIG, retry_failed=args.retry_failed, dry_run=args.dry_run)",
        '    print(json.dumps(result, ensure_ascii=False, indent=2))',
        "",
    ]
    filepath = os.path.join(code_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return filepath

def _build_filepath(output_config: dict, meta: dict, default_suffix: str = "") -> tuple:
    output_dir = output_config.get("dir", "")
    pattern = output_config.get("filename_pattern", "")
    title_prefix = output_config.get("title_prefix", "")

    if not pattern:
        if meta.get("type") == "interp":
            num = meta.get("num", "")
            title = re.sub(r'[\\/:*?"<>|]', '', meta.get("title", ""))[:50]
            name = f"第{num}条【{title}】{default_suffix}" if default_suffix else f"第{num}条【{title}】"
            title_line = f"{name}："
        elif meta.get("type") in ("evidence", "evidence_sub"):
            evidence = meta.get("evidence", "")
            yaodian = meta.get("yaodian_name", "")
            sub = meta.get("sub_name", "")
            if sub:
                name = f"证据分析【{evidence}】{yaodian}（{sub}）"
            else:
                name = f"证据分析【{evidence}】{yaodian}"
            title_line = f"{name}："
        elif meta.get("type") == "json_dict":
            evidence = meta.get("evidence", "")
            point = meta.get("point", "")
            name = f"证据分析【{evidence}】非法证据排除（{point}）"
            title_line = f"{name}："
        else:
            task_id = meta.get("task_id", meta.get("id", "output"))
            name = re.sub(r'[\\/:*?"<>|]', '', task_id)[:200]
            title_line = f"{name}："
    else:
        name = pattern
        for key, val in meta.items():
            if isinstance(val, str):
                name = name.replace(f"{{{key}}}", val)
            elif isinstance(val, (int, float)):
                name = name.replace(f"{{{key}}}", str(val))
        name = re.sub(r'[\\/:*?"<>|]', '', name)[:200]
        title_line = f"{title_prefix}：" if title_prefix else name

    filename = name + ".md"
    filepath = os.path.join(output_dir, filename)
    return filepath, title_line


def _process_interp(task, api, pdf_path, system_prompt, template, output_dir, chunk_size, output_config=None):
    meta = task.metadata
    num = meta["num"]
    title = meta["title"]
    sp = meta["start_page"]
    ep = meta["end_page"]
    next_num = meta.get("next_num")

    raw_text = extract_pdf_pages(pdf_path, sp, ep)
    if len(raw_text) < 20:
        safe_title = re.sub(r'[\\/:*?"<>|]', '', title)[:50]
        filepath = os.path.join(output_dir, f"第{num}条【{safe_title}】司法解释.md")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("司法解释目录：\n该条文暂无相关的司法解释。\n")
        return {"status": "no_interpretation", "count": 0}

    if template:
        prompt = template.replace("{text}", smart_truncate(raw_text, chunk_size)).replace("{num}", num).replace("{title}", title)
    else:
        nd = next_num if next_num else "?"
        prompt = (
            f"你是专业的法律文书分析助手。下面是第{num}条（{title}）到"
            f"第{nd}条之间的PDF原始文本。\n\n"
            f"【任务】提取所有司法文件/司法解释的标题。每个文件通常包含文件标题和发文字号。\n\n"
            f"【输出规则】\n"
            f"1. 将发文字号和日期合并到标题后面，用括号包裹\n"
            f"2. 只输出标题+文号，不要输出正文内容\n"
            f"3. 如果无相关文件，输出：该条文暂无相关的司法解释。\n\n"
            f"【输出格式】（每行以数字序号开头）\n1. 标题1（文号）\n\n"
            f"【待处理文本】\n{smart_truncate(raw_text, chunk_size)}"
        )

    result = api.call(prompt, system=system_prompt)
    if not result.success:
        raise RuntimeError(result.error)

    titles = _parse_titles(result.content)

    output_cfg = output_config or {"dir": output_dir}
    filepath, title_line = _build_filepath(
        output_cfg if output_cfg else {"dir": output_dir},
        meta, default_suffix="司法解释"
    )
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        if titles:
            f.write("司法解释目录：\n")
            for i, t in enumerate(titles, 1):
                f.write(f"{i}. {t}\n")
        else:
            f.write("司法解释目录：\n该条文暂无相关的司法解释。\n")

    return {"status": "done" if titles else "no_interpretation", "count": len(titles), "titles": titles}


def _process_evidence(task, api, chapter_text, system_prompt, template, meta, output_config=None):
    evidence = meta.get("evidence", "")
    yaodian_name = meta.get("yaodian_name", "")
    sub_item = meta.get("sub_item")
    sub_name = meta.get("sub_name", "")
    point = meta.get("point", yaodian_name)
    nature = meta.get("nature", "")
    treatment = meta.get("treatment", "")

    if template:
        label = f"({sub_name})" if sub_item else yaodian_name
        prompt = (template
            .replace("{text}", chapter_text)
            .replace("{label}", label)
            .replace("{evidence}", evidence)
            .replace("{point}", point)
            .replace("{nature}", nature)
            .replace("{treatment}", treatment))
    else:
        target = f"({sub_name})" if sub_item else f"({yaodian_name})"
        prompt = (
            f"从以下《刑事证据审查手册》正文中，找到与{target}对应的段落，"
            f"提取审查规则内容并进行提炼总结。\n\n"
            f"【输出格式】连续段落，不要序号列表。\n"
            f"【限制】只提取审查规则，不要法规索引。\n"
            f"【要求】保留核心要点，去除案例案情，纠正OCR错误。\n\n"
            f"【正文】\n{chapter_text}"
        )

    result = api.call(prompt, system=system_prompt)
    if not result.success:
        raise RuntimeError(result.error)

    try:
        from format_output import format_output
        formatted = format_output(result.content)
    except Exception:
        formatted = result.content

    output_cfg = output_config or {"dir": output_dir}
    filepath, title_line = _build_filepath(
        output_cfg if output_cfg else {"dir": output_dir},
        meta
    )
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"{title_line}\n{formatted}")

    return {"content": formatted, "file": filepath, "chars": len(formatted)}

def _parse_titles(api_output: str) -> list:
    NOISE = ["该条文暂无相关的司法解释", "待处理文本", "【输出格式", "【任务"]
    titles = []
    for line in api_output.split("\n"):
        line = line.strip()
        if not line:
            continue
        m = re.match(r"\s*\d+[\.\u3000\u3001、]\s*(.+)", line)
        t = m.group(1).strip() if m else line
        if not t or len(t) < 6:
            continue
        if any(nk in t for nk in NOISE):
            continue
        if t not in titles:
            titles.append(t)
    return titles


def run_from_config(config_path: str, retry_failed: bool = False, dry_run: bool = False):
    config = load_config(config_path)

    api_cfg_data = config["api"]
    api_config = APIConfig(
        url=api_cfg_data["url"],
        key=api_cfg_data["key"],
        model=api_cfg_data.get("model", ""),
        max_tokens=api_cfg_data.get("max_tokens", 4096),
        temperature=api_cfg_data.get("temperature", 0.01),
        max_retries=config.get("settings", {}).get("max_retries", 3),
        retry_base_delay=config.get("settings", {}).get("retry_base_delay", 5.0),
        single_timeout=config.get("settings", {}).get("single_timeout", 60),
        task_timeout=config.get("settings", {}).get("task_timeout", 300),
        interval=config.get("settings", {}).get("api_interval", 2),
        max_consecutive_failures=config.get("settings", {}).get("max_consecutive_failures", 5),
    )

    output_dir = config.get("output", {}).get("dir", "")
    task_name = config.get("task_name", "未命名任务")
    notify_cfg = config.get("notify", {})
    settings = config.get("settings", {})

    run_config = RunConfig(
        resume=settings.get("resume", True),
        retry_failed=retry_failed,
        notify_enabled=notify_cfg.get("enabled", True),
        notify_interval=notify_cfg.get("interval_tasks", 10),
        notify_interval_sec=notify_cfg.get("interval_seconds", 300),
        output_dir=output_dir,
        task_name=task_name,
        max_workers=settings.get("max_workers", 5),
    )

    tasks = build_task_items(config)

    if dry_run:
        print(f"任务: {task_name}", flush=True)
        print(f"总数: {len(tasks)}", flush=True)
        current_group = ""
        for t in tasks:
            if t.group and t.group != current_group:
                current_group = t.group
                print(f"\n--- {current_group} ---", flush=True)
            print(f"  {t.label}", flush=True)
        return {"total": len(tasks), "dry_run": True}

    processor = create_processor(config)
    runner = TaskRunner(api_config, run_config, processor)
    return runner.run(tasks)


def run_from_dict(config: dict, retry_failed: bool = False, dry_run: bool = False):
    required = ["api", "output"]
    for key in required:
        if key not in config:
            raise ValueError(f"配置缺少必填项: {key}")
    if not config["api"].get("url") or not config["api"].get("key"):
        raise ValueError("api.url 和 api.key 为必填项")
    if not config.get("input", {}).get("pdf_path"):
        raise ValueError("input.pdf_path 为必填项")

    api_cfg_data = config["api"]
    api_config = APIConfig(
        url=api_cfg_data["url"],
        key=api_cfg_data["key"],
        model=api_cfg_data.get("model", ""),
        max_tokens=api_cfg_data.get("max_tokens", 4096),
        temperature=api_cfg_data.get("temperature", 0.01),
        max_retries=config.get("settings", {}).get("max_retries", 3),
        retry_base_delay=config.get("settings", {}).get("retry_base_delay", 5.0),
        single_timeout=config.get("settings", {}).get("single_timeout", 60),
        task_timeout=config.get("settings", {}).get("task_timeout", 300),
        interval=config.get("settings", {}).get("api_interval", 2),
        max_consecutive_failures=config.get("settings", {}).get("max_consecutive_failures", 5),
    )

    output_dir = config.get("output", {}).get("dir", "")
    task_name = config.get("task_name", "未命名任务")
    notify_cfg = config.get("notify", {})
    settings = config.get("settings", {})

    run_config = RunConfig(
        resume=settings.get("resume", True),
        retry_failed=retry_failed,
        notify_enabled=notify_cfg.get("enabled", True),
        notify_interval=notify_cfg.get("interval_tasks", 10),
        notify_interval_sec=notify_cfg.get("interval_seconds", 300),
        output_dir=output_dir,
        task_name=task_name,
        max_workers=settings.get("max_workers", 5),
    )

    tasks = build_task_items(config)

    if dry_run:
        print(f"任务: {task_name}", flush=True)
        print(f"总数: {len(tasks)}", flush=True)
        current_group = ""
        for t in tasks:
            if t.group and t.group != current_group:
                current_group = t.group
                print(f"\n--- {current_group} ---", flush=True)
            print(f"  {t.label}", flush=True)
        return {"total": len(tasks), "dry_run": True}

    processor = create_processor(config)
    runner = TaskRunner(api_config, run_config, processor)
    return runner.run(tasks)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python run_task.py <config.json> [--dry-run] [--retry-failed]")
        sys.exit(1)

    config_file = sys.argv[1]
    retry = "--retry-failed" in sys.argv
    dry = "--dry-run" in sys.argv

    run_from_config(config_file, retry_failed=retry, dry_run=dry)
