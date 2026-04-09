# law-pdf-extract

通用 PDF 智能提取脚手架，用于法律文档的批量提取与结构化输出。

## 目录结构

```
law-pdf-extract/
├── SKILL.md                 # 本文件（技能说明）
├── .gitignore               # Git 忽略规则
├── references/
│   └── config-reference.md  # 配置文件完整字段说明
└── scripts/
    ├── run_task.py          # 主入口 - 任务加载、PDF提取、调用LLM
    ├── api_client.py        # API 客户端 - 限流、熔断、重试
    ├── task_runner.py       # 任务调度器 - 并发、断点续传、飞书通知
    ├── format_output.py     # 输出格式化
    ├── feishu_notifier.py   # 飞书群通知
    ├── feishu_listener.py   # 飞书 WebSocket 监听（触发式运行）
    └── .feishu_ws_listener.py # 飞书监听配置文件
```

## 适用场景

- 法律 PDF 批量提取（司法解释、证据规则、量刑指导意见等）
- 结构化数据提取（从非结构化 PDF 中提取条目/要点/表格）
- 任何需要"读 PDF → 调 LLM → 写文件"的批量任务

## 核心工作流

### 1. 创建任务

新建一个任务目录，标准结构：

```
<任务根目录>/
├── 规则/          # 提取规则文档（人工编写的提取要求）
├── 输入/          # 原始文件（PDF、JSON数据等）
├── 代码/          # 提取脚本（config.json + run.py）
├── 测试/          # 测试输出
└── 输出/          # 最终结果
```

### 2. 配置任务

编写 `config.json`（详见 `references/config-reference.md`）：

```json
{
  "task_name": "示例提取任务",
  "api": {
    "url": "https://api.example.com/v3/chat/completions",
    "key": "sk-xxx",
    "model": "model-name"
  },
  "input": {
    "pdf_path": "输入/源文件.pdf",
    "data_source": "toc",
    "data_path": "输入/目录.json",
    "page_ranges": {"第一章": [1, 50], "第二章": [51, 100]}
  },
  "prompt": {
    "system": "你是专业的法律文书分析助手。",
    "template": "从以下文本中提取...：\n{text}",
    "chunk_size": 20000
  },
  "output": {
    "dir": "输出",
    "filename_pattern": "{evidence}_{yaodian_name}"
  },
  "settings": {
    "max_workers": 5,
    "resume": true,
    "api_interval": 2
  },
  "notify": {
    "enabled": true,
    "chat_id": "oc_xxx",
    "app_id": "cli_xxx"
  }
}
```

### 3. 运行任务

```bash
# 直接运行
python 代码/run.py

# 预览任务列表（不实际调用API）
python 代码/run.py --dry-run

# 重试失败任务
python 代码/run.py --retry-failed
```

### 4. GitHub 版本管理

每次新增提取任务后，**必须**将任务代码文件提交到 GitHub 仓库。

#### 仓库结构

GitHub 仓库根目录按**任务根目录名称**命名子目录：

```
law-pdf-extract/                    # GitHub 仓库
├── SKILL.md
├── references/
├── scripts/                        # 脚手架通用脚本
└── <任务根目录名>/                 # 每个任务一个子目录
    ├── 代码/
    │   ├── config.json
    │   └── run.py
    ├── 规则/
    │   └── 提取规则.md
    └── README.md                   # 任务说明（可选）
```

> 只提交 `代码/` 和 `规则/` 目录，**不提交** `输入/`（源文件）、`输出/`（结果）和 `测试/`（中间产物）。

#### 自动提交方式

Agent 在完成代码文件编写后，应执行以下命令：

```bash
cd <仓库本地路径>
# 复制任务代码文件到仓库
xcopy /E /Y /I "<任务根目录>\代码" "<任务根目录名>\代码"
xcopy /E /Y /I "<任务根目录>\规则" "<任务根目录名>\规则"

# Git 提交与推送
git add <任务根目录名>/
git commit -m "feat: 新增 <task_name> 提取任务"
git push origin main
```

或使用 Python subprocess 执行：

```python
import subprocess, shutil, os

def git_commit_task(repo_dir, task_root_dir, task_name):
    """将任务的代码和规则文件提交到GitHub仓库"""
    task_folder_name = os.path.basename(task_root_dir.rstrip(os.sep))

    # 复制 代码/ 和 规则/ 到仓库
    for subdir in ["代码", "规则"]:
        src = os.path.join(task_root_dir, subdir)
        dst = os.path.join(repo_dir, task_folder_name, subdir)
        if os.path.exists(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)

    # Git 操作
    subprocess.run(["git", "add", task_folder_name], cwd=repo_dir, check=True)
    subprocess.run(
        ["git", "commit", "-m", f"feat: 新增 {task_name} 提取任务"],
        cwd=repo_dir, check=True
    )
    subprocess.run(["git", "push", "origin", "main"], cwd=repo_dir, check=True)
```

#### 提交规范

提交信息格式：`<type>: <description>`

| type | 说明 | 示例 |
|------|------|------|
| feat | 新增任务 | `feat: 新增 量刑指导意见提取 任务` |
| fix | 修复脚本 | `fix: 修复 extract.py 区域定位问题` |
| refactor | 重构代码 | `refactor: 重写量刑情节提取器v8.3` |
| docs | 文档更新 | `docs: 更新 config-reference.md` |
| chore | 杂务 | `chore: 更新 .gitignore` |

## 数据源类型

| data_source | 说明 | 适用场景 |
|-------------|------|---------|
| toc | 目录索引 | 提取法条对应的司法解释 |
| list | 自定义列表 | 自定义页码范围的任务 |
| table | 表格提取 | 提取 PDF 中的表格数据 |
| json | JSON 数据 | 从 JSON 文件读取任务列表 |

## 断点续传

脚本自动在输出目录生成 `_progress.json`，记录每个任务的完成状态。中断后重新运行会自动跳过已完成的任务。

## 飞书通知

支持通过飞书群推送任务进度。需要在配置中填写 `notify.chat_id` 和 `notify.app_id`，并通过环境变量 `FEISHU_APP_SECRET` 设置应用密钥。

## 依赖

- Python 3.8+
- pdfplumber（PDF 文本提取）
- aiohttp（API 异步调用）
- PyJWT（飞书通知鉴权）
- requests（飞书通知 HTTP 调用）
