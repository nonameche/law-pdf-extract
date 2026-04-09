# 配置参考

## 任务配置结构（JSON）

每个提取任务通过一个 JSON 配置文件定义，无需编写 Python 代码。

```json
{
  "task_name": "司法解释标题提取",
  "api": {
    "url": "https://ark.cn-beijing.volces.com/api/v3/chat/completions",
    "key": "your-api-key",
    "model": "doubao-seed-2-0-lite-260215",
    "max_tokens": 4096,
    "temperature": 0.01
  },
  "input": {
    "pdf_path": "path/to/input.pdf",
    "data_source": "json",
    "data_path": "path/to/tasks.json",
    "page_ranges": {
      "分组名1": [1, 50],
      "分组名2": [51, 100]
    }
  },
  "output": {
    "dir": "path/to/output",
    "format": "markdown",
    "filename_pattern": "{num}_{title}"
  },
  "settings": {
    "resume": true,
    "retry_failed": false,
    "api_interval": 2,
    "max_retries": 3,
    "single_timeout": 60,
    "task_timeout": 300,
    "max_consecutive_failures": 5
  },
  "notify": {
    "enabled": true,
    "interval_tasks": 10,
    "interval_seconds": 300
  },
  "prompt": {
    "system": "你是专业的法律文书分析助手。",
    "template": "处理文本: {text}\n提取与 {label} 相关的内容。",
    "chunk_size": 20000
  }
}
```

## 配置项说明

### api

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| url | string | 必填 | API 端点 |
| key | string | 必填 | API 密钥 |
| model | string | 必填 | 模型名称 |
| max_tokens | int | 4096 | 最大输出 token |
| temperature | float | 0.01 | 温度 |

### input

| 字段 | 类型 | 说明 |
|------|------|------|
| pdf_path | string | PDF 文件路径 |
| data_source | string | 数据来源: json / toc / list |
| data_path | string | 任务数据文件路径 |
| page_ranges | dict | 按分组的页码范围映射 |

### output

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| dir | string | 必填 | 输出目录路径 |
| filename_pattern | string | 自动推断 | 文件名模板，如 `第{num}条【{title}】司法解释` |
| title_prefix | string | 同文件名 | .md 文件第一行标题前缀 |

**filename_pattern 可用变量（取决于数据来源）：**

- toc 模式: `{num}` `{title}` `{start_page}` `{end_page}`
- json 模式: `{evidence}` `{yaodian_name}` `{sub_name}` `{sub_item}`
- list 模式: task_list 中的所有自定义字段 + `{id}` `{label}`

**title_prefix 示例：**
```json
"output": {
    "dir": "E:\输出",
    "title_prefix": "证据审查规则",
    "filename_pattern": "【{evidence}】{yaodian_name}（{sub_name}）"
}
```
输出文件内容:
```
证据审查规则：
正文内容...
```

### settings

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| resume | bool | true | 断点续跑 |
| retry_failed | bool | false | 重试已失败任务 |
| api_interval | float | 2.0 | API 调用间隔（秒） |
| max_retries | int | 3 | 单任务最大重试次数 |
| single_timeout | float | 60 | 单次请求超时（秒） |
| task_timeout | float | 300 | 单任务总超时（秒） |
| max_consecutive_failures | int | 5 | 连续失败熔断阈值 |

### notify

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| enabled | bool | true | 启用飞书通知 |
| interval_tasks | int | 10 | 每处理 N 个任务推送进度 |
| interval_seconds | float | 300 | 进度推送最小时间间隔（秒） |

### prompt

| 字段 | 类型 | 说明 |
|------|------|------|
| system | string | 系统提示词 |
| template | string | 用户提示词模板，支持变量替换 |
| chunk_size | int | 文本分块大小（字符数），超过则分批调用 |
