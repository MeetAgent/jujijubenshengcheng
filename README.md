# 剧集剧本生成流水线（0.1 → 0.8）

本项目实现了从多集视频与台词数据，自动完成：ASR → 线索提取 → 全局对齐 → 回填 → 对话轮次重构 → 情节提取 → 剧本撰写 → 合并导出 全流程。

## 快速开始

### 环境准备
```bash
# 激活虚拟环境
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 配置设置
1. 修改 `pipeline.yaml` 中的 GCP 配置：
   - `project_id`: 你的 Google Cloud 项目 ID
   - `credentials_path`: 服务账号密钥文件路径
   - `bucket_name`: GCS 存储桶名称

### 运行方式

#### 1. 交互式运行（推荐）
```bash
python run_0_1_to_0_8.py --interactive
```
- 选择集合目录（`new_pipeline/output/` 下的集合）
- 选择执行模式：
  - 从头开始
  - 从指定步骤开始
  - 强制重跑指定步骤
  - 修复无效文件

#### 2. 非交互式运行
```bash
# 从 Step0.1 开始
python run_0_1_to_0_8.py --collection "集合名" --start-step 0.1

# 从 Step0.3 开始
python run_0_1_to_0_8.py --collection "集合名" --start-step 0.3
```

#### 3. 完整流程（包含上传）
```bash
# 设置环境变量
export STEP0_INPUT_DIR="/path/to/mp4/files"
export STEP0_COLLECTION="集合名"
export STEP0_BUCKET_NAME="your-bucket"

# 运行完整流程（Step0 到 Step0.8）
python run_full_0_to_0_8.py
```

### 环境变量
- `FORCE_OVERWRITE=1` 强制覆盖已存在的步骤产物
- `SKIP_LLM=1` 让 0.4 仅走"快速回填"，不调用模型
- `DEBUG_DRAFT=1` 让 0.4 额外输出草稿 `0_4_dialogue_draft.txt`

## 流水线步骤与产物

### 核心步骤

| 步骤 | 功能 | 输入 | 输出 | 说明 |
|------|------|------|------|------|
| **Step0** | 视频上传 | 本地 MP4 文件 | GCS 路径 | 仅 `run_full_0_to_0_8.py` 包含 |
| **Step0.1** | ASR 语音识别 | GCS 视频 | `0_1_timed_dialogue.srt` | 语音转文字，带时间戳 |
| **Step0.2** | 线索提取 | SRT 字幕 | `0_2_clues.json` | 提取角色、关系、首次出场 |
| **Step0.3** | 全局对齐 | 所有线索 | `global_character_graph_llm.json` | LLM 全局角色对齐 |
| **Step0.4** | 说话人回填 | 对齐结果 | `0_4_calibrated_dialogue.txt` | 快速回填，无 LLM |
| **Step0.5** | 轮次重构 | 校准对话 | `0_5_dialogue_turns.json` | 法证纠错+语义分析 |
| **Step0.6** | 情节提取 | 对话轮次 | `0_6_script_flow.json` | 场景+情节+角色 Bio |
| **Step0.7** | 剧本撰写 | 剧本流 | `0_7_script.stmf` | STMF 格式剧本 |
| **Step0.8** | 合并导出 | 所有 STMF | Fountain/FDX | 最终剧本格式 |

### 详细产物说明

#### Step0.1 ASR（`step0_1_asr.py`）
- **产物**：`episode_xxx/0_1_timed_dialogue.srt`
- **功能**：语音识别转文字，带精确时间戳

#### Step0.2 线索提取（`step0_2_clue_extraction.py`）
- **产物**：`episode_xxx/0_2_clues.json`
- **功能**：提取角色、关系、首次出场信息
- **特点**：串行处理，先验信息累积

#### Step0.3 全局对齐（`step0_3_global_alignment_llm.py`）
- **产物**：
  - `global/global_character_graph_llm.json`（全局角色图）
  - `global/global_characters.csv`（角色汇总表）
  - `episode_xxx/0_3_speaker_hints.csv`（说话人提示）
- **功能**：LLM 全局角色对齐，建立关系网络

#### Step0.4 说话人回填（`step0_4_speaker_calibration.py`）
- **产物**：`episode_xxx/0_4_calibrated_dialogue.txt`
- **功能**：基于提示快速回填说话人身份
- **特点**：无 LLM 调用，纯规则匹配

#### Step0.5 对话轮次重构（`step0_5_integrated_analysis.py`）
- **产物**：
  - `episode_xxx/0_5_dialogue_turns.json`（结构化对话）
  - `episode_xxx/0_5_analysis_summary.md`（分析摘要）
- **功能**：法证纠错+轮次重构+语义分析
- **特点**：需要视频 URI，进行深度纠错

#### Step0.6 情节提取（`step0_6_plot_extraction.py`）
- **产物**：
  - `episode_xxx/0_6_script_flow.json`（剧本流）
  - `episode_xxx/0_6_script_draft.md`（剧本草稿）
- **功能**：提取场景、情节、角色 Bio
- **特点**：集成首次出场信息，精确 Casting Bio

#### Step0.7 剧本撰写（`step0_7_script_writing.py`）
- **产物**：
  - `episode_xxx/0_7_script.stmf`（STMF 剧本）
  - `episode_xxx/0_7_script_analysis.json`（分析结果）
- **功能**：生成标准 STMF 格式剧本
- **特点**：分场景处理，避免截断

#### Step0.8 合并导出（`step0_8_final_script.py`）
- **产物**（集合根目录）：
  - `0_8_merged_script.stmf`（合并 STMF）
  - `0_8_complete_screenplay.fountain`（Fountain 格式）
  - `0_8_complete_screenplay.fdx`（FDX 格式）
- **功能**：合并所有剧集，导出最终剧本
- **特点**：Title 使用集合名

## 运行脚本与检查逻辑

### 主要脚本

#### `run_0_1_to_0_8.py`（推荐）
- **功能**：从 Step0.1 开始的主运行脚本
- **特点**：
  - 交互式集合选择
  - 支持从任意步骤开始
  - 强制重跑模式
  - 文件完整性检查
  - **自动生成运行报告**

#### `run_full_0_to_0_8.py`
- **功能**：包含 Step0 上传的完整流程
- **特点**：
  - 非交互式，通过环境变量配置
  - 适合批处理和自动化

### 文件完整性检查

| 步骤 | 检查文件 | 位置 |
|------|----------|------|
| 0.1 | `0_1_timed_dialogue.srt` | `episode_xxx/` |
| 0.2 | `0_2_clues.json` | `episode_xxx/` |
| 0.3 | `global_character_graph_llm.json` | `global/` 或根目录 |
| 0.4 | `0_4_calibrated_dialogue.txt` | `episode_xxx/` |
| 0.5 | `0_5_dialogue_turns.json` | `episode_xxx/` |
| 0.6 | `0_6_script_flow.json` + `0_6_script_draft.md` | `episode_xxx/` |
| 0.7 | `0_7_script.stmf` + `0_7_script_analysis.json` | `episode_xxx/` |
| 0.8 | `0_8_complete_screenplay.fountain` + `.fdx` | 集合根目录 |

## 目录结构

```
项目根目录/
├── run_0_1_to_0_8.py          # 主运行脚本（推荐）
├── run_full_0_to_0_8.py       # 完整流程脚本
├── pipeline.yaml              # 配置文件
├── requirements.txt           # 依赖包
├── README.md                  # 本文档
├── gcp-genai-for-game-*.json  # GCP 服务账号密钥
├── venv/                      # 虚拟环境
└── new_pipeline/
    ├── core/                  # 核心模块
    │   ├── config.py          # 配置管理
    │   ├── client.py          # GenAI 客户端（含 Token 统计）
    │   ├── utils.py           # 工具函数
    │   ├── exceptions.py      # 异常定义
    │   └── report_generator.py # 运行报告生成器
    ├── steps/                 # 步骤实现
    │   ├── step0_upload.py    # Step0: 视频上传
    │   ├── step0_1_asr.py     # Step0.1: ASR
    │   ├── step0_2_clue_extraction.py
    │   ├── step0_3_global_alignment_llm.py
    │   ├── step0_4_speaker_calibration.py
    │   ├── step0_5_integrated_analysis.py
    │   ├── step0_6_plot_extraction.py
    │   ├── step0_7_script_writing.py
    │   └── step0_8_final_script.py
    ├── input/                 # 输入目录（MP4 文件）
    │   └── <collection>/
    │       ├── 1.mp4
    │       ├── 2.mp4
    │       └── ...
    └── output/                # 输出目录
        └── <collection>_output/
            ├── global/                    # 全局文件
            │   ├── global_character_graph_llm.json
            │   ├── global_characters.csv
            │   └── 0_3_preprocessed_data.json
            ├── episode_001/               # 分集文件
            │   ├── 0_1_timed_dialogue.srt
            │   ├── 0_2_clues.json
            │   ├── 0_3_speaker_hints.csv
            │   ├── 0_4_calibrated_dialogue.txt
            │   ├── 0_5_dialogue_turns.json
            │   ├── 0_5_analysis_summary.md
            │   ├── 0_6_script_flow.json
            │   ├── 0_6_script_draft.md
            │   ├── 0_7_script.stmf
            │   └── 0_7_script_analysis.json
            ├── episode_002/
            ├── ...
            ├── 0_8_merged_script.stmf     # 最终产物
            ├── 0_8_complete_screenplay.fountain
            ├── 0_8_complete_screenplay.fdx
            └── pipeline_execution_report.md  # 运行报告
```

## 运行报告功能

### 自动报告生成
运行完成后，系统会自动生成详细的执行报告 `pipeline_execution_report.md`，包含：

- **执行概览**：各步骤状态、执行时间
- **Token 使用统计**：总调用次数、输入/输出/总 token 数
- **各步骤详细统计**：处理剧集数、成功率、特定指标
- **性能分析**：最慢/最快步骤、平均时间
- **成本估算**：基于 token 使用量的成本预估

### 报告示例
```markdown
# 流水线执行报告

**生成时间**: 2024-01-15 14:30:25  
**总执行时间**: 1250.5 秒 (20.8 分钟)

## 执行概览
| 步骤 | 状态 | 执行时间(秒) | 说明 |
|------|------|-------------|------|
| Step0.1 ASR | completed | 180.2 | 语音识别转文字 (处理74集) |
| Step0.2 线索提取 | completed | 95.8 | 提取角色线索和关系 (处理74集) |
| ...

## Token 使用统计
**总计**:
- 总调用次数: 156
- 输入 Token: 2,450,000
- 输出 Token: 890,000
- 总 Token: 3,340,000
```


## 常见问题（FAQ）

### 运行问题
- **交互模式不显示集合目录？**
  - 已兼容：双层同名与单层（存在 `episode_*`/`global/` 即识别为集合）

- **步骤已跑成功但被判未完成？**
  - 检查路径已对齐当前产物命名与层级；若仍异常，请检查文件完整性

- **0.4 只做回填，不调用 LLM？**
  - 是。0.4 已改为纯回填；之后 0.5 做正式校准/重构

- **Fountain 标题如何设置？**
  - 0.8 使用集合输出目录名作为 `Title:`（例如：`33065-晚来不识卿…_output`）

### 配置问题
- **GCP 配置错误？**
  - 检查 `pipeline.yaml` 中的 `project_id`、`credentials_path`、`bucket_name`
  - 确保服务账号有必要的权限

- **Token 消耗过高？**
  - 查看运行报告中的 Token 统计
  - 考虑调整 `max_tokens` 或使用更便宜的模型

## 技术架构

### 核心组件
- **PipelineConfig**：统一配置管理
- **GenAIClient**：Google GenAI 客户端（含 Token 统计）
- **PipelineUtils**：通用工具函数
- **PipelineReportGenerator**：运行报告生成器

### 设计原则
- **最小变更**：避免大范围重构
- **统一配置**：通过 `PipelineConfig` 注入路径
- **完整统计**：自动记录时间、Token、成功率
- **模块化**：各步骤独立，易于维护

## 版本与依赖

- **Python**: 3.8+
- **主要依赖**：见 `requirements.txt`
- **Google GenAI**: Vertex AI 接入
- **配置要求**：`pipeline.yaml` 中的 GCP 配置

---

## 更新日志

### v2.0 (当前版本)
- ✅ 添加完整的运行报告生成功能
- ✅ 集成 Token 使用统计
- ✅ 优化文件完整性检查
- ✅ 统一各步骤返回状态
- ✅ 移除内容截断限制
- ✅ 集成首次出场信息到 Casting Bio
- ✅ 动态 Fountain 标题设置

### v1.0
- 基础流水线实现
- 8 个核心步骤
- 交互式运行脚本
