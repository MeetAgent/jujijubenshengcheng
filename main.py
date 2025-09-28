# run_pipeline_api.py
import os
import sys
import time
import json
import asyncio
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel
from contextlib import asynccontextmanager

# 添加路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from new_pipeline.core import PipelineConfig, PipelineUtils
from new_pipeline.core.report_generator import PipelineReportGenerator
from new_pipeline.steps.step0_1_asr import Step0_1ASR
from new_pipeline.steps.step0_2_clue_extraction import Step0_2ClueExtraction
from new_pipeline.steps.step0_3_global_alignment_llm import Step0_3GlobalAlignmentLLM
from new_pipeline.steps.step0_4_speaker_calibration import Step0_4SpeakerCalibration
from new_pipeline.steps.step0_5_integrated_analysis import Step0_5IntegratedAnalysis
from new_pipeline.steps.step0_6_plot_extraction import Step0_6PlotExtraction
from new_pipeline.steps.step0_7_script_writing import Step0_7ScriptWriting
from new_pipeline.steps.step0_8_final_script import Step0_8FinalScript

from new_pipeline.steps.commont_log import log
from dotenv import load_dotenv

load_dotenv()

# 全局状态存储（生产环境应使用 Redis/DB）
PIPELINE_STATES: Dict[str, Dict] = {}  # {collection: {status, logs, stats, ...}}
LOG_BUFFERS: Dict[str, List[str]] = {}

# 步骤映射
STEPS = [
    ("0.1", Step0_1ASR, "ASR"),
    ("0.2", Step0_2ClueExtraction, "线索提取"),
    ("0.3", Step0_3GlobalAlignmentLLM, "全局角色对齐"),
    ("0.4", Step0_4SpeakerCalibration, "说话人校准"),
    ("0.5", Step0_5IntegratedAnalysis, "对话轮次重构"),
    ("0.6", Step0_6PlotExtraction, "情节抽取"),
    ("0.7", Step0_7ScriptWriting, "剧本撰写（STMF）"),
    ("0.8", Step0_8FinalScript, "合并与导出")
]

EXPECTED_FILES = {
    "0.1": ["0_1_timed_dialogue.srt"],
    "0.2": ["0_2_clues.json"],
    "0.3": ["global_character_graph_llm.json"],
    "0.4": ["0_4_calibrated_dialogue.txt"],
    "0.5": ["0_5_dialogue_turns.json"],
    "0.6": ["0_6_script_flow.json", "0_6_script_draft.md"],
    "0.7": ["0_7_script.stmf", "0_7_script_analysis.json"],
    "0.8": ["0_8_complete_screenplay.fountain", "0_8_complete_screenplay.fdx"]
}

# 日志捕获（简化）
class LogCapture:
    def __init__(self, collection: str):
        self.collection = collection
        if collection not in LOG_BUFFERS:
            LOG_BUFFERS[collection] = []
    
    def info(self, msg: str):
        LOG_BUFFERS[self.collection].append(f"[{time.strftime('%H:%M:%S')}] {msg}")
        print(msg)  # 保留控制台输出

# 替换全局 log
original_log = log
log = None  # 将在 run_pipeline 中动态替换

# --- 工具函数（复用原逻辑，略作调整）---

def check_invalid_files(config: PipelineConfig, step_name: str) -> List[str]:
    output_root = config.project_root
    episodes = PipelineUtils.get_episode_list(output_root)
    if step_name not in EXPECTED_FILES:
        return []
    required_files = EXPECTED_FILES[step_name]
    invalid_episodes = []
    for episode_id in episodes:
        for filename in required_files:
            if step_name == "0.3":
                file_path = os.path.join(output_root, "global", filename)
            elif step_name == "0.8":
                file_path = os.path.join(output_root, filename)
            else:
                file_path = os.path.join(output_root, episode_id, filename)
            if not os.path.exists(file_path):
                invalid_episodes.append(episode_id)
                break
            else:
                try:
                    if filename.endswith('.json'):
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            if step_name == "0.5":
                                valid = bool(
                                    (isinstance(data, dict) and (data.get('dialogue_turns') or data.get('turns') or data.get('dialogues')))
                                    or (isinstance(data, list) and len(data) > 0)
                                )
                                if not valid:
                                    invalid_episodes.append(episode_id)
                                    break
                            elif not data:
                                invalid_episodes.append(episode_id)
                                break
                    elif os.path.getsize(file_path) < 100:
                        invalid_episodes.append(episode_id)
                        break
                except Exception:
                    invalid_episodes.append(episode_id)
                    break
    return invalid_episodes

def delete_step_files(config: PipelineConfig, step_name: str, target_episodes: Optional[List[str]] = None):
    output_root = config.project_root
    all_steps = list(EXPECTED_FILES.keys())
    if step_name not in all_steps:
        return
    start_index = all_steps.index(step_name)
    for i in range(start_index, len(all_steps)):
        current_step = all_steps[i]
        required_files = EXPECTED_FILES[current_step]
        episodes = target_episodes or PipelineUtils.get_episode_list(output_root)
        for episode_id in episodes:
            for filename in required_files:
                if current_step == "0.3":
                    file_path = os.path.join(output_root, "global", filename)
                elif current_step == "0.8":
                    file_path = os.path.join(output_root, filename)
                else:
                    file_path = os.path.join(output_root, episode_id, filename)
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass

def _fail(step: str, result) -> bool:
    status = (result or {}).get("status")
    if step in ("0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8"):
        return status != "completed" and status != "already_exists"
    return status != "success" and status != "already_exists"

class PipelineStats:
    def __init__(self):
        self.step_stats: Dict[str, Dict[str, Any]] = {}
        self.total_start_time = time.time()
    
    def start_step(self, step_name: str):
        self.step_stats[step_name] = {
            "start_time": time.time(),
            "end_time": None,
            "duration": None,
            "tokens_used": 0,
            "status": "running"
        }
    
    def end_step(self, step_name: str, tokens_used: int = 0, status: str = "completed"):
        if step_name in self.step_stats:
            self.step_stats[step_name]["end_time"] = time.time()
            self.step_stats[step_name]["duration"] = self.step_stats[step_name]["end_time"] - self.step_stats[step_name]["start_time"]
            self.step_stats[step_name]["tokens_used"] = tokens_used
            self.step_stats[step_name]["status"] = status

    def to_dict(self):
        return {
            "total_duration": time.time() - self.total_start_time,
            "total_tokens": sum(stats.get("tokens_used", 0) for stats in self.step_stats.values()),
            "steps": self.step_stats
        }

def run_step(step_name: str, step_class, config: PipelineConfig, stats: PipelineStats, log_capture) -> tuple[bool, Any, Any]:
    log_capture.info(f"\n{step_name}: {step_class.__name__} …")
    stats.start_step(step_name)
    try:
        step_instance = step_class(config)
        result = step_instance.run()
        if _fail(step_name, result):
            log_capture.info(f"❌ {step_name} 失败: {result}")
            stats.end_step(step_name, status="failed")
            return False, step_instance, result
        # 文件检查（简化：跳过，或可加参数控制）
        tokens_used = result.get("tokens_used", 0) if isinstance(result, dict) else 0
        stats.end_step(step_name, tokens_used=tokens_used, status="completed")
        log_capture.info(f"✅ {step_name} 完成")
        return True, step_instance, result
    except Exception as e:
        log_capture.info(f"❌ {step_name} 异常: {e}")
        stats.end_step(step_name, status="failed")
        return False, None, {"error": str(e)}

# --- 核心执行函数 ---
async def run_pipeline(collection: str, start_step: str, mode: str, target_episodes: Optional[List[str]] = None, skip_integrity_check: bool = False):
    global log
    log = LogCapture(collection)
    
    PIPELINE_STATES[collection] = {
        "status": "running",
        "start_time": time.time(),
        "current_step": None,
        "error": None,
        "stats": None
    }

    try:
        base_output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "new_pipeline", "output"))
        outer_path = os.path.join(base_output_dir, collection)
        inner_same_path = os.path.join(outer_path, collection)
        output_root = os.path.abspath(inner_same_path) if os.path.isdir(inner_same_path) else os.path.abspath(outer_path)
        os.makedirs(output_root, exist_ok=True)

        config = PipelineConfig()
        config.config.setdefault("project", {})
        config.config["project"]["root_dir"] = output_root
        config.config["project"]["output_dir"] = output_root

        # 找起始索引
        start_index = next((i for i, (s, _, _) in enumerate(STEPS) if s == start_step), -1)
        if start_index == -1:
            raise ValueError(f"Invalid start_step: {start_step}")

        if mode == "force_rerun":
            log.info(f"🗑️ 强制重跑模式：删除步骤 {start_step} 的现有文件...")
            delete_step_files(config, start_step, target_episodes)

        stats = PipelineStats()
        report_generator = PipelineReportGenerator(config)

        for i in range(start_index, len(STEPS)):
            step_num, step_class, _ = STEPS[i]
            PIPELINE_STATES[collection]["current_step"] = step_num
            success, step_instance, step_result = run_step(step_num, step_class, config, stats, log)
            client = getattr(step_instance, 'client', None) if step_instance else None
            report_generator.record_step_end(f"step{step_num.replace('.', '_')}", step_result, client)
            if not success:
                raise Exception(f"Step {step_num} failed")

        # 成功
        PIPELINE_STATES[collection]["status"] = "completed"
        PIPELINE_STATES[collection]["stats"] = stats.to_dict()
        log.info("🎉 全流程完成")

        # 生成报告
        try:
            report_file = report_generator.generate_report(config.output_dir)
            log.info(f"📊 运行报告已生成: {report_file}")
        except Exception as e:
            log.info(f"⚠️ 报告生成失败: {e}")

    except Exception as e:
        PIPELINE_STATES[collection]["status"] = "failed"
        PIPELINE_STATES[collection]["error"] = str(e)
        log.info(f"❌ 流水线失败: {e}")
    finally:
        log = original_log  # 恢复

# --- Pydantic Models ---
class RunRequest(BaseModel):
    collection: str
    start_step: str = "0.1"
    mode: str = "resume"  # resume, force_rerun, fix_invalid
    target_episodes: Optional[List[str]] = None
    skip_integrity_check: bool = False

class StatusResponse(BaseModel):
    collection: str
    status: str  # running, completed, failed
    current_step: Optional[str] = None
    error: Optional[str] = None
    stats: Optional[Dict] = None

# --- FastAPI App ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时加载集合
    yield
    # 关闭时清理

app = FastAPI(title="剧集剧本生成流水线 API", lifespan=lifespan)

@app.get("/collections")
def list_collections():
    base_output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "new_pipeline", "output"))
    candidates = []
    try:
        for name in sorted(os.listdir(base_output_dir)):
            outer_path = os.path.join(base_output_dir, name)
            if not os.path.isdir(outer_path):
                continue
            inner_same = os.path.isdir(os.path.join(outer_path, name))
            has_global = os.path.isdir(os.path.join(outer_path, "global"))
            has_episode = any(d.startswith("episode_") for d in os.listdir(outer_path) if os.path.isdir(os.path.join(outer_path, d)))
            if inner_same or has_global or has_episode:
                candidates.append(name)
    except FileNotFoundError:
        pass
    return {"collections": candidates}

@app.get("/status/{collection}", response_model=StatusResponse)
def get_status(collection: str):
    if collection not in PIPELINE_STATES:
        # 检查文件状态（未运行过）
        base_output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "new_pipeline", "output"))
        outer_path = os.path.join(base_output_dir, collection)
        inner_same_path = os.path.join(outer_path, collection)
        output_root = os.path.abspath(inner_same_path) if os.path.isdir(inner_same_path) else os.path.abspath(outer_path)
        if not os.path.exists(output_root):
            raise HTTPException(status_code=404, detail="Collection not found")
        
        # 返回静态状态
        steps_status = {}
        for step_num, _, _ in STEPS:
            invalid = check_invalid_files(PipelineConfig(), step_num)  # 注意：这里 config 不完整，仅用于路径
            # 你需要正确构造 config，此处简化
            total = len(PipelineUtils.get_episode_list(output_root))
            valid = total - len(invalid)
            if step_num == "0.3":
                global_file = os.path.join(output_root, "global", "global_character_graph_llm.json")
                exists = os.path.exists(global_file)
                status = "completed" if exists else "not_started"
            else:
                if valid == total:
                    status = "completed"
                elif valid == 0:
                    status = "not_started"
                else:
                    status = "partial"
            steps_status[step_num] = status
        return StatusResponse(
            collection=collection,
            status="not_started",
            current_step=None,
            error=None,
            stats=None
        )
    return StatusResponse(
        collection=collection,
        **PIPELINE_STATES[collection]
    )

@app.post("/run")
def start_pipeline(request: RunRequest, background_tasks: BackgroundTasks):
    collection = request.collection
    if collection in PIPELINE_STATES and PIPELINE_STATES[collection]["status"] == "running":
        raise HTTPException(status_code=400, detail="Pipeline already running for this collection")
    
    # 验证 collection 存在
    base_output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "new_pipeline", "output"))
    outer_path = os.path.join(base_output_dir, collection)
    if not os.path.exists(outer_path):
        raise HTTPException(status_code=404, detail="Collection not found")

    # 处理 fix_invalid 模式
    if request.mode == "fix_invalid":
        # 构造 config 用于检查
        inner_same_path = os.path.join(outer_path, collection)
        output_root = os.path.abspath(inner_same_path) if os.path.isdir(inner_same_path) else os.path.abspath(outer_path)
        config = PipelineConfig()
        config.config["project"] = {"root_dir": output_root, "output_dir": output_root}
        invalid_episodes = check_invalid_files(config, request.start_step)
        if not invalid_episodes:
            return {"message": "No invalid files found", "status": "skipped"}
        request.target_episodes = invalid_episodes

    background_tasks.add_task(
        run_pipeline,
        collection=request.collection,
        start_step=request.start_step,
        mode=request.mode,
        target_episodes=request.target_episodes,
        skip_integrity_check=request.skip_integrity_check
    )
    return {"message": "Pipeline started", "collection": request.collection, "mode": request.mode}

@app.get("/logs/{collection}")
def get_logs(collection: str, limit: int = 100):
    logs = LOG_BUFFERS.get(collection, [])
    return {"logs": logs[-limit:]}
