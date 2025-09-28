#!/usr/bin/env python3
"""
FastAPI 服务：剧集剧本生成流水线 Web API
支持与 run_0_1_to_0_8.py 相同的所有功能和执行模式
"""

import os
import time
import json
import asyncio
from typing import Dict, List, Any, Optional, Literal
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from enum import Enum

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


class ExecutionMode(str, Enum):
    """执行模式枚举"""
    RERUN = "rerun"  # 从头开始重新执行
    RESUME = "resume"  # 从指定步骤开始（跳过已完成的步骤）
    FORCE_RERUN = "force_rerun"  # 强制重跑指定步骤
    FIX_INVALID = "fix_invalid"  # 只修复无效文件


class PipelineStats:
    """流水线统计信息"""
    def __init__(self):
        self.step_stats: Dict[str, Dict[str, Any]] = {}
        self.total_start_time = time.time()
    
    def start_step(self, step_name: str):
        """开始步骤计时"""
        self.step_stats[step_name] = {
            "start_time": time.time(),
            "end_time": None,
            "duration": None,
            "tokens_used": 0,
            "status": "running"
        }
    
    def end_step(self, step_name: str, tokens_used: int = 0, status: str = "completed"):
        """结束步骤计时"""
        if step_name in self.step_stats:
            self.step_stats[step_name]["end_time"] = time.time()
            self.step_stats[step_name]["duration"] = self.step_stats[step_name]["end_time"] - self.step_stats[step_name]["start_time"]
            self.step_stats[step_name]["tokens_used"] = tokens_used
            self.step_stats[step_name]["status"] = status
    
    def print_summary(self):
        """打印统计摘要"""
        log.info("\n" + "="*60)
        log.info("📊 流水线执行统计")
        log.info("="*60)
        
        total_duration = time.time() - self.total_start_time
        total_tokens = sum(stats.get("tokens_used", 0) for stats in self.step_stats.values())
        
        log.info(f"总执行时间: {total_duration:.2f}秒 ({total_duration/60:.1f}分钟)")
        log.info(f"总Token消耗: {total_tokens:,}")
        log.info("")
        
        for step_name, stats in self.step_stats.items():
            duration = stats.get("duration", 0)
            tokens = stats.get("tokens_used", 0)
            status = stats.get("status", "unknown")
            status_icon = "✅" if status == "completed" else "❌" if status == "failed" else "⏳"
            
            log.info(f"{status_icon} {step_name}: {duration:.2f}秒, {tokens:,} tokens")
        
        log.info("="*60)


def check_file_integrity(config: PipelineConfig, step_name: str) -> bool:
    """检查步骤输出文件的完整性"""
    output_root = config.project_root
    episodes = PipelineUtils.get_episode_list(output_root)
    
    # 定义每个步骤的预期输出文件
    expected_files = {
        "0.1": ["0_1_timed_dialogue.srt"],
        "0.2": ["0_2_clues.json"],
        "0.3": ["global_character_graph_llm.json"],
        "0.4": ["0_4_calibrated_dialogue.txt"],
        "0.5": ["0_5_dialogue_turns.json"],
        "0.6": ["0_6_script_flow.json", "0_6_script_draft.md"],
        "0.7": ["0_7_script.stmf", "0_7_script_analysis.json"],
        # 0.8 为集合级别汇总产物（存放在集合根目录 output_root 下）
        "0.8": ["0_8_complete_screenplay.fountain", "0_8_complete_screenplay.fdx"]
    }
    
    if step_name not in expected_files:
        log.info(f"⚠️ 未知步骤: {step_name}")
        return True
    
    required_files = expected_files[step_name]
    missing_files = []
    
    for episode_id in episodes:
        for filename in required_files:
            if step_name == "0.3":  # 全局文件（在 global/ 下）
                file_path = os.path.join(output_root, "global", filename)
            elif step_name == "0.8":  # 全局合并文件（集合根目录下）
                file_path = os.path.join(output_root, filename)
            else:  # 剧集文件
                file_path = os.path.join(output_root, episode_id, filename)
            
            if not os.path.exists(file_path):
                missing_files.append(f"{episode_id}/{filename}")
            else:
                # 检查文件是否为空或损坏
                try:
                    if filename.endswith('.json'):
                        with open(file_path, 'r', encoding='utf-8') as f:
                            json.load(f)
                    elif os.path.getsize(file_path) == 0:
                        missing_files.append(f"{episode_id}/{filename} (空文件)")
                except (json.JSONDecodeError, Exception) as e:
                    missing_files.append(f"{episode_id}/{filename} (损坏: {e})")
    
    if missing_files:
        log.info(f"❌ {step_name} 文件完整性检查失败:")
        for missing in missing_files[:10]:  # 只显示前10个
            log.info(f"   缺少: {missing}")
        if len(missing_files) > 10:
            log.info(f"   ... 还有 {len(missing_files) - 10} 个文件")
        return False
    
    log.info(f"✅ {step_name} 文件完整性检查通过")
    return True


def _fail(step: str, result) -> bool:
    """判断步骤是否失败"""
    status = (result or {}).get("status")
    if step in ("0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8"):
        return status != "completed" and status != "already_exists"
    return status != "success" and status != "already_exists"


def run_step(step_name: str, step_class, config: PipelineConfig, stats: PipelineStats) -> tuple[bool, Any, Any]:
    """运行单个步骤，返回 (成功状态, 步骤实例, 结果)"""
    log.info(f"\n{step_name}: {step_class.__name__} …")
    stats.start_step(step_name)
    
    try:
        step_instance = step_class(config)
        result = step_instance.run()
        
        # 检查步骤是否成功
        if _fail(step_name, result):
            log.info(f"❌ {step_name} 失败: {result}")
            stats.end_step(step_name, status="failed")
            return False, step_instance, result
        
        # 文件完整性检查
        if not check_file_integrity(config, step_name):
            log.info(f"❌ {step_name} 文件完整性检查失败")
            stats.end_step(step_name, status="failed")
            return False, step_instance, result
        
        # 尝试从结果中提取token使用量（如果可用）
        tokens_used = 0
        if isinstance(result, dict):
            tokens_used = result.get("tokens_used", 0)
        
        stats.end_step(step_name, tokens_used=tokens_used, status="completed")
        log.info(f"✅ {step_name} 完成")
        return True, step_instance, result
        
    except Exception as e:
        log.info(f"❌ {step_name} 异常: {e}")
        stats.end_step(step_name, status="failed")
        return False, None, {"error": str(e)}


def delete_step_files(config: PipelineConfig, step_name: str, target_episodes: Optional[List[str]] = None):
    """删除指定步骤及其之后步骤的输出文件"""
    output_root = config.project_root
    
    # 定义每个步骤的预期输出文件
    expected_files = {
        "0.1": ["0_1_timed_dialogue.srt"],
        "0.2": ["0_2_clues.json"],
        "0.3": ["global_character_graph_llm.json"],
        "0.4": ["0_4_calibrated_dialogue.txt"],
        "0.5": ["0_5_dialogue_turns.json"],
        "0.6": ["0_6_script_flow.json", "0_6_script_draft.md"],
        "0.7": ["0_7_script.stmf", "0_7_script_analysis.json"],
        "0.8": ["0_8_complete_screenplay.fountain", "0_8_complete_screenplay.fdx"]
    }
    
    # 获取所有步骤列表
    all_steps = list(expected_files.keys())
    
    # 找到起始步骤的索引
    if step_name not in all_steps:
        return
    
    start_index = all_steps.index(step_name)
    deleted_count = 0
    
    # 处理从起始步骤开始的所有步骤
    for i in range(start_index, len(all_steps)):
        current_step = all_steps[i]
        required_files = expected_files[current_step]
        
        if target_episodes:
            episodes = target_episodes
        else:
            episodes = PipelineUtils.get_episode_list(output_root)
        
        for episode_id in episodes:
            for filename in required_files:
                if current_step == "0.3":  # 全局文件（在 global/ 下）
                    file_path = os.path.join(output_root, "global", filename)
                elif current_step == "0.8":  # 全局合并文件（集合根目录下）
                    file_path = os.path.join(output_root, filename)
                else:  # 剧集文件
                    file_path = os.path.join(output_root, episode_id, filename)
                
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        deleted_count += 1
                        log.info(f"🗑️ 删除: {file_path}")
                    except Exception as e:
                        log.info(f"❌ 删除失败: {file_path} - {e}")
    
    log.info(f"✅ 删除了 {deleted_count} 个文件")


def check_invalid_files(config: PipelineConfig, step_name: str) -> List[str]:
    """检查指定步骤的无效文件"""
    output_root = config.project_root
    episodes = PipelineUtils.get_episode_list(output_root)
    
    # 定义每个步骤的预期输出文件
    expected_files = {
        "0.1": ["0_1_timed_dialogue.srt"],
        "0.2": ["0_2_clues.json"],
        "0.3": ["global_character_graph_llm.json"],
        "0.4": ["0_4_calibrated_dialogue.txt"],
        "0.5": ["0_5_dialogue_turns.json"],
        "0.6": ["0_6_script_flow.json", "0_6_script_draft.md"],
        "0.7": ["0_7_script.stmf", "0_7_script_analysis.json"],
        "0.8": ["0_8_complete_screenplay.fountain", "0_8_complete_screenplay.fdx"]
    }
    
    if step_name not in expected_files:
        return []
    
    required_files = expected_files[step_name]
    invalid_episodes = []
    
    for episode_id in episodes:
        for filename in required_files:
            if step_name == "0.3":  # 全局文件（在 global/ 下）
                file_path = os.path.join(output_root, "global", filename)
            elif step_name == "0.8":  # 全局合并文件（集合根目录下）
                file_path = os.path.join(output_root, filename)
            else:  # 剧集文件
                file_path = os.path.join(output_root, episode_id, filename)
            
            if not os.path.exists(file_path):
                invalid_episodes.append(episode_id)
                break
            else:
                # 检查文件是否为空或损坏
                try:
                    if filename.endswith('.json'):
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            # 针对不同步骤的结构校验
                            if step_name == "0.5":
                                valid = False
                                if isinstance(data, dict):
                                    # 接受 dialogue_turns（主）、turns/dialogues（兼容）
                                    valid = bool(data.get('dialogue_turns') or data.get('turns') or data.get('dialogues'))
                                elif isinstance(data, list):
                                    valid = len(data) > 0
                                if not valid:
                                    invalid_episodes.append(episode_id)
                                    break
                            else:
                                # 通用（宽松）校验：非空即可
                                if not data:
                                    invalid_episodes.append(episode_id)
                                    break
                    elif os.path.getsize(file_path) < 100:  # 文件太小
                        invalid_episodes.append(episode_id)
                        break
                except (json.JSONDecodeError, Exception):
                    invalid_episodes.append(episode_id)
                    break
    
    return invalid_episodes


# 定义 Pydantic 模型
class RunPipelineRequest(BaseModel):
    """运行流水线请求模型"""
    collection: str
    start_step: str = "0.1"
    bucket: Optional[str] = None
    skip_integrity_check: bool = False
    execution_mode: ExecutionMode = ExecutionMode.RESUME
    target_episodes: Optional[List[str]] = []


class CollectionResponse(BaseModel):
    """集合响应模型"""
    collections: List[str]


class StepStatusResponse(BaseModel):
    """步骤状态响应模型"""
    step: str
    description: str
    status: str
    invalid_files: List[str]
    valid_count: int
    total_count: int


class PipelineStatusResponse(BaseModel):
    """流水线状态响应模型"""
    collection: str
    steps: List[StepStatusResponse]
    output_dir: str


class PipelineResult(BaseModel):
    """流水线执行结果模型"""
    success: bool
    message: str
    report_file: Optional[str] = None
    stats: Optional[Dict[str, Any]] = None


# FastAPI 应用
app = FastAPI(title="剧集剧本生成流水线 API", 
              description="支持从 Step 0.1 到 Step 0.8 的完整流水线执行，支持多种执行模式",
              version="1.0.0")


@app.get("/")
def read_root():
    return {"message": "剧集剧本生成流水线 API", "version": "1.0.0"}


@app.get("/collections", response_model=CollectionResponse)
def get_collections():
    """获取可用集合列表"""
    base_output_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "new_pipeline", "output"))
    candidates = []
    
    try:
        for name in sorted(os.listdir(base_output_dir)):
            outer_path = os.path.join(base_output_dir, name)
            if not os.path.isdir(outer_path):
                continue
            inner_same = os.path.isdir(os.path.join(outer_path, name))
            has_global = os.path.isdir(os.path.join(outer_path, "global"))
            has_episode = False
            
            try:
                for d in os.listdir(outer_path):
                    if d.startswith("episode_") and os.path.isdir(os.path.join(outer_path, d)):
                        has_episode = True
                        break
            except Exception:
                pass
            
            if inner_same or has_global or has_episode:
                candidates.append(name)
    except FileNotFoundError:
        candidates = []
    
    return CollectionResponse(collections=candidates)


@app.post("/run_pipeline", response_model=PipelineResult)
async def run_pipeline(request: RunPipelineRequest):
    """运行流水线"""
    # 验证起始步骤
    valid_steps = ["0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8"]
    if request.start_step not in valid_steps:
        raise HTTPException(status_code=400, detail=f"无效的起始步骤: {request.start_step}，可用步骤: {', '.join(valid_steps)}")
    
    # 配置
    config = PipelineConfig()
    base_output_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "new_pipeline", "output"))
    outer_path = os.path.join(base_output_dir, request.collection)
    inner_same_path = os.path.join(outer_path, request.collection)
    
    # 兼容双层与单层目录结构
    if os.path.isdir(inner_same_path):
        output_root = os.path.abspath(inner_same_path)
    else:
        output_root = os.path.abspath(outer_path)
    
    if not os.path.exists(output_root):
        raise HTTPException(status_code=404, detail=f"集合目录不存在: {output_root}")
    
    # 允许外部覆盖 bucket
    if request.bucket:
        config.config.setdefault("gcp", {})
        config.config["gcp"]["bucket_name"] = request.bucket

    config.config.setdefault("project", {})
    config.config["project"]["root_dir"] = output_root
    config.config["project"]["output_dir"] = output_root

    # 检查输入目录，root_dir
    log.info(f"项目根目录: {config.project_root}, 输出目录: {config.output_dir}")

    # 确定目标剧集
    target_episodes = []
    if request.execution_mode == ExecutionMode.FIX_INVALID:
        # 检查无效文件
        invalid_episodes = check_invalid_files(config, request.start_step)
        if not invalid_episodes:
            log.info(f"✅ 步骤 {request.start_step} 没有无效文件")
            return PipelineResult(success=True, message=f"步骤 {request.start_step} 没有无效文件需要修复")
        target_episodes = invalid_episodes
    elif request.target_episodes:
        target_episodes = request.target_episodes

    log.info("="*60)
    log.info("🎬 剧集剧本生成流水线")
    log.info("="*60)
    log.info(f"输出目录: {output_root}")
    log.info(f"集合名称: {request.collection}")
    log.info(f"起始步骤: {request.start_step}")
    log.info(f"执行模式: {request.execution_mode}")
    log.info(f"跳过完整性检查: {request.skip_integrity_check}")
    if target_episodes:
        log.info(f"目标剧集: {len(target_episodes)} 个")
    log.info("="*60)

    # 初始化统计
    stats = PipelineStats()
    
    # 定义步骤列表
    steps = [
        ("0.1", Step0_1ASR, "ASR"),
        ("0.2", Step0_2ClueExtraction, "线索提取"),
        ("0.3", Step0_3GlobalAlignmentLLM, "全局角色对齐"),
        ("0.4", Step0_4SpeakerCalibration, "说话人校准"),
        ("0.5", Step0_5IntegratedAnalysis, "对话轮次重构"),
        ("0.6", Step0_6PlotExtraction, "情节抽取"),
        ("0.7", Step0_7ScriptWriting, "剧本撰写（STMF）"),
        ("0.8", Step0_8FinalScript, "合并与导出")
    ]
    
    # 找到起始步骤
    start_index = 0
    for i, (step_num, _, _) in enumerate(steps):
        if step_num == request.start_step:
            start_index = i
            break
    else:
        raise HTTPException(status_code=400, detail=f"无效的起始步骤: {request.start_step}")
    
    # 处理强制重跑模式
    if request.execution_mode == ExecutionMode.FORCE_RERUN:
        log.info(f"🗑️ 强制重跑模式：删除步骤 {request.start_step} 的现有文件...")
        log.info("⚠️ 注意：强制重跑将删除现有文件，请确保已备份重要数据")
        delete_step_files(config, request.start_step, target_episodes)
    
    log.info(f"从步骤 {request.start_step} 开始执行 ({request.execution_mode} 模式)...")
    
    # 初始化报告生成器
    report_generator = PipelineReportGenerator(config)
    
    try:
        # 执行步骤
        for i in range(start_index, len(steps)):
            step_num, step_class, step_desc = steps[i]
            step_name = f"step{step_num.replace('.', '_')}"
            
            # 记录步骤开始
            report_generator.record_step_start(step_name)
            
            # 如果跳过完整性检查，则修改检查函数
            if request.skip_integrity_check:
                global check_file_integrity
                original_check = check_file_integrity
                check_file_integrity = lambda config, step_name: True
            
            success, step_instance, step_result = run_step(step_num, step_class, config, stats)
            
            # 恢复原始检查函数
            if request.skip_integrity_check:
                check_file_integrity = original_check
            
            # 记录步骤结束（获取客户端token使用信息）
            client = getattr(step_instance, 'client', None) if step_instance else None
            report_generator.record_step_end(step_name, step_result, client)
            
            if not success:
                log.info(f"\n❌ 流水线在步骤 {step_num} 失败")
                stats.print_summary()
                return PipelineResult(success=False, message=f"流水线在步骤 {step_num} 失败: {step_result}")
        
        log.info("\n🎉 全流程完成")
        stats.print_summary()
        
        # 生成运行报告
        try:
            report_file = report_generator.generate_report(config.output_dir)
            log.info(f"\n📊 运行报告已生成: {report_file}")
            return PipelineResult(success=True, 
                                message="流水线执行成功", 
                                report_file=report_file, 
                                stats={ 
                                    "total_duration": time.time() - stats.total_start_time,
                                    "total_tokens": sum(s.get("tokens_used", 0) for s in stats.step_stats.values()),
                                    "step_stats": stats.step_stats
                                })
        except Exception as e:
            log.info(f"\n⚠️ 报告生成失败: {e}")
            return PipelineResult(success=True, 
                                message="流水线执行成功但报告生成失败", 
                                stats={ 
                                    "total_duration": time.time() - stats.total_start_time,
                                    "total_tokens": sum(s.get("tokens_used", 0) for s in stats.step_stats.values()),
                                    "step_stats": stats.step_stats
                                })
    
    except Exception as e:
        log.info(f"\n❌ 运行异常: {e}")
        stats.print_summary()
        return PipelineResult(success=False, message=f"运行异常: {e}")


@app.get("/check_status/{collection}", response_model=PipelineStatusResponse)
def check_pipeline_status(collection: str):
    """检查指定集合的流水线状态"""
    base_output_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "new_pipeline", "output"))
    collection_path = os.path.join(base_output_dir, collection)
    inner_same_path = os.path.join(collection_path, collection)
    
    # 兼容双层与单层目录结构
    if os.path.isdir(inner_same_path):
        output_root = os.path.abspath(inner_same_path)
    else:
        output_root = os.path.abspath(collection_path)
    
    if not os.path.exists(output_root):
        raise HTTPException(status_code=404, detail=f"集合目录不存在: {output_root}")
    
    # 配置
    config = PipelineConfig()
    config.config.setdefault("project", {})
    config.config["project"]["root_dir"] = output_root
    config.config["project"]["output_dir"] = output_root
    
    # 检查各步骤的文件状态
    steps = [
        ("0.1", "ASR"),
        ("0.2", "线索提取"),
        ("0.3", "全局角色对齐"),
        ("0.4", "说话人校准"),
        ("0.5", "对话轮次重构"),
        ("0.6", "情节抽取"),
        ("0.7", "剧本撰写（STMF）"),
        ("0.8", "合并与导出")
    ]
    
    step_statuses = []
    for step_num, step_desc in steps:
        invalid_files = check_invalid_files(config, step_num)
        total_episodes = len(PipelineUtils.get_episode_list(config.project_root))
        valid_count = total_episodes - len(invalid_files)
        
        if step_num == "0.3":  # 全局步骤
            candidates = [
                os.path.join(config.project_root, "global", "global_character_graph_llm.json"),
                os.path.join(config.project_root, "global_character_graph_llm.json"),
            ]
            if any(os.path.exists(p) for p in candidates):
                status = "✅ 完成"
            else:
                status = "❌ 未完成"
        else:
            if len(invalid_files) == 0:
                status = "✅ 完成"
            elif len(invalid_files) == total_episodes:
                status = "❌ 未开始"
            else:
                status = f"⚠️ 部分完成 ({valid_count}/{total_episodes})"
        
        step_statuses.append(StepStatusResponse(
            step=step_num,
            description=step_desc,
            status=status,
            invalid_files=invalid_files,
            valid_count=valid_count,
            total_count=total_episodes
        ))
    
    return PipelineStatusResponse(
        collection=collection,
        steps=step_statuses,
        output_dir=output_root
    )


@app.get("/step_files/{collection}/{step_name}")
def check_step_files(collection: str, step_name: str):
    """检查指定步骤的文件完整性"""
    base_output_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "new_pipeline", "output"))
    collection_path = os.path.join(base_output_dir, collection)
    inner_same_path = os.path.join(collection_path, collection)
    
    # 兼容双层与单层目录结构
    if os.path.isdir(inner_same_path):
        output_root = os.path.abspath(inner_same_path)
    else:
        output_root = os.path.abspath(collection_path)
    
    if not os.path.exists(output_root):
        raise HTTPException(status_code=404, detail=f"集合目录不存在: {output_root}")
    
    # 配置
    config = PipelineConfig()
    config.config.setdefault("project", {})
    config.config["project"]["root_dir"] = output_root
    config.config["project"]["output_dir"] = output_root
    
    # 检查文件完整性
    is_valid = check_file_integrity(config, step_name)
    
    if is_valid:
        return {"step": step_name, "collection": collection, "valid": True, "message": f"步骤 {step_name} 文件完整性检查通过"}
    else:
        invalid_files = check_invalid_files(config, step_name)
        return {"step": step_name, "collection": collection, "valid": False, "message": f"步骤 {step_name} 文件完整性检查失败", "invalid_files": invalid_files}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)