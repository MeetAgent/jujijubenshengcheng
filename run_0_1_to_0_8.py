#!/usr/bin/env python3
"""
从 Step0.1 到 Step0.8 的一键运行脚本（跳过Step0上传）
支持从指定步骤开始重试、统计token和用时、文件完整性检查
"""

import os
import sys
import time
import json
import argparse
from typing import Dict, List, Any, Optional

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


def delete_step_files(config: PipelineConfig, step_name: str, target_episodes: List[str] = None):
    """删除指定步骤的输出文件"""
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
    
    if step_name not in expected_files:
        return
    
    required_files = expected_files[step_name]
    deleted_count = 0
    
    if target_episodes:
        episodes = target_episodes
    else:
        episodes = PipelineUtils.get_episode_list(output_root)
    
    for episode_id in episodes:
        for filename in required_files:
            if step_name == "0.3":  # 全局文件（在 global/ 下）
                file_path = os.path.join(output_root, "global", filename)
            elif step_name == "0.8":  # 全局合并文件（集合根目录下）
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


def interactive_step_selection(config: PipelineConfig) -> tuple:
    """互动式步骤选择"""
    log.info("\n" + "="*60)
    log.info("🎯 互动式流水线配置")
    log.info("="*60)
    
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
    
    log.info("📊 各步骤文件状态检查:")
    step_status = {}
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
        
        step_status[step_num] = {
            "status": status,
            "invalid_files": invalid_files,
            "valid_count": valid_count,
            "total_count": total_episodes
        }
        log.info(f"  {step_num}: {step_desc} - {status}")
    
    log.info("\n🎮 选择执行模式:")
    log.info("1. 从头开始 (重新执行所有步骤)")
    log.info("2. 从指定步骤开始 (跳过已完成的步骤)")
    log.info("3. 强制重跑指定步骤 (删除现有文件重新执行)")
    log.info("4. 修复无效文件 (只重跑有问题的文件)")
    
    while True:
        try:
            choice = input("\n请选择模式 (1-4): ").strip()
            if choice in ["1", "2", "3", "4"]:
                break
            log.info("❌ 无效选择，请输入 1-4")
        except KeyboardInterrupt:
            log.info("\n👋 用户取消")
            return None, None, None
    
    if choice == "1":
        return "0.1", "rerun", []
    elif choice == "2":
        log.info("\n📋 可用步骤:")
        for step_num, step_desc in steps:
            log.info(f"  {step_num}: {step_desc}")
        
        while True:
            try:
                start_step = input("\n请输入起始步骤 (0.1-0.8): ").strip()
                if start_step in [s[0] for s in steps]:
                    return start_step, "resume", []
                log.info("❌ 无效步骤，请输入 0.1-0.8")
            except KeyboardInterrupt:
                log.info("\n👋 用户取消")
                return None, None, None
    elif choice == "3":
        log.info("\n📋 可重跑的步骤:")
        for step_num, step_desc in steps:
            status_info = step_status[step_num]
            log.info(f"  {step_num}: {step_desc} - {status_info['status']}")
        
        while True:
            try:
                target_step = input("\n请输入要重跑的步骤 (0.1-0.8): ").strip()
                if target_step in [s[0] for s in steps]:
                    return target_step, "force_rerun", []
                log.info("❌ 无效步骤，请输入 0.1-0.8")
            except KeyboardInterrupt:
                log.info("\n👋 用户取消")
                return None, None, None
    elif choice == "4":
        log.info("\n🔧 检测到的问题文件:")
        problem_steps = []
        for step_num, step_desc in steps:
            status_info = step_status[step_num]
            if status_info["invalid_files"]:
                problem_steps.append(step_num)
                log.info(f"  {step_num}: {step_desc} - {len(status_info['invalid_files'])} 个问题文件")
        
        if not problem_steps:
            log.info("✅ 没有发现问题文件")
            return None, None, None
        
        while True:
            try:
                target_step = input(f"\n请选择要修复的步骤 ({'/'.join(problem_steps)}): ").strip()
                if target_step in problem_steps:
                    return target_step, "fix_invalid", step_status[target_step]["invalid_files"]
                log.info(f"❌ 无效步骤，请从 {problem_steps} 中选择")
            except KeyboardInterrupt:
                log.info("\n👋 用户取消")
                return None, None, None


def main() -> int:
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="运行剧集剧本生成流水线")
    parser.add_argument("--start-step", type=str, default="0.1", 
                       help="从指定步骤开始 (0.1-0.8, 默认: 0.1)")
    parser.add_argument("--collection", type=str, 
                       help="输出集合名称 (覆盖环境变量)")
    parser.add_argument("--bucket", type=str,
                       help="GCS存储桶名称 (覆盖环境变量)")
    parser.add_argument("--skip-integrity-check", action="store_true",
                       help="跳过文件完整性检查")
    parser.add_argument("--interactive", action="store_true",
                       help="启用互动式模式")
    parser.add_argument("--force-rerun", action="store_true",
                       help="强制重跑指定步骤")
    parser.add_argument("--fix-invalid", action="store_true",
                       help="只修复无效文件")
    
    # 检查是否没有传入任何参数
    if len(sys.argv) == 1:
        log.info("🎬 剧集剧本生成流水线")
        log.info("="*60)
        log.info("📋 使用说明:")
        log.info("  本工具支持从 Step 0.1 到 Step 0.8 的完整流水线执行")
        log.info("")
        log.info("🚀 快速开始:")
        log.info("  python run_0_1_to_0_8.py --interactive")
        log.info("  (推荐: 启用互动式模式，引导您完成配置)")
        log.info("")
        log.info("⚙️ 主要参数:")
        log.info("  --interactive           启用互动式模式 (推荐新手使用)")
        log.info("  --collection NAME       指定输出集合名称")
        log.info("  --start-step STEP       从指定步骤开始 (0.1-0.8)")
        log.info("  --force-rerun          强制重跑指定步骤")
        log.info("  --fix-invalid          只修复无效文件")
        log.info("  --skip-integrity-check  跳过文件完整性检查")
        log.info("  --bucket NAME          GCS存储桶名称")
        log.info("")
        log.info("📖 使用示例:")
        log.info("  # 互动式运行 (推荐)")
        log.info("  python run_0_1_to_0_8.py --interactive")
        log.info("")
        log.info("  # 从头开始运行指定集合")
        log.info("  python run_0_1_to_0_8.py --collection 我的剧本集合")
        log.info("")
        log.info("  # 从指定步骤开始")
        log.info("  python run_0_1_to_0_8.py --collection 我的剧本集合 --start-step 0.5")
        log.info("")
        log.info("  # 强制重跑某个步骤")
        log.info("  python run_0_1_to_0_8.py --collection 我的剧本集合 --start-step 0.6 --force-rerun")
        log.info("")
        log.info("💡 提示: 使用 --help 查看完整参数说明")
        log.info("="*60)
        return 0
    
    args = parser.parse_args()
    
    # 读取集合名称（互动模式下允许交互选择）
    collection = args.collection or os.environ.get("STEP0_COLLECTION", "").strip()
    if args.interactive and not collection:
        # 在 new_pipeline/output 下列出候选集合目录（支持双层同名或单层包含 episode_*/global 的目录）
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
        if not candidates:
            log.info("❌ 未找到可用集合目录，请使用 --collection 指定或先生成输出")
            return 1
        log.info("\n📂 可用集合目录：")
        for idx, name in enumerate(candidates, 1):
            log.info(f"  {idx}. {name}")
        while True:
            try:
                sel = input("\n请输入要使用的集合编号: ").strip()
                if sel.isdigit() and 1 <= int(sel) <= len(candidates):
                    collection = candidates[int(sel) - 1]
                    break
                log.info("❌ 无效选择，请输入有效编号")
            except KeyboardInterrupt:
                log.info("\n👋 用户取消")
                return 1
    if not collection:
        log.info("❌ 缺少集合名称，请使用 --collection 参数或设置 STEP0_COLLECTION 环境变量")
        return 1

    # 配置
    config = PipelineConfig()
    base_output_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "new_pipeline", "output"))
    outer_path = os.path.join(base_output_dir, collection)
    inner_same_path = os.path.join(outer_path, collection)
    # 兼容双层与单层目录结构
    if os.path.isdir(inner_same_path):
        output_root = os.path.abspath(inner_same_path)
    else:
        output_root = os.path.abspath(outer_path)
    os.makedirs(output_root, exist_ok=True)

    # 允许外部覆盖 bucket
    bucket_name = args.bucket or os.environ.get("STEP0_BUCKET_NAME")
    if bucket_name:
        config.config.setdefault("gcp", {})
        config.config["gcp"]["bucket_name"] = bucket_name

    config.config.setdefault("project", {})
    config.config["project"]["root_dir"] = output_root
    config.config["project"]["output_dir"] = output_root

    # 检查输入目录，root_dir
    log.info(f"项目根目录: {config.project_root}, 输出目录: {config.output_dir}")


    # 互动式模式处理
    if args.interactive:
        result = interactive_step_selection(config)
        if result is None:
            return 1
        start_step, mode, target_episodes = result
    else:
        start_step = args.start_step
        if args.force_rerun:
            mode = "force_rerun"
        elif args.fix_invalid:
            mode = "fix_invalid"
            # 检查无效文件
            invalid_episodes = check_invalid_files(config, start_step)
            if not invalid_episodes:
                log.info(f"✅ 步骤 {start_step} 没有无效文件")
                return 0
            target_episodes = invalid_episodes
        else:
            mode = "resume"
        target_episodes = []

    log.info("="*60)
    log.info("🎬 剧集剧本生成流水线")
    log.info("="*60)
    log.info(f"输出目录: {output_root}")
    log.info(f"集合名称: {collection}")
    log.info(f"起始步骤: {start_step}")
    log.info(f"执行模式: {mode}")
    log.info(f"跳过完整性检查: {args.skip_integrity_check}")
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
        if step_num == start_step:
            start_index = i
            break
    else:
        log.info(f"❌ 无效的起始步骤: {start_step}")
        log.info(f"可用步骤: {', '.join([s[0] for s in steps])}")
        return 1
    
    # 处理强制重跑模式
    if mode == "force_rerun":
        log.info(f"🗑️ 强制重跑模式：删除步骤 {start_step} 的现有文件...")
        log.info("⚠️ 注意：强制重跑将删除现有文件，请确保已备份重要数据")
        delete_step_files(config, start_step, target_episodes)
    
    log.info(f"从步骤 {start_step} 开始执行 ({mode} 模式)...")
    
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
            if args.skip_integrity_check:
                global check_file_integrity
                original_check = check_file_integrity
                check_file_integrity = lambda config, step_name: True
            
            success, step_instance, step_result = run_step(step_num, step_class, config, stats)
            
            # 恢复原始检查函数
            if args.skip_integrity_check:
                check_file_integrity = original_check
            
            # 记录步骤结束（获取客户端token使用信息）
            client = getattr(step_instance, 'client', None) if step_instance else None
            report_generator.record_step_end(step_name, step_result, client)
            
            if not success:
                log.info(f"\n❌ 流水线在步骤 {step_num} 失败")
                stats.print_summary()
                return 1
        
        log.info("\n🎉 全流程完成")
        stats.print_summary()
        
        # 生成运行报告
        try:
            report_file = report_generator.generate_report(config.output_dir)
            log.info(f"\n📊 运行报告已生成: {report_file}")
        except Exception as e:
            log.info(f"\n⚠️ 报告生成失败: {e}")
        
        return 0

    except KeyboardInterrupt:
        log.info("\n⚠️ 用户中断执行")
        stats.print_summary()
        return 1
    except Exception as e:
        log.info(f"\n❌ 运行异常: {e}")
        stats.print_summary()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


