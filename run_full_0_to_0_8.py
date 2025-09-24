#!/usr/bin/env python3
"""
从 Step0 到 Step0.8 的一键运行脚本。

环境变量（可选）：
- STEP0_INPUT_DIR: 本地 mp4 输入目录
- STEP0_COLLECTION: 输出集合名称（将作为 output 下的子目录名，同时用于 GCS 前缀）
- STEP0_BUCKET_NAME: GCS 存储桶名（覆盖 pipeline.yaml 中的 gcp.bucket_name）
- SKIP_LLM=1: Step0.4 仅回填，跳过 LLM
- FORCE_OVERWRITE=1: 跳过已存在输出检查（各步自身判定）

使用：
  python run_full_0_to_0_8.py
"""

import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from new_pipeline.core import PipelineConfig
from new_pipeline.steps.step0_upload import Step0Upload
from new_pipeline.steps.step0_1_asr import Step0_1ASR
from new_pipeline.steps.step0_2_clue_extraction import Step0_2ClueExtraction
from new_pipeline.steps.step0_3_global_alignment_llm import Step0_3GlobalAlignmentLLM
from new_pipeline.steps.step0_4_speaker_calibration import Step0_4SpeakerCalibration
from new_pipeline.steps.step0_5_integrated_analysis import Step0_5IntegratedAnalysis
from new_pipeline.steps.step0_6_plot_extraction import Step0_6PlotExtraction
from new_pipeline.steps.step0_7_script_writing import Step0_7ScriptWriting
from new_pipeline.steps.step0_8_final_script import Step0_8FinalScript

# 日志，patch了print。
from nb_log import get_logger
logger = get_logger(__name__)

load_dotenv()  # 从 .env 文件加载环境变量（如果存在）

def _fail(step: str, result) -> bool:
    status = (result or {}).get("status")
    if step in ("0",):
        return status != "success" and status != "completed" and status != "already_exists"
    if step in ("0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8"):
        return status != "completed" and status != "already_exists"
    return status != "success" and status != "already_exists"


def main() -> int:
    print("=== 运行 Step0 → Step0.8 ===")

    # 读取输入与集合
    input_dir = os.environ.get("STEP0_INPUT_DIR", "").strip()
    collection = os.environ.get("STEP0_COLLECTION", "").strip()

    if not input_dir:
        print("❌ 缺少 STEP0_INPUT_DIR 环境变量（本地 mp4 输入目录）")
        return 1
    if not os.path.isdir(input_dir):
        print(f"❌ 输入目录不存在: {input_dir}")
        return 1
    if not collection:
        collection = os.path.basename(input_dir.rstrip(os.sep)) + "_output"
        os.environ["STEP0_COLLECTION"] = collection

    # 配置
    config = PipelineConfig()
    # 修改输出目录命名方式：输入路径名称 + 集合名称
    input_basename = os.path.basename(input_dir.rstrip(os.sep))
    combined_collection_name = f"{input_basename}_{collection}"
    output_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "new_pipeline", "output", combined_collection_name))
    os.makedirs(output_root, exist_ok=True)

    # 允许外部覆盖 bucket
    if os.environ.get("STEP0_BUCKET_NAME"):
        config.config.setdefault("gcp", {})
        config.config["gcp"]["bucket_name"] = os.environ["STEP0_BUCKET_NAME"]

    config.config.setdefault("project", {})
    config.config["project"]["root_dir"] = output_root
    config.config["project"]["output_dir"] = output_root

    print(f"输入目录: {input_dir}")
    print(f"输出目录: {output_root}")
    print(f"集合名称: {collection}")

    try:
        # Step0: 上传到 GCS
        print("\n1) Step0: 上传到 GCS …")
        s0 = Step0Upload(config)
        r0 = s0.run()
        if _fail("0", r0):
            print(f"❌ Step0 失败: {r0}")
            return 1
        print("✅ Step0 完成")

        # Step0.1: ASR
        print("\n2) Step0.1: ASR …")
        s01 = Step0_1ASR(config)
        r01 = s01.run()
        if _fail("0.1", r01):
            print(f"❌ Step0.1 失败: {r01}")
            return 1
        print("✅ Step0.1 完成")

        # Step0.2: 线索提取
        print("\n3) Step0.2: 线索提取 …")
        s02 = Step0_2ClueExtraction(config)
        r02 = s02.run()
        if _fail("0.2", r02):
            print(f"❌ Step0.2 失败: {r02}")
            return 1
        print("✅ Step0.2 完成")

        # Step0.3: 全局对齐
        print("\n4) Step0.3: 全局角色对齐 …")
        s03 = Step0_3GlobalAlignmentLLM(config)
        r03 = s03.run()
        if _fail("0.3", r03):
            print(f"❌ Step0.3 失败: {r03}")
            return 1
        print("✅ Step0.3 完成")

        # Step0.4: 说话人校准（支持 SKIP_LLM=1 回填模式）
        print("\n5) Step0.4: 说话人校准 …")
        s04 = Step0_4SpeakerCalibration(config)
        r04 = s04.run()
        if _fail("0.4", r04):
            print(f"❌ Step0.4 失败: {r04}")
            return 1
        print("✅ Step0.4 完成")

        # Step0.5: 对话轮次重构
        print("\n6) Step0.5: 对话轮次重构 …")
        s05 = Step0_5IntegratedAnalysis(config)
        r05 = s05.run()
        if _fail("0.5", r05):
            print(f"❌ Step0.5 失败: {r05}")
            return 1
        print("✅ Step0.5 完成")

        # Step0.6: 情节抽取
        print("\n7) Step0.6: 情节抽取 …")
        s06 = Step0_6PlotExtraction(config)
        r06 = s06.run()
        if _fail("0.6", r06):
            print(f"❌ Step0.6 失败: {r06}")
            return 1
        print("✅ Step0.6 完成")

        # Step0.7: 剧本撰写（STMF）
        print("\n8) Step0.7: 剧本撰写（STMF） …")
        s07 = Step0_7ScriptWriting(config)
        r07 = s07.run()
        if _fail("0.7", r07):
            print(f"❌ Step0.7 失败: {r07}")
            return 1
        print("✅ Step0.7 完成")

        # Step0.8: 合并与导出（STMf→Fountain/FDX）
        print("\n9) Step0.8: 合并与导出 …")
        s08 = Step0_8FinalScript(config)
        r08 = s08.run()
        if _fail("0.8", r08):
            print(f"❌ Step0.8 失败: {r08}")
            return 1
        print("✅ Step0.8 完成")

        print("\n🎉 全流程完成")
        return 0

    except Exception as e:
        print(f"❌ 运行异常: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


