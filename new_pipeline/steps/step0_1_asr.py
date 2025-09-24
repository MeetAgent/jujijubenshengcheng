"""
Step0.1: 高精度ASR与说话人分离
基于严格的证据层级铁律，生成高精度的"台词+时间戳+匿名说话人ID"日志。
输出：
- 0_1_dialogue_detailed.json (机器可读的真相来源)
- 0_1_timed_dialogue.srt (人类可读的、用于调试的字幕文件)
"""

import os
import json
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pydantic import BaseModel

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import PipelineConfig, GenAIClient, PipelineUtils
from . import PipelineStep


class DialogueTurn(BaseModel):
    """对话轮次模型"""
    start_ms: int
    end_ms: int
    speaker_id: str
    line: str


class DialogueResponse(BaseModel):
    """对话响应模型"""
    dialogues: List[DialogueTurn]


class Step0_1ASR(PipelineStep):
    @property
    def step_number(self) -> int:
        return 0  # 作为前置，编号0，但名称区分为0.1

    @property
    def step_name(self) -> str:
        return "asr_pre"

    def check_dependencies(self, episode_id: str = None) -> bool:
        return True

    def get_output_files(self, episode_id: str = None) -> List[str]:
        if episode_id:
            return [
                f"{episode_id}/0_1_timed_dialogue.txt"
            ]
        return []

    def run(self, episode_id: str = None) -> Dict[str, Any]:
        if episode_id:
            return self._run_single_episode(episode_id)
        else:
            return self._run_all_episodes()

    def _run_single_episode(self, episode_id: str) -> Dict[str, Any]:
        import time
        start_time = time.time()
        
        # 输出路径（直接定位到 episode 输出目录）
        episode_out_dir = self.utils.get_episode_output_dir(self.config.output_dir, episode_id)
        # 双输出格式：JSON作为真相来源，SRT用于调试
        json_file = os.path.join(episode_out_dir, "0_1_dialogue_detailed.json")
        srt_file = os.path.join(episode_out_dir, "0_1_timed_dialogue.srt")

        if os.path.exists(json_file) and not os.environ.get("FORCE_OVERWRITE"):
            print(f"✅ {episode_id} 已有Step0.1输出文件，跳过处理")
            return {"status": "already_exists"}

        # 与 Step1 一致的 GCS 优先策略
        step0_path = self.utils.get_step_file_path(self.config.output_dir, episode_id, 0, "gcs_path.txt")
        video_uri = self.utils.load_text_file(step0_path, "").strip()
        if not video_uri:
            legacy_path = os.path.join(self.config.project_root, episode_id, "gcs_path.txt")
            if os.path.exists(legacy_path):
                video_uri = self.utils.load_text_file(legacy_path, "").strip()
        if not video_uri:
            legacy0_path = os.path.join(self.config.project_root, episode_id, "0_gcs_path.txt")
            if os.path.exists(legacy0_path):
                video_uri = self.utils.load_text_file(legacy0_path, "").strip()
        if not video_uri:
            video_uri = self.utils.get_video_uri(episode_id, self.config.project_root)
        # 使用与 step1 相同的模型配置（若缺失则回退 step3，再回退默认）
        model_config = self.config.get_model_config(1)

        # 按照证据层级铁律优化的system instruction
        system_instruction = """你是顶级的音视频分析引擎，正在执行一个高精度的转写与说话人日志生成任务。你的唯一目标是客观、准确地记录"在什么时间、谁、说了什么"。你必须严格遵循以下的【证据层级铁律】进行判断。

【铁律一：关于"内容"的真相来源】
1. **字幕 > 声音 (Text Source: Subtitles > ASR)**: 如果视频画面中存在硬字幕，字幕的文本内容是"第一级真相"，必须优先采纳。
2. **声音补充**: 只有在没有字幕或字幕明显不完整时，才使用你自己的语音识别（ASR）能力来补充或生成文本。
3. **忽略非对话**: 严格过滤所有背景音、音乐、音效。

【铁律二：关于"说话人"的真相来源】
1. **声音特征 > 视觉画面 (Speaker ID: Voiceprint > Visuals)**: 这是最高指令。你必须为每一个独特的"声音特征组合"（音色、音调、语速）分配一个稳定的`spk_ID`。
2. **"零样本声音克隆"思维**: 在你的内部处理中，为`spk_0`建立一个声音模型。当出现新的声音时，先与已有的声音模型（`spk_0`, `spk_1`...）进行比对。只有在你确认这是一个全新的、之前未出现过的声音时，才能分配一个新的ID（如 `spk_2`）。
3. **视觉作为辅助验证**: 画面中的人物口型和状态，只能作为"辅助证据"或"消歧线索"。**如果一个已知的声音（例如 `spk_0`）在说话，但画面上是另一个人，你必须相信声音，将说话人标记为 `spk_0`。** 这是纠正"镜头归属错误"的关键。

【铁律三：关于"分割"的客观标准】
1. **放弃语义合并**: 在此步骤，你的任务不是创造语义完整的长句。那是下游步骤的工作。
2. **技术性分割**: 你的分割点应该是基于以下客观事件：
   - **说话人变更**: 只要说话人ID发生变化，就必须切分。
   - **显著停顿**: 同一说话人连续说话时，若出现超过2秒的明显停顿，应进行切分。
   - **字幕块边界**: 每一个独立的字幕块，都应该是一个独立的对话条目。

【输出要求】
- 严格按照JSON schema输出。
- `speaker_id` 必须在整个视频中保持绝对的稳定性。
- `line` 的内容必须优先反映字幕。
- 时间戳必须精确到毫秒，反映声音的实际起止。"""

        user_prompt = """请严格遵循你在系统指令中学到的【证据层级铁律】，分析这个视频，生成高精度的对话日志。

特别强调：
1. **声音特征优先**：即使画面上是另一个人，只要声音特征匹配已知的spk_ID，就必须标记为该ID
2. **字幕内容优先**：优先使用视频中的字幕内容，字幕不完整时才使用声音识别
3. **技术性分割**：按说话人变更、显著停顿、字幕块边界进行分割，不要进行语义合并
4. **稳定性保证**：同一声音在整个视频中必须保持相同的spk_ID"""

        max_retries = 3
        retry_delay = 2  # 重试间隔秒数
        
        # 优化模型选择策略：优先使用2.5-flash提升速度，禁止1.5系列
        model_list = [
            'gemini-2.5-flash',  # 优先：速度与质量平衡，适合ASR任务
            'gemini-2.5-pro',    # 备选：最强推理能力
            'gemini-2.0-flash'   # 备选方案
        ]
        
        for attempt in range(max_retries + 2):  # 增加2次模型切换重试
            try:
                # 选择模型：前3次使用主模型，后2次切换备用模型
                if attempt < max_retries:
                    model_name = model_list[0]  # 使用主模型
                else:
                    model_name = model_list[attempt - max_retries + 1]  # 切换备用模型
                    print(f"🔄 第{attempt+1}次重试，切换模型: {model_name}")
                
                if attempt > 0:
                    print(f"🔄 第{attempt+1}次重试，使用模型: {model_name}")
                    import time
                    time.sleep(retry_delay * min(attempt, 3))  # 最大延迟6秒
                # --- [优化] 修正 max_tokens 参数，避免设置过大 ---
                # 使用structured output schema
                schema = {
                    "type": "object",
                    "properties": {
                        "dialogues": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "start_ms": {"type": "integer"},
                                    "end_ms": {"type": "integer"},
                                    "speaker_id": {"type": "string"},
                                    "line": {"type": "string"}
                                },
                                "required": ["start_ms", "end_ms", "speaker_id", "line"]
                            }
                        }
                    },
                    "required": ["dialogues"]
                }
                
                result = self.client.generate_content(
                    model=model_name,
                    prompt=user_prompt,
                    video_uri=video_uri,
                    max_tokens=65535,  # 使用最大token限制确保完整输出
                    temperature=0.0,   # 保持最低温度确保精准度和一致性
                    system_instruction=system_instruction,
                    schema=schema
                )
                # 解析structured output
                dialogues = result.get("dialogues", []) if isinstance(result, dict) else []
                if dialogues and len(dialogues) > 0:
                    print(f"✅ 第{attempt+1}次调用成功，识别到 {len(dialogues)} 条对话")
                    # 调试：检查第一个对话的字段
                    if dialogues:
                        first_dialogue = dialogues[0]
                        print(f"🔍 第一个对话字段: {list(first_dialogue.keys())}")
                        print(f"🔍 第一个对话内容: {first_dialogue}")
                    break
                else:
                    print(f"⚠️ 第{attempt+1}次调用成功但解析失败，model={model_name}")
                    print(f"返回结果: {result}")
                    if attempt < max_retries + 1:  # 允许更多重试
                        print(f"🔄 重试中...")
            except Exception as e:
                error_type = type(e).__name__
                print(f"❌ 第{attempt+1}次调用异常 ({error_type}): {e}")
                if attempt < max_retries + 1:  # 允许更多重试
                    print(f"🔄 重试中...")
                else:
                    print(f"❌ {episode_id} Step0.1 最终失败: {error_type} - {e}")
                    result = {"text": ""}

        # 最终解析结果（使用structured output）
        raw_dialogues = result.get("dialogues", []) if isinstance(result, dict) else []

        # 确保后处理逻辑被执行
        processed_dialogues = self._post_process_dialogues(raw_dialogues)

        # 保存JSON作为"真相来源"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(processed_dialogues, f, ensure_ascii=False, indent=2)
        print(f"✅ 已保存详细JSON输出: {json_file}")

        # 从JSON生成标准的SRT文件用于调试
        srt_content = self._render_to_srt(processed_dialogues)
        self.utils.save_text_file(srt_file, srt_content)
        print(f"✅ 已生成SRT调试文件: {srt_file}")

        processing_time = time.time() - start_time
        print(f"✅ {episode_id} Step0.1 完成 (用时: {processing_time:.2f}秒)")
        return {
            "status": "success", 
            "dialogues_count": len(processed_dialogues),
            "processing_time": round(processing_time, 2),
            "model_used": model_name
        }

    def _run_all_episodes(self) -> Dict[str, Any]:
        episodes = self.utils.get_episode_list(self.config.project_root)
        results = []
        print(f"开始Step0.1: 处理 {len(episodes)} 个剧集...")
        
        # 获取并行配置，支持动态调整
        max_workers = getattr(self.config, 'max_workers', 8)  # 默认8个线程，避免API限制
        print(f"使用 {max_workers} 个并行线程处理...")
        
        # 使用并行处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_episode = {
                executor.submit(self._run_single_episode, ep): ep 
                for ep in episodes
            }
            
            # 收集结果
            for future in tqdm(as_completed(future_to_episode), total=len(episodes), desc="Step0.1"):
                episode_id = future_to_episode[future]
                try:
                    result = future.result()
                    result["episode_id"] = episode_id
                    results.append(result)
                except Exception as e:
                    print(f"❌ Step0.1 处理 {episode_id} 失败: {e}")
                    results.append({
                        "episode_id": episode_id, 
                        "status": "failed", 
                        "error": str(e)
                    })
        
        # 生成统计报告
        stats = self._generate_statistics(results)
        print(f"\n📊 Step0.1 处理完成统计:")
        print(f"   总剧集数: {stats['total_episodes']}")
        print(f"   成功处理: {stats['success_count']}")
        print(f"   失败处理: {stats['failed_count']}")
        print(f"   总对话数: {stats['total_dialogues']}")
        print(f"   平均对话数: {stats['avg_dialogues']:.1f}")
        print(f"   成功率: {stats['success_rate']:.1f}%")
        
        return {
            "status": "completed", 
            "results": results,
            "statistics": stats
        }

    def _generate_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """生成处理统计信息"""
        total_episodes = len(results)
        # 将 already_exists 视为成功，避免重试后成功被误判为失败或未完成
        success_results = [r for r in results if r.get("status") == "success"]
        already_exists_results = [r for r in results if r.get("status") == "already_exists"]
        completed_results = success_results + already_exists_results
        failed_results = [r for r in results if r.get("status") == "failed"]
        
        success_count = len(completed_results)
        already_exists_count = len(already_exists_results)
        failed_count = len(failed_results)
        success_rate = (success_count / total_episodes * 100) if total_episodes > 0 else 0
        
        total_dialogues = sum(r.get("dialogues_count", 0) for r in success_results)
        avg_dialogues = (total_dialogues / success_count) if success_count > 0 else 0
        
        # 按剧集统计对话数
        episode_dialogues = {
            r.get("episode_id", "unknown"): r.get("dialogues_count", 0) 
            for r in success_results
        }
        
        return {
            "total_episodes": total_episodes,
            "success_count": success_count,
            "already_exists_count": already_exists_count,
            "failed_count": failed_count,
            "success_rate": success_rate,
            "total_dialogues": total_dialogues,
            "avg_dialogues": avg_dialogues,
            "episode_dialogues": episode_dialogues,
            "failed_episodes": [r.get("episode_id", "unknown") for r in failed_results]
        }

    def _post_process_dialogues(self, dialogues: List[Dict]) -> List[Dict]:
        """对模型返回的对话进行排序和清理，确保数据质量。"""
        if not dialogues:
            return []
        
        # 1. 按开始时间排序，这是关键一步
        dialogues.sort(key=lambda x: x.get('start_ms', 0))

        # 2. 清理每个条目的数据
        for d in dialogues:
            if isinstance(d.get('line'), str):
                d['line'] = d['line'].strip()
            if not isinstance(d.get('speaker_id'), str):
                d['speaker_id'] = "UNKNOWN"
            else:
                d['speaker_id'] = d['speaker_id'].strip()

        return dialogues
        
    def _render_to_srt(self, dialogues: List[Dict]) -> str:
        """将处理后的对话列表渲染为标准的SRT字幕格式。"""
        def _fmt(ms:int) -> str:
            s, ms_rem = divmod(int(ms), 1000)
            h, s_rem = divmod(s, 3600)
            m, s_final = divmod(s_rem, 60)
            return f"{h:02d}:{m:02d}:{s_final:02d},{ms_rem:03d}"
        
        lines = []
        for i, d in enumerate(dialogues, 1):
            start_time = _fmt(d.get('start_ms', 0))
            end_time = _fmt(d.get('end_ms', d.get('start_ms', 0)))
            speaker_id = d.get('speaker_id', 'UNKNOWN')
            text = d.get('line', '')
            
            lines.append(str(i))
            lines.append(f"{start_time} --> {end_time}")
            # 在字幕中也标注speaker_id，便于核对
            lines.append(f"[{speaker_id}] {text}")
            lines.append("")
        
        return "\n".join(lines)

    def _parse_dialogue_text(self, text: str) -> List[Dict]:
        """
        解析文本格式的对话输出，转换为标准格式
        输入格式：分钟:秒.毫秒-分钟:秒.毫秒[speaker_id]对话内容
        """
        import re
        
        if not text:
            return []
            
        dialogues = []
        lines = text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            # 更宽松的匹配格式：支持多种时间格式
            # 匹配：分钟:秒.毫秒-分钟:秒.毫秒[speaker_id]对话内容
            patterns = [
                r'(\d+):(\d+)\.(\d+)-(\d+):(\d+)\.(\d+)\[([^\]]+)\]\s*(.*)',  # 标准格式
                r'(\d+):(\d+)\.(\d+)-(\d+):(\d+)\.(\d+)\[([^\]]+)\]\s*(.*)',  # 兼容格式
            ]
            
            match = None
            for pattern in patterns:
                match = re.match(pattern, line)
                if match:
                    break
            
            if match:
                start_min, start_sec, start_ms, end_min, end_sec, end_ms, speaker_id, line_text = match.groups()
                
                try:
                    # 转换为毫秒
                    start_total_ms = int(start_min) * 60 * 1000 + int(start_sec) * 1000 + int(start_ms)
                    end_total_ms = int(end_min) * 60 * 1000 + int(end_sec) * 1000 + int(end_ms)
                    
                    dialogues.append({
                        'start_ms': start_total_ms,
                        'end_ms': end_total_ms,
                        'speaker_id': speaker_id,
                        'line': line_text.strip()
                    })
                except ValueError:
                    # 时间戳解析失败，跳过这一行
                    continue
        
        return dialogues


