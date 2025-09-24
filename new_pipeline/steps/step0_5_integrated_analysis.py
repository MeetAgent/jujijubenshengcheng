"""
Step0.5: 融合对话分析
将Step0.4的纠错能力与深层语义分析融合，一步到位完成：
1. 法证级纠错 (Forensic Correction)
2. 对话轮次重构 (Turn Reconstruction) 
3. 深层语义分析 (Semantic Analysis)

输出：
- 0_5_dialogue_turns.json (结构化对话轮次)
- 0_5_analysis_summary.md (分析摘要)
"""

import os
import json
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pydantic import BaseModel

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import PipelineConfig, GenAIClient, PipelineUtils
from . import PipelineStep

from new_pipeline.steps.commont_log import log


class DialogueTurn(BaseModel):
    """对话轮次模型"""
    turn_id: int
    speaker: str
    full_dialogue: str
    emotion: str
    intent: str
    original_indices: List[int]
    confidence: Optional[float] = None
    correction_notes: Optional[str] = None


class DialogueTurnsResponse(BaseModel):
    """对话轮次响应模型"""
    dialogue_turns: List[DialogueTurn]
    metadata: Optional[Dict[str, Any]] = None


class Step0_5IntegratedAnalysis(PipelineStep):
    """Step0.5: 融合对话分析"""
    
    @property
    def step_number(self) -> int:
        return 5

    @property
    def step_name(self) -> str:
        return "integrated_analysis"

    def check_dependencies(self, episode_id: str = None) -> bool:
        """检查依赖：需要Step0.4的输出"""
        if episode_id:
            # 检查Step0.4的输出文件
            calibrated_file = f"{self.config.project_root}/{episode_id}/0_4_calibrated_dialogue.txt"
            return os.path.exists(calibrated_file)
        return True

    def get_output_files(self, episode_id: str = None) -> List[str]:
        if episode_id:
            return [
                f"{episode_id}/0_5_dialogue_turns.json",
                f"{episode_id}/0_5_analysis_summary.md"
            ]
        return []

    def run(self, episode_id: str = None) -> Dict[str, Any]:
        if episode_id:
            return self._run_single_episode(episode_id)
        else:
            return self._run_all_episodes()

    def _run_single_episode(self, episode_id: str) -> Dict[str, Any]:
        """处理单个剧集"""
        import time
        start_time = time.time()
        
        # 输出路径
        episode_out_dir = self.utils.get_episode_output_dir(self.config.output_dir, episode_id)
        turns_file = os.path.join(episode_out_dir, "0_5_dialogue_turns.json")
        summary_file = os.path.join(episode_out_dir, "0_5_analysis_summary.md")

        if os.path.exists(turns_file) and not os.environ.get("FORCE_OVERWRITE"):
            return {"status": "already_exists"}

        # 检查依赖
        if not self.check_dependencies(episode_id):
            return {"status": "failed", "error": "Missing Step0.4 output"}

        # 加载输入数据
        try:
            # 1. 加载Step0.4的校准对话
            calibrated_file = f"{self.config.project_root}/{episode_id}/0_4_calibrated_dialogue.txt"
            calibrated_content = self.utils.load_text_file(calibrated_file, "")
            
            if not calibrated_content.strip():
                return {"status": "failed", "error": "Empty calibrated content"}
            
            # 2. 加载Step0.3的角色信息
            character_summary = self._build_character_summary(episode_id)
            
            # 3. 获取视频URI
            video_uri = self._get_video_uri(episode_id)
            
        except Exception as e:
            return {"status": "failed", "error": f"Failed to load input data: {str(e)}"}

        # 执行融合分析
        try:
            result = self._integrated_processing(
                calibrated_content, character_summary, video_uri, episode_id
            )
            
            if not result or not result.get('dialogue_turns'):
                # 尝试回退处理
                result = self._fallback_processing(calibrated_content, character_summary, episode_id)
                
                if not result or not result.get('dialogue_turns'):
                    return {"status": "failed", "error": "Both integrated and fallback processing failed"}
            
            # 保存结果
            self._save_results(result, turns_file, summary_file, episode_id)
            
            processing_time = time.time() - start_time
            return {
                "status": "success", 
                "turns_count": len(result.get('dialogue_turns', [])),
                "correction_count": result.get('metadata', {}).get('correction_count', 0),
                "reconstruction_count": result.get('metadata', {}).get('reconstruction_count', 0),
                "processing_time": round(processing_time, 2),
                "processing_mode": result.get('metadata', {}).get('processing_mode', 'unknown')
            }
            
        except Exception as e:
            return {"status": "failed", "error": f"Processing failed: {str(e)}"}

    def _integrated_processing(self, calibrated_content: str, character_summary: str, 
                             video_uri: str, episode_id: str) -> Dict[str, Any]:
        """融合处理：纠错+重构+分析"""
        
        # 构建融合prompt
        system_instruction = self._build_system_instruction()
        user_prompt = self._build_user_prompt(calibrated_content, character_summary)
        
        # 模型配置
        model_name = 'gemini-2.5-pro'  # 使用最强模型处理复杂任务
        
        # 输出schema
        schema = {
            "type": "object",
            "properties": {
                "dialogue_turns": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "turn_id": {"type": "integer"},
                            "speaker": {"type": "string"},
                            "full_dialogue": {"type": "string"},
                            "emotion": {"type": "string"},
                            "intent": {"type": "string"},
                            "original_indices": {
                                "type": "array",
                                "items": {"type": "integer"}
                            },
                            "confidence": {"type": "number"},
                            "correction_notes": {"type": "string"}
                        },
                        "required": ["turn_id", "speaker", "full_dialogue", "emotion", "intent", "original_indices"]
                    }
                },
                "metadata": {
                    "type": "object",
                    "properties": {
                        "total_turns": {"type": "integer"},
                        "correction_count": {"type": "integer"},
                        "reconstruction_count": {"type": "integer"},
                        "processing_mode": {"type": "string"}
                    }
                }
            },
            "required": ["dialogue_turns"]
        }
        
        # 调用模型
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    log.info(f"🔄 第{attempt+1}次重试融合分析...")
                
                result = self.client.generate_content(
                    model=model_name,
                    prompt=user_prompt,
                    video_uri=video_uri,
                    max_tokens=65535,
                    temperature=0.1,  # 稍微提高温度以增加创造性
                    system_instruction=system_instruction,
                    schema=schema
                )

                # debug: 打印完整响应内容
                log.debug(f"融合分析响应: {result}")
                
                # 验证输出
                if self._validate_output(result):
                    turns_count = len(result.get('dialogue_turns', []))
                    correction_count = result.get('metadata', {}).get('correction_count', 0)
                    reconstruction_count = result.get('metadata', {}).get('reconstruction_count', 0)
                    
                    log.info(f"✅ 融合分析成功，生成 {turns_count} 个对话轮次，"
                          f"纠错 {correction_count} 次，重构 {reconstruction_count} 次")
                    return result
                else:
                    log.info(f"⚠️ 第{attempt+1}次调用输出验证失败")
                    
            except Exception as e:
                log.info(f"❌ 第{attempt+1}次融合分析失败: {e}")
                if attempt == max_retries - 1:
                    raise e
        
        return None

    def _build_system_instruction(self) -> str:
        """构建系统指令"""
        return """你是一位顶级的"首席剧本分析师 (Chief Script Analyst)"。你的任务极其关键：将一份破碎且可能存在严重错误的"对话日志"，一步到位地转化为一份结构化的、包含深层语义的、可用于剧本创作的"对话单元"。

【核心挑战】
你收到的"对话草稿"存在两大缺陷：
1. 归属错误：由于镜头切换，台词可能被错误地分配给了当时在画面中的角色。
2. 语义破碎：一句完整的话被字幕格式切割成了多个不连贯的片段。

【你的工作流程：一个严格的思维链】
对于输入的每一段对话，你必须遵循以下三步思考流程：

第一步：法证级纠错 (Forensic Correction)
- 真相来源：视频是唯一的、最高的真相来源。
- 任务：像侦探一样，结合视频画面（口型、人物状态）和音频（声音特征），判断"草稿"中标记的说话人是否正确。
- 行动：如果草稿是错的，你必须基于视频证据，无条件地修正它。

第二步：对话轮次重构 (Turn Reconstruction)
- 任务：在你完成了逐句的内心纠错后，审视连续的台词。如果多句台词在语义、逻辑和情感上构成了一个由同一个人说出的、完整的思想单元，你就必须将它们合并成一句 full_dialogue。
- 关键：合并的依据是你纠错后的说话人，而不是草稿上的。

第三步：深层语义分析 (Semantic Analysis)
- 任务：只有当你形成了一个完整的 full_dialogue 之后，你才能对它进行分析。
- 分析维度：
  * emotion: 说话人表达这段完整对话时，最核心的情绪是什么？（例如：轻蔑、担忧、愤怒、试探）
  * intent: 说话人说这段话的最终目的或战术意图是什么？（例如：试图操纵对方、表达真诚的歉意、拖延时间、获取信息）

【强规则】
- 顺序至上：严格遵循"先纠错 -> 再重构 -> 最后分析"的思维链。
- 客观分析：emotion 和 intent 的分析必须基于台词内容和角色的已知信息，避免过度脑补。
- 完整输出：你必须处理输入草稿中的每一句台词，不能遗漏。合并后的 turn 应记录它包含了哪些原始行号。
- 输出格式：绝对禁止任何解释性文字。你的唯一输出必须是严格符合要求的JSON对象。"""

    def _build_user_prompt(self, calibrated_content: str, character_summary: str) -> str:
        """构建用户指令"""
        return f"""【本集人物参考】
{character_summary}

【待处理的对话草稿】
{calibrated_content}

【工作流程演示与要求】

请严格遵循你在系统指令中学到的"三步思维链"，处理上述"对话草稿"。下面是一个具体案例，用于演示你的思考过程：

示例输入草稿片段：
```
14
00:01:20,100 --> 00:01:21,500
[陆锦行] 你听我说，这件事

15
00:01:21,600 --> 00:01:22,800
[顾向晴] 真的不是你想的那样，我只是

16
00:01:22,900 --> 00:01:24,100
[陆锦行] 想保护你而已。
```

你的内心思考过程应该是这样的：
1. 【纠错】：我看到第 15 行，草稿标记是 [顾向晴]，但我观看 00:01:21 处的视频，发现当时说话的人嘴型和声音都是陆锦行。草稿错了，我必须将其修正为 [陆锦行]。
2. 【重构】：现在，14、15、16 三行的说话人都被我确定为 [陆锦行]。我将这三句台词连起来读："你听我说，这件事真的不是你想的那样，我只是想保护你而已。" 这在语义和情感上是完全连贯的一句话。因此，我必须将它们合并成一个对话回合 (Turn)。
3. 【分析】：对于这个合并后的完整对话 "你听我说...保护你而已。"，我分析出：说话人 emotion 是"急切、辩解"，他的 intent 是"试图澄清误会，并表达自己的保护姿态"。

最终，基于以上思考，你将输出如下JSON条目：
```json
{{
  "turn_id": 5,
  "speaker": "陆锦行",
  "full_dialogue": "你听我说，这件事真的不是你想的那样，我只是想保护你而已。",
  "emotion": "急切、辩解",
  "intent": "试图澄清误会，并表达自己的保护姿态",
  "original_indices": [14, 15, 16],
  "confidence": 0.95,
  "correction_notes": "基于视频证据确认说话人身份"
}}
```

现在，请开始处理【待处理的对话草稿】的全部内容，并严格按照以下JSON结构输出你的最终结果：

```json
{{
  "dialogue_turns": [
    {{
      "turn_id": 1,
      "speaker": "string (角色的规范名)",
      "full_dialogue": "string (合并后的完整台词)",
      "emotion": "string (对情感的精准概括)",
      "intent": "string (对说话人意图的深刻洞察)",
      "original_indices": [integer],
      "confidence": number,
      "correction_notes": "string"
    }}
  ],
  "metadata": {{
    "total_turns": integer,
    "correction_count": integer,
    "reconstruction_count": integer,
    "processing_mode": "integrated"
  }}
}}
```"""

    def _fallback_processing(self, calibrated_content: str, character_summary: str, 
                           episode_id: str) -> Dict[str, Any]:
        """回退处理：分步处理模式"""
        log.info(f"🔄 {episode_id} 使用回退处理模式")
        
        # 简化的分步处理
        # 这里可以调用现有的Step0.4逻辑，然后添加简单的分析
        try:
            # 解析校准后的对话
            dialogue_segments = self._parse_calibrated_dialogue(calibrated_content)
            
            # 简单的重构和分析
            turns = []
            for i, segment in enumerate(dialogue_segments):
                turn = {
                    "turn_id": i + 1,
                    "speaker": segment.get('speaker', 'UNKNOWN'),
                    "full_dialogue": segment.get('text', ''),
                    "emotion": "中性",  # 简化处理
                    "intent": "信息传达",  # 简化处理
                    "original_indices": [i + 1],
                    "confidence": 0.8,
                    "correction_notes": "回退模式处理"
                }
                turns.append(turn)
            
            return {
                "dialogue_turns": turns,
                "metadata": {
                    "total_turns": len(turns),
                    "correction_count": 0,
                    "reconstruction_count": 0,
                    "processing_mode": "fallback"
                }
            }
            
        except Exception as e:
            log.info(f"❌ 回退处理也失败: {e}")
            return {
                "dialogue_turns": [],
                "metadata": {
                    "total_turns": 0,
                    "correction_count": 0,
                    "reconstruction_count": 0,
                    "processing_mode": "failed"
                }
            }

    def _parse_calibrated_dialogue(self, content: str) -> List[Dict[str, Any]]:
        """解析校准后的对话内容"""
        segments = []
        lines = content.strip().split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.isdigit():  # 序号行
                # 时间戳行
                if i + 1 < len(lines):
                    timestamp = lines[i + 1].strip()
                    # 对话行
                    if i + 2 < len(lines):
                        dialogue = lines[i + 2].strip()
                        # 提取说话人和内容
                        if dialogue.startswith('[') and ']' in dialogue:
                            end_bracket = dialogue.find(']')
                            speaker = dialogue[1:end_bracket]
                            text = dialogue[end_bracket + 1:].strip()
                            
                            segments.append({
                                'speaker': speaker,
                                'text': text,
                                'timestamp': timestamp
                            })
                i += 4  # 跳过序号、时间戳、对话、空行
            else:
                i += 1
        
        return segments

    def _validate_output(self, result: Dict[str, Any]) -> bool:
        """验证输出质量"""
        try:
            if not result or not isinstance(result, dict):
                return False
            
            if 'dialogue_turns' not in result:
                return False
            
            turns = result['dialogue_turns']
            if not isinstance(turns, list) or len(turns) == 0:
                return False
            
            # 检查每个turn的必要字段
            for turn in turns:
                required_fields = ['turn_id', 'speaker', 'full_dialogue', 'emotion', 'intent', 'original_indices']
                if not all(field in turn for field in required_fields):
                    return False
                
                # 检查数据类型
                if not isinstance(turn['turn_id'], int):
                    return False
                if not isinstance(turn['original_indices'], list):
                    return False
            
            return True
            
        except Exception as e:
            log.info(f"⚠️ 输出验证失败: {e}")
            return False

    def _build_character_summary(self, episode_id: str) -> str:
        """构建角色信息摘要"""
        try:
            # 优先从当集文件加载
            hints_file = f"{self.config.project_root}/{episode_id}/0_3_speaker_hints.csv"
            if os.path.exists(hints_file):
                return self._load_character_summary_from_csv(hints_file)
            
            # 回退到全局文件
            global_file = f"{self.config.project_root}/global_character_graph_llm.json"
            if os.path.exists(global_file):
                return self._load_character_summary_from_global(global_file, episode_id)
            
            return "无角色信息"
            
        except Exception as e:
            log.info(f"⚠️ 构建角色摘要失败: {e}")
            return "无角色信息"

    def _load_character_summary_from_csv(self, csv_file: str) -> str:
        """从CSV文件加载角色摘要"""
        try:
            import csv
            characters = []
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    candidate = row.get('candidate', '').strip()
                    evidence = row.get('evidence', '').strip()
                    role_type = row.get('role_type', '').strip()
                    
                    if candidate and candidate != 'UNKNOWN':
                        char_info = f"- {candidate} ({role_type}): {evidence}"
                        characters.append(char_info)
            
            return "\n".join(characters) if characters else "无角色信息"
            
        except Exception as e:
            log.info(f"⚠️ 从CSV加载角色摘要失败: {e}")
            return "无角色信息"

    def _load_character_summary_from_global(self, global_file: str, episode_id: str) -> str:
        """从全局文件加载角色摘要"""
        try:
            with open(global_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 查找当集的角色信息
            per_episode = data.get('per_episode', [])
            for ep in per_episode:
                if ep.get('episode_id') == episode_id:
                    protagonists = ep.get('protagonists', [])
                    supporting = ep.get('supporting', [])
                    
                    summary_parts = []
                    for char in protagonists + supporting:
                        name = char.get('canonical_name', '')
                        traits = char.get('global_traits', '')
                        if name:
                            summary_parts.append(f"- {name}: {traits}")
                    
                    return "\n".join(summary_parts) if summary_parts else "无角色信息"
            
            return "无角色信息"
            
        except Exception as e:
            log.info(f"⚠️ 从全局文件加载角色摘要失败: {e}")
            return "无角色信息"

    def _get_video_uri(self, episode_id: str) -> str:
        """获取视频URI"""
        try:
            # 尝试多个路径
            paths = [
                f"{self.config.project_root}/{episode_id}/gcs_path.txt",
                f"{self.config.project_root}/{episode_id}/0_gcs_path.txt",
                f"{self.config.output_dir}/{episode_id}/0_gcs_path.txt"
            ]
            
            for path in paths:
                if os.path.exists(path):
                    return self.utils.load_text_file(path, "").strip()
            
            return self.utils.get_video_uri(episode_id, self.config.project_root)
            
        except Exception as e:
            log.info(f"⚠️ 获取视频URI失败: {e}")
            return ""

    def _save_results(self, result: Dict[str, Any], turns_file: str, 
                     summary_file: str, episode_id: str):
        """保存结果"""
        try:
            # 保存JSON结果
            with open(turns_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            
            # 生成分析摘要
            summary = self._generate_analysis_summary(result, episode_id)
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(summary)
                
        except Exception as e:
            log.info(f"⚠️ 保存结果失败: {e}")

    def _generate_analysis_summary(self, result: Dict[str, Any], episode_id: str) -> str:
        """生成分析摘要"""
        turns = result.get('dialogue_turns', [])
        metadata = result.get('metadata', {})
        
        summary = f"""# {episode_id} 对话分析摘要

## 处理统计
- **总对话轮次**: {metadata.get('total_turns', len(turns))}
- **纠错次数**: {metadata.get('correction_count', 0)}
- **重构次数**: {metadata.get('reconstruction_count', 0)}
- **处理模式**: {metadata.get('processing_mode', 'unknown')}

## 角色对话统计
"""
        
        # 统计每个角色的对话次数
        speaker_stats = {}
        for turn in turns:
            speaker = turn.get('speaker', 'UNKNOWN')
            speaker_stats[speaker] = speaker_stats.get(speaker, 0) + 1
        
        for speaker, count in sorted(speaker_stats.items()):
            summary += f"- **{speaker}**: {count} 轮对话\n"
        
        summary += "\n## 情感分析统计\n"
        
        # 统计情感分布
        emotion_stats = {}
        for turn in turns:
            emotion = turn.get('emotion', '未知')
            emotion_stats[emotion] = emotion_stats.get(emotion, 0) + 1
        
        for emotion, count in sorted(emotion_stats.items()):
            summary += f"- **{emotion}**: {count} 次\n"
        
        summary += "\n## 对话轮次详情\n"
        
        # 显示全部对话轮次
        for i, turn in enumerate(turns):
            summary += f"""
### 轮次 {turn.get('turn_id', i+1)}
- **说话人**: {turn.get('speaker', 'UNKNOWN')}
- **情感**: {turn.get('emotion', '未知')}
- **意图**: {turn.get('intent', '未知')}
- **对话内容**: {turn.get('full_dialogue', '')}
- **原始行号**: {turn.get('original_indices', [])}
"""
        
        if len(turns) > 5:
            summary += f"\n... 还有 {len(turns) - 5} 个对话轮次\n"
        
        return summary

    def _run_all_episodes(self) -> Dict[str, Any]:
        """处理所有剧集"""
        episodes = self.utils.get_episode_list(self.config.project_root)
        results = []
        log.info(f"开始Step0.5: 处理 {len(episodes)} 个剧集...")
        
        # 获取并行配置：步骤级 -> 全局 -> 默认 3
        step_conf = self.config.get_step_config(5) or {}
        max_workers = step_conf.get('max_workers')
        if max_workers is None:
            conc_conf = getattr(self.config, 'concurrency', {}) or {}
            max_workers = conc_conf.get('max_workers', 3)
        try:
            max_workers = int(max_workers)
        except Exception:
            max_workers = 3
        if max_workers < 1:
            max_workers = 1
        log.info(f"使用 {max_workers} 个并行线程处理...")
        
        # 使用并行处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_episode = {
                executor.submit(self._run_single_episode, ep): ep 
                for ep in episodes
            }
            
            # 收集结果
            for future in tqdm(as_completed(future_to_episode), total=len(episodes), desc="Step0.5"):
                episode_id = future_to_episode[future]
                try:
                    result = future.result()
                    result["episode_id"] = episode_id
                    results.append(result)
                    
                    # 实时显示处理结果
                    if result.get("status") == "success":
                        log.info(f"✅ {episode_id}: {result.get('turns_count', 0)} 轮对话, "
                              f"纠错 {result.get('correction_count', 0)} 次, "
                              f"重构 {result.get('reconstruction_count', 0)} 次")
                    elif result.get("status") == "already_exists":
                        log.info(f"⏭️ {episode_id}: 已存在，跳过处理")
                    else:
                        log.info(f"❌ {episode_id}: 处理失败")
                        
                except Exception as e:
                    log.info(f"❌ Step0.5 处理 {episode_id} 失败: {e}")
                    results.append({
                        "episode_id": episode_id, 
                        "status": "failed", 
                        "error": str(e)
                    })
        
        # 生成统计报告
        stats = self._generate_statistics(results)
        log.info("\n📊 Step0.5 处理完成统计:")
        log.info(f"   总剧集数: {stats['total_episodes']}")
        log.info(f"   成功处理: {stats['success_count']}")
        log.info(f"   失败处理: {stats['failed_count']}")
        log.info(f"   成功率: {stats['success_rate']:.1f}%")
        log.info(f"   总对话轮次: {stats['total_turns']}")
        log.info(f"   平均对话轮次: {stats['avg_turns']:.1f}")
        log.info(f"   总纠错次数: {stats['total_corrections']}")
        log.info(f"   总重构次数: {stats['total_reconstructions']}")
        log.info(f"   总处理时间: {stats['total_processing_time']} 秒")
        log.info(f"   平均处理时间: {stats['avg_processing_time']} 秒/episode")
        log.info(f"   并行线程数: {max_workers}")
        
        return {
            "status": "completed", 
            "results": results,
            "statistics": stats
        }

    def _generate_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """生成处理统计信息"""
        total_episodes = len(results)
        success_results = [r for r in results if r.get("status") == "success"]
        failed_results = [r for r in results if r.get("status") == "failed"]
        
        success_count = len(success_results)
        failed_count = len(failed_results)
        success_rate = (success_count / total_episodes * 100) if total_episodes > 0 else 0
        
        total_turns = sum(r.get("turns_count", 0) for r in success_results)
        avg_turns = (total_turns / success_count) if success_count > 0 else 0
        
        # 处理时间统计
        processing_times = [r.get("processing_time", 0) for r in success_results if r.get("processing_time")]
        total_time = sum(processing_times)
        avg_time = (total_time / len(processing_times)) if processing_times else 0
        
        # 纠错和重构统计
        total_corrections = sum(r.get("correction_count", 0) for r in success_results)
        total_reconstructions = sum(r.get("reconstruction_count", 0) for r in success_results)
        
        return {
            "total_episodes": total_episodes,
            "success_count": success_count,
            "failed_count": failed_count,
            "success_rate": success_rate,
            "total_turns": total_turns,
            "avg_turns": avg_turns,
            "total_processing_time": round(total_time, 2),
            "avg_processing_time": round(avg_time, 2),
            "total_corrections": total_corrections,
            "total_reconstructions": total_reconstructions,
            "failed_episodes": [r.get("episode_id", "unknown") for r in failed_results]
        }
