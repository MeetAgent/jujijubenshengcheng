"""
Step0.2: 分集角色与关系线索提取
从每一集的视频和对话文本中提取角色线索和关系信息
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import PipelineStep
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Dict, List, Any
import json


class Step0_2ClueExtraction(PipelineStep):
    """分集角色与关系线索提取步骤"""
    
    def __init__(self, config):
        super().__init__(config)
        self._step_name = "Step0.2"
        self._description = "分集角色与关系线索提取"
        # 先验文件路径（串行时用于跨集传递）
        self.prior_file = os.path.join(self.config.project_root, "0_2_prior.json")
    
    @property
    def step_number(self) -> int:
        """步骤编号"""
        return 0.2
    
    @property
    def step_name(self) -> str:
        """步骤名称"""
        return self._step_name
    
    def run(self) -> Dict[str, Any]:
        """运行线索提取"""
        return self._run_all_episodes()
    
    def _run_all_episodes(self) -> Dict[str, Any]:
        """处理所有剧集（串行 + 先验注入/合并）"""
        episodes = self.utils.get_episode_list(self.config.project_root)
        episodes = sorted(episodes)
        results = []
        print(f"开始{self.step_name}: 串行处理 {len(episodes)} 个剧集...")

        # 确保先验文件存在
        prior = self._load_prior()

        for episode_id in tqdm(episodes, total=len(episodes), desc=self.step_name):
            try:
                result = self._run_single_episode(episode_id, prior)
                result["episode_id"] = episode_id
                results.append(result)
                # 将本集信息合并入先验并保存
                self._merge_prior(prior, episode_id)
                self._save_prior(prior)
            except Exception as e:
                print(f"❌ {self.step_name} 处理 {episode_id} 失败: {e}")
                results.append({
                    "episode_id": episode_id, 
                    "status": "failed", 
                    "error": str(e)
                })

        stats = self._generate_statistics(results)
        print(f"\n📊 {self.step_name} 处理完成统计:")
        print(f"   总剧集数: {stats['total_episodes']}")
        print(f"   成功处理: {stats['success_count']}")
        print(f"   失败处理: {stats['failed_count']}")
        print(f"   成功率: {stats['success_rate']:.1f}%")

        return {
            "status": "completed", 
            "results": results,
            "statistics": stats
        }
    
    def _run_single_episode(self, episode_id: str, prior: Dict[str, Any] = None) -> Dict[str, Any]:
        """处理单个剧集（在构建prompt时注入先验）"""
        print(f"{self.step_name}: 处理 {episode_id}")
        
        # 检查是否已有输出文件
        output_file = f"{self.config.project_root}/{episode_id}/0_2_clues.json"
        if os.path.exists(output_file) and not os.getenv('FORCE_OVERWRITE'):
            print(f"✅ {episode_id} 已有{self.step_name}输出文件，跳过处理")
            return {"status": "already_exists"}
        
        # 检查输入文件（优先SRT，兼容旧版TXT）
        asr_srt = f"{self.config.project_root}/{episode_id}/0_1_timed_dialogue.srt"
        asr_txt = f"{self.config.project_root}/{episode_id}/0_1_timed_dialogue.txt"
        asr_file = asr_srt if os.path.exists(asr_srt) else (asr_txt if os.path.exists(asr_txt) else None)
        if not asr_file:
            print(f"❌ {episode_id} 缺少ASR输入文件: {asr_srt} 或 {asr_txt}")
            return {"status": "failed", "error": "缺少ASR输入文件"}
        
        # 获取视频URI
        video_uri = self.utils.get_video_uri(episode_id, self.config.project_root)
        
        # 读取ASR对话内容
        with open(asr_file, 'r', encoding='utf-8') as f:
            asr_content = f.read()
        
        # 使用模型配置
        model_config = self.config.get_model_config_by_name("step0_2")  # 使用step0_2配置
        
        # 思考预算配置（0表示跳过思考，提升速度）
        thinking_budget = model_config.get("thinking_budget", 0)
        print(f"🧠 思考预算配置: {thinking_budget} (0=跳过思考，提升速度)")
        
        # 构建prompt
        system_instruction = """你是影视剧分析的AI助手，正在执行大规模分析任务的第一步。

你的唯一任务：只记录你看到和听到的“原始线索”，避免进行任何推断或下结论。严格按JSON结构输出，不要添加解释。

核心任务：
1. 显式角色识别 (Explicit Characters) — 仅基于可核验命名信号
   - 仅收录两类来源：
     a) onscreen_name（人名条/角色卡）：必须输出 name、title_or_role、appearance_method=onscreen_name、evidence_modality=onscreen_card、evidence_quote（卡面文字）
     b) explicit_dialogue（明确全名呼唤，非称谓/非从属）：appearance_method=explicit_dialogue，附 evidence_modality=subtitle|voice 与 evidence_quote
   - 禁止将以下来源落地为显式角色：visual_inference / prior / honorific（小姐/总/夫人/医生…）/“X的助理/司机/保镖/下属/随从”等功能/从属称谓
   - 守门规则：若 name 匹配“.+的(助理|司机|保镖|下属|随从)$”，或仅凭视觉/称谓/同框得到的命名，一律不要进入 explicit_characters；改在 observed_interactions 或 speaker_profiles 记录证据

2. 匿名说话人档案 (Speaker Profiles)
   - 按 spk_X 建档：visual（外观）、voice（音色/声纹）、speaking_style（可选）、key_lines（1–2句代表台词）
   - honorific 可记录在 key_lines 中，但不得用于改写姓名或生成从属命名

3. 观察到的互动 (Observed Interactions) — 只记录证据，不做推断
   - participants: [发言者spk/角色, 对象（如有）]
   - summary: 客观描述（例如：spk_2 对 spk_1 说话（address）；spk_3 提及“陆总”（mention））
   - evidence: 关键台词/卡面文本
   - action_type: address | mention（区分正在对谁说 vs 提及第三人）
   - addressee: 若 address，写规范名/spk_ID/second_person；否则留空
   - referents: 若 mention，写被提及对象数组；否则为空
   - speech_act: report | command | question | inform
   - observation_type: vocal | subtitle | onscreen_card | visual_context
   - certainty_level: high | medium | low（短确认/礼貌性归 low）
   - start_ms/end_ms、evidence_span

命名与合并规则（强一致）：
- 命名优先级：人名条/卡（onscreen_name） > 明确全名呼唤（explicit_dialogue），禁止臆造中文全名
- honorific/称谓与视觉/先验仅作为证据，不得改名或生成“X的助理/司机/保镖/下属/随从”等命名
- 若当集人名条/字幕与先验冲突，以当集为准；先验仅作别名或证据参考
- 换装/形象变化写入 traits，保持同一规范名

输出要求：
- 仅输出 explicit_characters、speaker_profiles、observed_interactions 三个部分
- 不输出 name_alignment_hints，不输出从属/配偶等关系结论
- 证据字段齐全（modality/quote/action_type/speech_act/certainty 等）"""

        prior_text = self._render_prior_text(prior)

        user_prompt = f"""请分析以下视频和对话内容，提取角色线索和关系信息：

【已知角色先验（最高优先）】
{prior_text}

ASR对话内容：
{asr_content}

请按照要求提取：
1. 显式角色列表
2. 每个spk_X的详细档案
3. 所有关系线索三元组

注意：仔细观看视频中的视觉信息，结合对话内容进行综合分析。"""

        # 定义JSON schema
        schema = {
            "type": "object",
            "properties": {
                "episode_id": {"type": "string"},
                "explicit_characters": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "appearance_method": {"type": "string", "enum": ["onscreen_name", "explicit_dialogue", "prior", "visual_inference"]},
                            "confidence": {"type": "number"},
                            "traits": {"type": "string"},
                            "title_or_role": {"type": "string"},
                            "evidence_quote": {"type": "string"},
                            "evidence_modality": {"type": "string", "enum": ["subtitle", "voice", "onscreen_card"]}
                        },
                        "required": ["name", "appearance_method", "confidence", "traits", "evidence_quote", "evidence_modality"]
                    }
                },
                "speaker_profiles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "visual_description": {"type": "string"},
                            "key_lines": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "emotional_tone": {"type": "string"},
                            "speaking_style": {"type": "string"}
                        },
                        "required": ["id", "visual_description", "key_lines", "emotional_tone", "speaking_style"]
                    }
                },
                "observed_interactions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "participants": {"type": "array", "items": {"type": "string"}},
                            "summary": {"type": "string"},
                            "evidence": {"type": "string"},
                            "action_type": {"type": "string", "enum": ["address", "mention"]},
                            "addressee": {"type": "string"},
                            "referents": {"type": "array", "items": {"type": "string"}},
                            "speech_act": {"type": "string", "enum": ["report", "command", "question", "inform"]},
                            "observation_type": {"type": "string", "enum": ["vocal", "subtitle", "onscreen_card", "visual_context"]},
                            "certainty_level": {"type": "string", "enum": ["high", "medium", "low"]},
                            "evidence_span": {"type": "string"},
                            "start_ms": {"type": "integer"},
                            "end_ms": {"type": "integer"}
                        },
                        "required": ["participants", "summary", "evidence", "action_type"]
                    }
                }
            },
            "required": ["episode_id", "explicit_characters", "speaker_profiles", "observed_interactions"]
        }
        
        # 重试机制
        max_retries = 3
        retry_delay = 2
        
        # 定义模型列表，用于重试时切换
        model_list = [
            'gemini-2.5-flash',  # 主要模型
            'gemini-2.0-flash',  # 备用模型1
            'gemini-2.5-pro'     # 备用模型2
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
                
                result = self.client.generate_content(
                    model=model_name,
                    prompt=user_prompt,
                    video_uri=video_uri,
                    max_tokens=model_config.get("max_tokens", 65535),
                    temperature=model_config.get("temperature", 0.1),
                    system_instruction=system_instruction,
                    schema=schema,
                    thinking_budget=thinking_budget
                )
                
                # 解析结果
                if isinstance(result, dict) and result.get("episode_id"):
                    print(f"✅ 第{attempt+1}次调用成功，提取到线索")
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
                    print(f"❌ {episode_id} {self.step_name} 最终失败: {error_type} - {e}")
                    result = {"episode_id": episode_id, "explicit_characters": [], "speaker_profiles": [], "relationship_clues": []}
        
        # 确保episode_id正确
        if isinstance(result, dict):
            result["episode_id"] = episode_id
        
        # 保存结果
        self.utils.save_json_file(output_file, result, ensure_ascii=False)
        print(f"✅ {episode_id} {self.step_name} 完成")
        
        return {
            "status": "success",
            "explicit_characters_count": len(result.get("explicit_characters", [])),
            "speaker_profiles_count": len(result.get("speaker_profiles", [])),
            "observed_interactions_count": len(result.get("observed_interactions", []))
        }

    def _load_prior(self) -> Dict[str, Any]:
        """加载先验文件，不存在则返回默认结构"""
        if os.path.exists(self.prior_file):
            try:
                with open(self.prior_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载先验失败，将使用空先验: {e}")
        return {
            "version": 1,
            "updated_at": "",
            "characters": [],
            "name_variants": {}
        }

    def _save_prior(self, prior: Dict[str, Any]):
        """保存先验文件"""
        try:
            from datetime import datetime
            prior["updated_at"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            self.utils.save_json_file(self.prior_file, prior, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ 保存先验失败: {e}")

    def _render_prior_text(self, prior: Dict[str, Any]) -> str:
        """将先验渲染为短文本注入Prompt"""
        if not prior or not prior.get("characters"):
            return "(无)"
        lines = []
        # 角色摘要（限制前若干，避免token过大）
        for ch in prior.get("characters", [])[:30]:
            name = ch.get("canonical_name", "")
            traits = ch.get("traits_brief", "")
            aliases = ch.get("aliases", [])
            alias_text = f"；常见别称：{'/'.join(aliases)}" if aliases else ""
            lines.append(f"- {name}：{traits}{alias_text}")
        # 变体归一
        variants = prior.get("name_variants", {})
        if variants:
            pair_lines = [f"{k}→{v}" for k, v in list(variants.items())[:50]]
            lines.append("变体归一：" + "；".join(pair_lines))
        return "\n".join(lines)

    def _merge_prior(self, prior: Dict[str, Any], episode_id: str):
        """将本集 explicit_characters 合并进先验"""
        # 读取本集输出
        file_path = os.path.join(self.config.project_root, episode_id, "0_2_clues.json")
        if not os.path.exists(file_path):
            return
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"⚠️ 读取本集线索失败，跳过先验合并: {e}")
            return

        characters = prior.setdefault("characters", [])
        name_variants = prior.setdefault("name_variants", {})

        # 简单的名称到索引映射
        name_to_idx = {c.get("canonical_name", ""): idx for idx, c in enumerate(characters)}

        for ec in data.get("explicit_characters", []):
            name = (ec.get("name") or "").strip()
            if not name:
                continue
            traits = (ec.get("traits") or "").strip()
            confidence = float(ec.get("confidence", 0.6) or 0.6)

            if name in name_to_idx:
                idx = name_to_idx[name]
                ch = characters[idx]
                # 合并 traits_brief（以较短摘要为主，避免无上限增长）
                old_traits = ch.get("traits_brief", "")
                if traits and traits not in old_traits:
                    ch["traits_brief"] = (old_traits + "; " + traits).strip("; ") if old_traits else traits
                # 更新证据与置信度（简单平均）
                ch["evidence_count"] = int(ch.get("evidence_count", 0)) + 1
                old_conf = float(ch.get("confidence", confidence) or confidence)
                ch["confidence"] = round((old_conf + confidence) / 2, 4)
                # 追加来源
                sources = set(ch.get("sources", []))
                sources.add(episode_id)
                ch["sources"] = sorted(list(sources))
            else:
                characters.append({
                    "canonical_name": name,
                    "aliases": [],
                    "traits_brief": traits,
                    "evidence_count": 1,
                    "confidence": round(confidence, 4),
                    "sources": [episode_id]
                })
                name_to_idx[name] = len(characters) - 1

        # 注：如需维护变体归一，可在此根据当集提示添加映射（当前最小实现不自动新增）
    
    def _generate_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """生成统计信息"""
        total_episodes = len(results)
        success_count = len([r for r in results if r.get("status") == "success"])
        failed_count = len([r for r in results if r.get("status") == "failed"])
        already_exists_count = len([r for r in results if r.get("status") == "already_exists"])
        
        total_explicit_characters = sum(r.get("explicit_characters_count", 0) for r in results)
        total_speaker_profiles = sum(r.get("speaker_profiles_count", 0) for r in results)
        total_observed_interactions = sum(r.get("observed_interactions_count", 0) for r in results)
        
        return {
            "total_episodes": total_episodes,
            "success_count": success_count,
            "failed_count": failed_count,
            "already_exists_count": already_exists_count,
            "success_rate": (success_count / total_episodes * 100) if total_episodes > 0 else 0,
            "total_explicit_characters": total_explicit_characters,
            "total_speaker_profiles": total_speaker_profiles,
            "total_observed_interactions": total_observed_interactions,
            "avg_explicit_characters": total_explicit_characters / success_count if success_count > 0 else 0,
            "avg_speaker_profiles": total_speaker_profiles / success_count if success_count > 0 else 0,
            "avg_observed_interactions": total_observed_interactions / success_count if success_count > 0 else 0
        }


if __name__ == "__main__":
    from new_pipeline.core import PipelineConfig
    config = PipelineConfig()
    step = Step0_2ClueExtraction(config)
    result = step.run()
    print(f"最终结果: {result}")
