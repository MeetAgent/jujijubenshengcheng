"""
Step0.7: 剧本撰写
基于Step0.6的情节结构提取结果，撰写标准剧本格式
"""

import os
import time
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pydantic import BaseModel

from core import PipelineConfig, GenAIClient, PipelineUtils
from core.exceptions import StepDependencyError, ModelCallError
from . import PipelineStep

from new_pipeline.steps.commont_log import log

class ScriptScene(BaseModel):
    """剧本场景"""
    scene_id: str
    slug: str  # 场景标题，如 "INT. 菜市场 - DAY"
    location: str
    time_of_day: str  # DAY, NIGHT, MORNING, EVENING
    characters: List[str]
    action_lines: List[str]
    dialogues: List[Dict[str, Any]]

class ScriptMeta(BaseModel):
    """剧本元数据"""
    episode_id: str
    title: str
    duration: float
    total_scenes: int
    main_characters: List[str]

class EpisodeScript(BaseModel):
    """单集剧本"""
    meta: ScriptMeta
    scenes: List[ScriptScene]
    stmf_content: str

class Step0_7ScriptWriting(PipelineStep):
    """Step0.7: 剧本撰写"""
    
    @property
    def step_number(self) -> int:
        return 7
    
    @property
    def step_name(self) -> str:
        return "script_writing"

    def _get_this_step_config(self) -> Dict[str, Any]:
        """精确获取当前步骤在pipeline.yaml中的配置，避免与step7/其它混淆"""
        try:
            steps = getattr(self.config, 'steps_config', None) or {}
            for key in ['step0_7', '0_7']:
                if key in steps:
                    sc = steps[key]
                    if sc.get('name') == self.step_name:
                        return sc
            for sc in steps.values():
                if isinstance(sc, dict) and sc.get('name') == self.step_name:
                    return sc
        except Exception:
            pass
        return {}
    
    def check_dependencies(self, episode_id: str = None) -> bool:
        """检查依赖"""
        if episode_id:
            # 改为检查 Step0.6 的简化输出 script_flow
            flow_file = os.path.join(
                self.utils.get_episode_output_dir(self.config.output_dir, episode_id),
                "0_6_script_flow.json"
            )
            return os.path.exists(flow_file)
        else:
            # 检查所有剧集的Step0.6输出
            episodes = self.utils.get_episode_list(self.config.project_root)
            for ep in episodes:
                flow_file = os.path.join(
                    self.utils.get_episode_output_dir(self.config.output_dir, ep),
                    "0_6_script_flow.json"
                )
                if not os.path.exists(flow_file):
                    log.info(f"缺少依赖文件: {flow_file}")
                    return False
            return True
    
    def get_output_files(self, episode_id: str = None) -> List[str]:
        """获取输出文件列表"""
        if episode_id:
            return [
                f"{episode_id}/0_7_script.stmf",
                f"{episode_id}/0_7_script_analysis.json"
            ]
        return []
    
    def run(self, episode_id: str = None) -> Dict[str, Any]:
        """运行剧本撰写步骤"""
        if episode_id:
            return self._run_single_episode(episode_id)
        else:
            return self._run_all_episodes()
    
    def _run_single_episode(self, episode_id: str) -> Dict[str, Any]:
        """处理单个剧集"""
        start_time = time.time()
        log.info(f"Step0.7: 处理 {episode_id}")
        
        # 检查是否已经存在输出文件
        episode_out_dir = self.utils.get_episode_output_dir(self.config.output_dir, episode_id)
        script_file = os.path.join(episode_out_dir, "0_7_script.stmf")
        analysis_file = os.path.join(episode_out_dir, "0_7_script_analysis.json")
        
        if os.path.exists(script_file) and os.path.exists(analysis_file) and not os.environ.get("FORCE_OVERWRITE"):
            log.info(f"✅ {episode_id} 已有Step0.7输出文件，跳过处理")
            return {"status": "already_exists"}
        
        # 读取Step0.6/0.5 产物（改为消费 script_flow）
        flow_file = os.path.join(episode_out_dir, "0_6_script_flow.json")
        turns_file = os.path.join(episode_out_dir, "0_5_dialogue_turns.json")
        
        script_flow = {}
        if os.path.exists(flow_file):
            script_flow = self.utils.load_json_file(flow_file, {})
        dialogue_turns = []
        if os.path.exists(turns_file):
            td = self.utils.load_json_file(turns_file, {})
            dialogue_turns = td.get("dialogue_turns", [])
        
        # 获取视频URI
        video_uri = self.utils.get_video_uri(episode_id, self.config.project_root)
        
        # 获取模型配置
        model_config = self.config.get_model_config(7)
        
        try:
            step_conf = self.config.get_step_config_by_name('step0_7')
            stmf_mode = (step_conf.get('stmf_mode') or 'bracketed').lower()
            use_video = bool(step_conf.get('use_video', False))
            
            # 优先：基于 Step0.6 简化输出 script_flow，通过 LLM 重写为好莱坞风格的方括号 STMF
            if script_flow and stmf_mode == 'bracketed':
                stmf_content = self._render_from_script_flow_hollywood(script_flow, dialogue_turns, episode_id, model_config, use_video, video_uri)
                # 日志记录 stmf_content 位置
                log.info(f"{episode_id}，{stmf_content} 0_7_script.stmf: {script_file}")
                self.utils.save_text_file(script_file, stmf_content)
                # 保存分析（简要）
                analysis = {
                    "episode_id": episode_id,
                    "mode": "script_flow_hollywood",
                    "use_video": use_video,
                    "scenes_count": len((script_flow or {}).get("scenes", [])),
                    "beats_count": 0
                }
                self.utils.save_json_file(analysis_file, analysis)
                processing_time = time.time() - start_time
                log.info(f"✅ {episode_id} Step0.7 完成 (用时: {processing_time:.2f}秒)")
                return {"status": "success", "processing_time": round(processing_time, 2), "scenes_count": analysis["scenes_count"], "characters_count": None, "total_dialogues": len(dialogue_turns)}
            
            # 否则：使用现有生成/规范化逻辑（兼容保留）
            plot_data = {}
            script_result = self._write_script(plot_data, dialogue_turns, video_uri, model_config, episode_id)
            step_conf2 = self.config.get_step_config(7)
            stmf_mode2 = (step_conf2.get('stmf_mode') or 'bracketed').lower()
            if stmf_mode2 == 'bracketed':
                normalized_stmf = self._render_bracketed_from_json(plot_data, dialogue_turns, episode_id)
            else:
                normalized_stmf = self._normalize_stmf_for_fountain(script_result.stmf_content)
            script_result.stmf_content = normalized_stmf
            self.utils.save_text_file(script_file, normalized_stmf)
            self.utils.save_json_file(analysis_file, script_result.dict())
            processing_time = time.time() - start_time
            log.info(f"✅ {episode_id} Step0.7 完成 (用时: {processing_time:.2f}秒),{script_file}保存了STMF文件")
            return {"status": "success", "processing_time": round(processing_time, 2), "scenes_count": len(script_result.scenes), "characters_count": None, "total_dialogues": len(dialogue_turns)}
        except Exception as e:
            log.info(f"❌ {episode_id} Step0.7 失败: {e}")
            return {"status": "failed", "error": str(e)}

    def _write_script(self, plot_data: Dict, dialogue_turns: List[Dict], 
                     video_uri: str, model_config: Dict, episode_id: str) -> EpisodeScript:
        """撰写剧本"""
        
        # 配置：是否使用视频作为额外证据
        step_conf = self.config.get_step_config_by_name('step0_7')
        use_video = bool(step_conf.get('use_video', False))

        # 构建系统指令
        system_instruction = """你是一位专业的电影剧本作家，擅长将情节结构转换为标准的好莱坞剧本格式。
 
 【核心任务】
 1. **剧本格式转换**：将情节结构转换为STMF格式的剧本
 2. **场景构建**：根据情节分析构建完整的场景描述
 3. **对话整合**：将对话轮次数据整合到剧本中
 4. **动作描述**：基于情节和对话生成生动的动作描述
 
 【STMF格式规范】
 - 纯文本，UTF-8编码
 - 每行格式：`标签 [属性] : 内容`
 - 空行允许，以#开头为注释
 - 每行必须以换行符结束
 
 【核心标签】
 1. **META** - 元数据：episode_id, title, duration
 2. **SCENE** - 场景标题：slug, loc, tod
 3. **ACTION** - 动作描述：角色行为和场景描述
 4. **DIALOG** - 对话块：entity, display, char
 5. **TRANS** - 转场：场景切换
 
 【好莱坞格式规范】
 - 场景标题：INT./EXT. 地点 - 时间
 - 角色名称：使用display作为显示名
 - 画外音：使用 (V.O.) 或 (O.S.)
 - 动作描述：现在时态，简洁有力
 
 【写作风格要求】
 请以好莱坞专业编剧的风格撰写，专注于视觉化 storytelling。动作描述必须简洁、有力、客观；避免描写人物的内心思想、使用过多的形容词，或给出具体的导演指令。让角色的情绪通过他们的行为来展现。
 
 【输出要求】
 严格按照JSON Schema输出，确保剧本格式的完整性和专业性。"""

        # 构建用户提示
        if use_video:
            mode_header = "【模式】视频+转写：在不违背画面证据的前提下，整合情节与台词；所有非台词细节以视频为真。"
        else:
            mode_header = "【模式】纯转写编辑：不使用视频，仅基于情节结构与对话轮次进行规范化剧本转写，禁止臆造新增画面细节。"

        # 可选背景：全剧大纲与人物小传
        step_conf = self.config.get_step_config_by_name('step0_7')
        bg_outline = ""
        bg_chars = ""
        try:
            if step_conf.get('use_outline', False):
                outline_path = os.path.join(self.config.output_dir, 'global', 'series_outline.md')
                if os.path.exists(outline_path):
                    bg_outline = self.utils.load_text_file(outline_path)
            if step_conf.get('use_character_bios', False):
                graph_path = os.path.join(self.config.output_dir, 'global', 'global_character_graph_llm.json')
                if os.path.exists(graph_path):
                    graph = self.utils.load_json_file(graph_path, {})
                    chars = graph.get('characters', [])
                    lines = []
                    for ch in chars:
                        name = ch.get('canonical_name', '')
                        traits = ch.get('traits', '')
                        aliases = ", ".join(ch.get('aliases', []))
                        lines.append(f"- {name}｜别名：{aliases}｜特征：{traits}")
                    bg_chars = "\n".join(lines)
        except Exception:
            pass

        bg_block = ""
        if bg_outline or bg_chars:
            bg_block = f"""
【背景（仅用于消歧与一致性；当与视频/对话冲突时，以视频/对话为准；use_video=false时禁止据此虚构画面）】
大纲：{bg_outline}
人物小传：
{bg_chars}
"""
 
        # 从plot_data提炼可用的scene_snapshot摘要（仅用于一致性约束）
        def _summarize_snapshots(pd: Dict) -> str:
            lines = []
            for sc in pd.get('scenes', []):
                snap = sc.get('scene_snapshot') or {}
                if not snap:
                    continue
                sid = sc.get('scene_id', '')
                locb = snap.get('location_brief', '')
                blk = snap.get('blocking', '')
                props = ", ".join((snap.get('props') or []))
                hooks = ", ".join((snap.get('continuity_hooks') or []))
                cue = ", ".join((snap.get('camera_cues') or []))
                lines.append(f"- {sid}: {locb}｜blocking={blk}｜props={props}｜hooks={hooks}｜camera={cue}")
            return "\n".join(lines)

        snapshot_block = _summarize_snapshots(plot_data)
        snapshot_clause = ""
        if snapshot_block:
            snapshot_clause = f"""
【一致性硬约束（scene_snapshot）】
以下由Step0.6输出的客观场景快照为事实依据，用于道具/站位/驾驶身份/连贯要素的强约束；若未给出相应字段，禁止臆造：
{snapshot_block}
"""

        user_prompt = f"""{mode_header}
{snapshot_clause}
{bg_block}

请基于以下情节结构数据，撰写第{episode_id}集的完整剧本：

**情节结构数据**:
{self._format_plot_data(plot_data)}

**对话轮次数据**:
{self._format_dialogue_turns(dialogue_turns)}

**撰写要求**:

1. **场景构建**：
   - 根据情节分析中的场景信息构建完整的场景描述
   - 每个场景包含：场景标题、地点、时间、参与角色
   - 场景标题格式：INT./EXT. 地点 - 时间

2. **动作描述**：
   - 基于情节和对话生成生动的动作描述（use_video=true时需与视频一致；use_video=false时不得臆造；必须满足scene_snapshot的一致性约束）
   - 描述角色的行为、表情、肢体语言
   - 描述场景环境和氛围

3. **对话整合**：
   - 将对话轮次数据完整整合到剧本中
   - 保持原始对话的完整性和准确性
   - 使用标准的角色名称格式

4. **格式规范**：
   - 严格按照STMF格式输出
   - 确保每行都是独立的，不使用转义字符
   - 遵循好莱坞剧本格式规范

5. **角色标准化**：
   - 使用display作为显示名
   - 保持角色名称的一致性
   - 正确处理画外音和旁白

请生成完整的STMF格式剧本，确保格式正确、内容完整、专业规范。"""

        # 定义输出Schema
        schema = {
            "type": "OBJECT",
            "properties": {
                "meta": {
                    "type": "OBJECT",
                    "properties": {
                        "episode_id": {"type": "STRING"},
                        "title": {"type": "STRING"},
                        "duration": {"type": "NUMBER"},
                        "total_scenes": {"type": "INTEGER"},
                        "main_characters": {
                            "type": "ARRAY",
                            "items": {"type": "STRING"}
                        }
                    },
                    "required": ["episode_id", "title", "duration", "total_scenes", "main_characters"]
                },
                "scenes": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "scene_id": {"type": "STRING"},
                            "slug": {"type": "STRING"},
                            "location": {"type": "STRING"},
                            "time_of_day": {"type": "STRING"},
                            "characters": {
                                "type": "ARRAY",
                                "items": {"type": "STRING"}
                            },
                            "action_lines": {
                                "type": "ARRAY",
                                "items": {"type": "STRING"}
                            },
                            "dialogues": {
                                "type": "ARRAY",
                                "items": {
                                    "type": "OBJECT",
                                    "properties": {
                                        "character": {"type": "STRING"},
                                        "display_name": {"type": "STRING"},
                                        "dialogue": {"type": "STRING"}
                                    },
                                    "required": ["character", "display_name", "dialogue"]
                                }
                            }
                        },
                        "required": ["scene_id", "slug", "location", "time_of_day", "characters", "action_lines", "dialogues"]
                    }
                },
                "stmf_content": {"type": "STRING"}
            },
            "required": ["meta", "scenes", "stmf_content"]
        }
        
        kwargs = {
            'model': model_config['model'],
            'prompt': user_prompt,
            'system_instruction': system_instruction,
            'schema': schema,
            'max_tokens': model_config.get('max_tokens', 65535),
            'temperature': model_config.get('temperature', 0.1)
        }
        if use_video and video_uri:
            kwargs['video_uri'] = video_uri

        result = self.client.generate_content(**kwargs)

        # debug: 打印完整响应内容
        log.debug(f"剧本撰写响应: {result}")
        
        # 验证和转换结果
        return self._validate_and_convert_result(result, episode_id)
    
    def _format_plot_data(self, plot_data: Dict) -> str:
        """格式化情节数据"""
        formatted = []
        formatted.append(f"剧集ID: {plot_data.get('episode_id', 'N/A')}")
        formatted.append(f"标题: {plot_data.get('title', 'N/A')}")
        formatted.append(f"主要角色: {', '.join(plot_data.get('main_characters', []))}")
        formatted.append(f"场景数量: {len(plot_data.get('scenes', []))}")
        formatted.append(f"情节节点数量: {len(plot_data.get('plot_beats', []))}")
        formatted.append("")
        
        # 场景信息
        formatted.append("场景信息:")
        for scene in plot_data.get('scenes', []):
            formatted.append(f"  场景 {scene.get('scene_id', 'N/A')}: {scene.get('location', 'N/A')}")
            formatted.append(f"    轮次: {scene.get('start_turn', 0)}-{scene.get('end_turn', 0)}")
            formatted.append(f"    角色: {', '.join(scene.get('characters', []))}")
            formatted.append(f"    关键事件: {', '.join(scene.get('key_events', []))}")
            formatted.append(f"    情感弧线: {scene.get('emotional_arc', 'N/A')}")
            formatted.append("")
        
        # 情节节点
        formatted.append("情节节点:")
        for beat in plot_data.get('plot_beats', []):
            formatted.append(f"  节点 {beat.get('beat_id', 'N/A')} ({beat.get('beat_type', 'N/A')})")
            formatted.append(f"    描述: {beat.get('description', 'N/A')}")
            formatted.append(f"    参与角色: {', '.join(beat.get('characters_involved', []))}")
            formatted.append(f"    情感基调: {beat.get('emotional_tone', 'N/A')}")
            formatted.append("")
        
        return "\n".join(formatted)
    
    def _format_dialogue_turns(self, dialogue_turns: List[Dict]) -> str:
        """格式化对话轮次数据"""
        if not dialogue_turns:
            return "无对话数据"
        
        formatted = []
        formatted.append(f"对话轮次数量: {len(dialogue_turns)}")
        formatted.append("")
        
        for turn in dialogue_turns:
            formatted.append(f"轮次 {turn.get('turn_id', 'N/A')}: [{turn.get('speaker', 'N/A')}] {turn.get('full_dialogue', 'N/A')}")
            formatted.append(f"  情感: {turn.get('emotion', 'N/A')}, 意图: {turn.get('intent', 'N/A')}")
            formatted.append("")
        
        return "\n".join(formatted)
    
    def _validate_and_convert_result(self, result: Dict, episode_id: str) -> EpisodeScript:
        """验证和转换结果"""
        try:
            # 确保episode_id正确
            if "meta" in result:
                result["meta"]["episode_id"] = episode_id
            
            # 验证必要字段
            if "meta" not in result:
                result["meta"] = {
                    "episode_id": episode_id,
                    "title": f"{episode_id} 剧本",
                    "duration": 1.0,
                    "total_scenes": 0,
                    "main_characters": []
                }
            
            if "scenes" not in result:
                result["scenes"] = []
            
            if "stmf_content" not in result:
                result["stmf_content"] = self._generate_default_stmf(result["meta"], result["scenes"])
            
            # 创建EpisodeScript对象
            return EpisodeScript(**result)
            
        except Exception as e:
            log.info(f"Warning: 结果验证失败，使用默认值: {e}")
            # 返回默认结构
            return EpisodeScript(
                meta=ScriptMeta(
                    episode_id=episode_id,
                    title=f"{episode_id} 剧本",
                    duration=1.0,
                    total_scenes=0,
                    main_characters=[]
                ),
                scenes=[],
                stmf_content=f"META episode_id={episode_id} title=\"{episode_id} 剧本\" duration=1.0\n\n# 剧本生成失败"
            )
    
    def _generate_default_stmf(self, meta: Dict, scenes: List[Dict]) -> str:
        """生成默认STMF内容"""
        stmf_lines = []
        stmf_lines.append(f"META episode_id={meta['episode_id']} title=\"{meta['title']}\" duration={meta['duration']}")
        stmf_lines.append("")
        
        for scene in scenes:
            stmf_lines.append(f"SCENE [slug]=\"{scene.get('slug', 'N/A')}\" [loc] : {scene.get('location', 'N/A')} [tod] : {scene.get('time_of_day', 'DAY')}")
            stmf_lines.append("")
            
            # 动作描述
            for action in scene.get('action_lines', []):
                stmf_lines.append(f"ACTION : {action}")
                stmf_lines.append("")
            
            # 对话
            for dialogue in scene.get('dialogues', []):
                # 标准头：仅display/char，不输出entity
                display = dialogue.get('display_name', '')
                char = dialogue.get('character', '')
                header_parts = []
                if display:
                    header_parts.append(f"[display] : {display}")
                if char:
                    header_parts.append(f"[char] : {char}")
                if header_parts:
                    stmf_lines.append("DIALOG " + " ".join(header_parts))
                # 台词
                stmf_lines.append(f"DIALOG : {dialogue.get('dialogue', '')}")
                stmf_lines.append("ENDDIALOG")
                stmf_lines.append("")
        
        return "\n".join(stmf_lines)

    def _normalize_stmf_for_fountain(self, content: str) -> str:
        """规范化STMF以最大兼容性：
        - 移除entity属性，仅保留display/char
        - 合并连续的DIALOG头（多行display/char），仅输出一行头；丢弃占位'角色'
        - 将角色头与台词分离：头部仅属性行，台词使用`DIALOG : 文本`
        - 规范PAREN行为：保持`PAREN : 文本`（括号由下游添加）
        - 清理多余空格与多余空行
        """
        import re
        src = content.split('\n')
        out: List[str] = []
        i = 0
        n = len(src)
        while i < n:
            line = src[i].rstrip()
            if not line:
                # 压缩空行：最多保留一个
                if not out or out[-1] != "":
                    out.append("")
                i += 1
                continue
            if ':' not in line:
                out.append(line)
                i += 1
                continue
            tag_part, rest = line.split(':', 1)
            tag = tag_part.strip()
            rest = rest.strip()
            # 处理连续的DIALOG头（属性行）
            if tag.startswith('DIALOG') and ('[display]' in line or '[char]' in line or '[entity]' in line):
                display_val = None
                char_val = None
                # 向前聚合连续的DIALOG属性行
                j = i
                while j < n:
                    l2 = src[j].rstrip()
                    if ':' not in l2:
                        break
                    t2, r2 = l2.split(':', 1)
                    t2 = t2.strip()
                    if not t2.startswith('DIALOG'):
                        break
                    if ('[display]' in l2) or ('[char]' in l2) or ('[entity]' in l2):
                        dv = self._extract_attr_value(l2, 'display')
                        cv = self._extract_attr_value(l2, 'char')
                        if dv and dv != '角色':
                            display_val = dv
                        if cv and cv != '角色':
                            char_val = cv
                        j += 1
                        continue
                    else:
                        break
                # 输出合并后的单行头（移除 entity，占位'角色'不输出）
                header_parts = []
                if display_val and display_val != '角色':
                    header_parts.append(f"[display] : {display_val}")
                if char_val and char_val != '角色':
                    header_parts.append(f"[char] : {char_val}")
                if header_parts:
                    out.append("DIALOG " + " ".join(header_parts))
                # 向后推进
                i = j
                continue
            # 标准化DIALOG台词
            if tag == 'DIALOG':
                out.append(f"DIALOG : {rest}")
                i += 1
                continue
            # 标准化PAREN
            if tag == 'PAREN':
                out.append(f"PAREN : {rest}")
                i += 1
                continue
            # 其它标签原样（矫正空格）
            out.append(f"{tag} : {rest}")
            i += 1

        # 二次压缩多余空行
        normalized: List[str] = []
        prev_blank = False
        for l in out:
            if l.strip() == "":
                if not prev_blank:
                    normalized.append("")
                prev_blank = True
            else:
                normalized.append(l)
                prev_blank = False
        return "\n".join(normalized).strip() + "\n"

    def _render_bracketed_from_json(self, plot_data: Dict, dialogue_turns: List[Dict], episode_id: str) -> str:
        """直接从plot_data与dialogue_turns渲染为方括号STMF，遵循最大兼容格式：
        [EPISODE]/[SCENE]/[ACTION]/[CHARACTER]/[PAREN]/[DIALOG]/[TRANS]
        """
        out_lines: List[str] = []
        import re
        # EPISODE 头
        ep_num_match = re.findall(r"\d+", episode_id or "")
        ep_num = ep_num_match[0] if ep_num_match else episode_id
        out_lines.append(f"[EPISODE] Episode {ep_num}")
        out_lines.append("")

        scenes = plot_data.get('scenes', [])
        # 建quick索引：turn_id -> turn
        turns_by_id = {}
        for t in dialogue_turns or []:
            if isinstance(t, dict) and t.get('turn_id') is not None:
                turns_by_id[t['turn_id']] = t

        def scene_heading(scene_idx: int, sc: Dict) -> str:
            loc = sc.get('location') or (sc.get('scene_snapshot') or {}).get('location_brief') or '场景'
            loc_up = str(loc).upper()
            tod = '日'
            # 简易TOD判定
            if '夜' in loc or '夜' in (sc.get('emotional_arc') or ''):
                tod = '夜'
            elif '白天' in (sc.get('emotional_arc') or ''):
                tod = '日'
            # INT/EXT判定
            scene_type = 'INT.' if any(k in loc for k in ['内','中','室','厅']) else 'EXT.'
            return f"{scene_idx}-1. {scene_type} {loc_up} – {tod}"

        def map_emotion_to_paren(em: str, intent: str) -> str:
            txt = ''
            if em:
                if any(k in em for k in ['轻蔑','冷笑']):
                    txt = '轻蔑'
                elif any(k in em for k in ['愤怒','生气']):
                    txt = '愤怒'
                elif any(k in em for k in ['担忧','不安','焦虑']):
                    txt = '不安'
                elif any(k in em for k in ['平静','冷静']):
                    txt = ''
            if not txt and intent:
                if '威胁' in intent or '施压' in intent:
                    txt = '施压'
                elif '试探' in intent:
                    txt = '试探'
            return f"({txt})" if txt else ''

        scene_index = 1
        for sc in scenes:
            # 场景标题
            out_lines.append(f"[SCENE] {scene_heading(scene_index, sc)}")
            out_lines.append("")
            scene_index += 1
            # 场景动作（优先snapshot.actions / key_events）
            snap = sc.get('scene_snapshot') or {}
            actions_pool = []
            actions_pool += (snap.get('actions') or [])
            actions_pool += (sc.get('key_events') or [])
            actions_pool = [str(a).strip() for a in actions_pool if str(a).strip()]

            # 对话：从dialogue_turns切片（简单按turn_id范围映射；若无范围信息，则按顺序输出全部turns）
            turns_seq = []
            st = sc.get('start_turn')
            ed = sc.get('end_turn')
            if st is not None and ed is not None:
                for tid in range(st, ed + 1):
                    if tid in turns_by_id:
                        turns_seq.append(turns_by_id[tid])
            else:
                turns_seq = dialogue_turns or []

            # 交织输出：开场ACTION、每个turn前插入一条ACTION、最后收尾ACTION
            def pop_action() -> str:
                return actions_pool.pop(0) if actions_pool else ''

            # 开场动作（若有）
            a0 = pop_action()
            if a0:
                out_lines.append(f"[ACTION] {a0}")

            last_char = None
            for turn in turns_seq:
                # 每个turn前尝试插入一条动作
                ai = pop_action()
                if ai:
                    out_lines.append(f"[ACTION] {ai}")

                spk = (turn.get('speaker') or '').strip()
                if not spk:
                    continue
                cue = spk.upper()
                if cue != last_char:
                    out_lines.append("")
                    out_lines.append(f"[CHARACTER] {cue}")
                    last_char = cue
                # paren（情绪/意图/V.O.等）
                em = (turn.get('emotion') or '').strip()
                it = (turn.get('intent') or '').strip()
                par = map_emotion_to_paren(em, it)
                text = (turn.get('full_dialogue') or '').strip()
                if '(电话' in text or '电话' in text:
                    par = (par[:-1] + '; V.O.)') if par else '(V.O.)'
                if par:
                    out_lines.append(f"[PAREN] {par}")
                if text:
                    out_lines.append(f"[DIALOG] {text}")
            # 输出剩余动作
            if actions_pool:
                out_lines.append("")
                for a in actions_pool:
                    out_lines.append(f"[ACTION] {a}")
                actions_pool.clear()

            # 场景结束转场
            out_lines.append("")
            out_lines.append("[TRANS] CUT TO:")
            out_lines.append("")

        # 压缩空行
        compact: List[str] = []
        prev_blank = False
        for l in out_lines:
            if l.strip() == '':
                if not prev_blank:
                    compact.append("")
                prev_blank = True
            else:
                compact.append(l)
                prev_blank = False
        return "\n".join(compact).strip() + "\n"

    def _render_from_index_and_narrative(self, index_json: Dict[str, Any], narrative_md: str, dialogue_turns: List[Dict], episode_id: str) -> str:
        """基于Step0.6的narrative与index渲染方括号STMF：
        - 顺序以beats.order/start_turn为准；无beats时按scenes顺序
        - ACTION来自narrative的对应[情节N]段落与index.key_actions
        - 台词来自0.5的turns，按beat起止贴入
        """
        import re
        lines: List[str] = []
        # EPISODE 头
        import re as _re
        ep_num_match = _re.findall(r"\d+", episode_id or "")
        ep_num = ep_num_match[0] if ep_num_match else episode_id
        lines.append(f"[EPISODE] Episode {ep_num}")
        lines.append("")
        # 解析narrative分段
        para_by_idx: Dict[int, List[str]] = {}
        cur = None
        for raw in (narrative_md or '').split('\n'):
            s = raw.strip()
            m = re.match(r"^\[情节(\d+)\]", s)
            if m:
                cur = int(m.group(1))
                para_by_idx[cur] = []
                # 去掉标签后的本行剩余
                remain = s[m.end():].strip()
                if remain:
                    para_by_idx[cur].append(remain)
                continue
            if cur is not None and s:
                para_by_idx[cur].append(s)
        # 索引
        scenes = index_json.get('scenes', [])
        beats = index_json.get('beats', [])
        beats = sorted(beats, key=lambda b: (b.get('order') if b.get('order') is not None else 10**9,
                                             b.get('start_turn') if b.get('start_turn') is not None else 10**9))
        # turn索引
        turns_map = {t.get('turn_id'): t for t in (dialogue_turns or []) if isinstance(t, dict)}
        turns_seq = dialogue_turns or []
        # 场景标题辅助
        def make_heading(idx: int, heading_hint: str, tod_hint: str) -> str:
            loc = (heading_hint or '场景').upper()
            tod = '日'
            if '夜' in (tod_hint or ''):
                tod = '夜'
            stype = 'INT.' if any(k in loc for k in ['内','中','室','厅']) else 'EXT.'
            return f"{idx}-1. {stype} {loc} – {tod}"
        # 遍历beats（无beats时退化：用scenes整体）
        if beats:
            scene_idx = 0
            last_scene_id = None
            for i, bt in enumerate(beats, start=1):
                # 找到所属scene
                sc_hint = scenes[scene_idx] if scenes else None
                if sc_hint and (last_scene_id != sc_hint.get('scene_id')):
                    lines.append(f"[SCENE] {make_heading(scene_idx+1, sc_hint.get('heading_hint',''), sc_hint.get('tod_hint',''))}")
                    lines.append("")
                    last_scene_id = sc_hint.get('scene_id')
                # ACTION：来自narrative情节段落 i
                for s in para_by_idx.get(i, []):
                    # 过滤列表前缀标记
                    s_clean = re.sub(r"^[-•\*]\s*", "", s)
                    lines.append(f"[ACTION] {s_clean}")
                # 关键动作补充
                for a in (bt.get('key_actions') or []):
                    lines.append(f"[ACTION] {a}")
                # 对话：按turn区间
                st = bt.get('start_turn')
                ed = bt.get('end_turn')
                beat_turns = []
                if st is not None and ed is not None:
                    for t in turns_seq:
                        tid = t.get('turn_id')
                        if tid is not None and st <= tid <= ed:
                            beat_turns.append(t)
                for t in beat_turns:
                    spk = (t.get('speaker') or '').strip().upper()
                    if not spk:
                        continue
                    lines.append(f"[CHARACTER] {spk}")
                    em = (t.get('emotion') or '').strip()
                    it = (t.get('intent') or '').strip()
                    par = ''
                    if em or it:
                        p = []
                        if any(k in em for k in ['轻蔑','冷笑']): p.append('轻蔑')
                        elif any(k in em for k in ['愤怒','生气']): p.append('愤怒')
                        elif any(k in em for k in ['担忧','不安','焦虑']): p.append('不安')
                        if '威胁' in it or '施压' in it: p.append('施压')
                        elif '试探' in it: p.append('试探')
                        if p:
                            par = '(' + '; '.join(p) + ')'
                    text = (t.get('full_dialogue') or '').strip()
                    if par:
                        lines.append(f"[PAREN] {par}")
                    if text:
                        lines.append(f"[DIALOG] {text}")
                lines.append("")
                lines.append("[TRANS] CUT TO:")
                lines.append("")
        else:
            # 退化：仅按scenes顺序 + 全量turns
            for idx, sc in enumerate(scenes or [], start=1):
                lines.append(f"[SCENE] {make_heading(idx, sc.get('heading_hint',''), sc.get('tod_hint',''))}")
                lines.append("")
                for t in turns_seq:
                    spk = (t.get('speaker') or '').strip().upper()
                    if not spk:
                        continue
                    lines.append(f"[CHARACTER] {spk}")
                    text = (t.get('full_dialogue') or '').strip()
                    if text:
                        lines.append(f"[DIALOG] {text}")
                lines.append("")
                lines.append("[TRANS] CUT TO:")
                lines.append("")
        # 压缩空行
        compact: List[str] = []
        prev_blank = False
        for l in lines:
            if l.strip() == '':
                if not prev_blank:
                    compact.append("")
                prev_blank = True
            else:
                compact.append(l)
                prev_blank = False
        return "\n".join(compact).strip() + "\n"

    def _normalize_scene_headings_from_flow(self, script_flow: Dict, stmf_text: str) -> str:
        """将生成的 STMF 中的 [SCENE] 行，按 script_flow.scenes 的顺序替换为标准 setting。
        同时在缺失时补全 INT./EXT. 与 DAY/NIGHT。
        - 仅替换 [SCENE] 开头的行
        - 避免更改其它内容
        """
        try:
            scenes = script_flow.get('scenes') or []
            def _std(s: str) -> str:
                txt = (s or 'SCENE').strip()
                up = txt.upper()
                has_ie = up.startswith('INT.') or up.startswith('EXT.') or up.startswith('INT/EXT')
                has_tod = any(t in up for t in [' DAY', ' NIGHT', ' DAWN', ' DUSK']) or ' – ' in txt or ' - ' in txt
                if has_ie and any(k in up for k in [' DAY', ' NIGHT', ' DAWN', ' DUSK']):
                    return txt
                # 猜测INT/EXT
                ie = 'INT.'
                low = txt.lower()
                if any(k in low for k in ['外', '室外', 'ext.']):
                    ie = 'EXT.'
                # 提取地点（去掉已有前缀）
                loc = txt
                loc = loc.replace('INT.', '').replace('EXT.', '').replace('INT/EXT.', '').strip(' -–—')
                if not loc:
                    loc = 'LOCATION'
                # 猜测TOD
                tod = 'DAY'
                if any(k in low for k in ['夜', 'night']):
                    tod = 'NIGHT'
                elif any(k in low for k in ['黄昏', 'dusk']):
                    tod = 'DUSK'
                elif any(k in low for k in ['黎明', '清晨', 'dawn']):
                    tod = 'DAWN'
                return f"{ie} {loc} – {tod}"

            settings = [ _std(s.get('setting')) for s in scenes ]
            lines = stmf_text.split('\n')
            out: List[str] = []
            idx = 0
            for line in lines:
                if line.startswith('[SCENE]') and idx < len(settings):
                    out.append(f"[SCENE] {settings[idx]}")
                    idx += 1
                else:
                    out.append(line)
            return "\n".join(out).rstrip('\n') + "\n"
        except Exception:
            return stmf_text

    def _postprocess_stmf(self, script_flow: Dict, episode_id: str, stmf_text: str) -> str:
        """后处理 STMF：
        - [SCENE] 标题编号为 <ep>-<idx>. 并标准化 INT./EXT./DAY/NIGHT
        - 从 script_flow 注入缺失的 [PAREN]
        - 移除 [TRANS] 行
        - 压缩空行
        """
        try:
            # 移除 [TRANS]
            lines = [l for l in stmf_text.split('\n') if not l.startswith('[TRANS]')]

            # 合并连续的 [PAREN] 行
            lines = self._merge_consecutive_paren_lines(lines)

            # 计算剧集编号
            import re as _re
            ep_num_match = _re.findall(r"\d+", episode_id or "")
            ep_num = ep_num_match[0] if ep_num_match else (episode_id or "1")
            # 去除前导零
            try:
                ep_num = str(int(ep_num))
            except Exception:
                pass

            # 准备标准化后的 setting 列表
            def std_setting(s: str) -> str:
                tmp = self._normalize_scene_headings_from_flow({'scenes':[{'setting':s}]}, '[SCENE] X\n')
                return tmp.split('\n')[0].replace('[SCENE] ','')
            settings = [ std_setting((s.get('setting') or 'SCENE').strip()) for s in (script_flow.get('scenes') or []) ]

            # 场景标题编号（追加 narrative_device 后缀）
            idx = 0
            for i, l in enumerate(lines):
                if l.startswith('[SCENE]'):
                    if idx < len(settings):
                        suffix = ''
                        # 从 script_flow 读取 narrative_device
                        nd = None
                        if idx < len((script_flow.get('scenes') or [])):
                            nd = (script_flow['scenes'][idx].get('narrative_device') or '').strip()
                        if nd:
                            suffix = f" ({nd})"
                        lines[i] = f"[SCENE] {ep_num}-{idx+1}. {settings[idx]}{suffix}"
                    idx += 1

            # 组织每场对白的 parenthetical 列表
            par_by_scene: List[List[str]] = []
            for sc in (script_flow.get('scenes') or []):
                arr: List[str] = []
                for el in (sc.get('elements') or []):
                    if (el.get('element_type') or '').upper() == 'DIALOGUE':
                        arr.append((el.get('parenthetical') or '').strip())
                par_by_scene.append(arr)

            # 切块：按 [SCENE]
            blocks: List[List[str]] = []
            cur: List[str] = []
            for l in lines:
                if l.startswith('[SCENE]'):
                    if cur:
                        blocks.append(cur)
                    cur = [l]
                else:
                    cur.append(l)
            if cur:
                blocks.append(cur)
            # 修正：若首块不是以 [SCENE] 开头（如包含 [EPISODE] 前言），丢弃该块以与场景索引对齐
            if blocks and (not blocks[0] or not blocks[0][0].startswith('[SCENE]')):
                blocks = blocks[1:]

            # 注入 PAREN：改为“忠实渲染上游”，不再自行判断新增，仅当上游提供时渲染
            new_blocks: List[List[str]] = []
            for si, block in enumerate(blocks):
                pars = par_by_scene[si] if si < len(par_by_scene) else []
                di = 0
                nb: List[str] = []
                for l in block:
                    if l.startswith('[DIALOG]'):
                        # 检查前一行是否已经是 [PAREN]，如果是则不重复添加
                        if nb and nb[-1].startswith('[PAREN]'):
                            # 前一行已经是 [PAREN]，直接添加对话
                            nb.append(l)
                        else:
                            # 前一行不是 [PAREN]，可以注入
                            par_txt = pars[di] if di < len(pars) else ''
                            if par_txt:
                                nb.append(f"[PAREN] ({par_txt})")
                            nb.append(l)
                        di += 1
                    else:
                        nb.append(l)
                new_blocks.append(nb)

            # 压缩空行
            out: List[str] = []
            prev_blank = False
            for block in new_blocks:
                for l in block:
                    if l.strip() == '':
                        if not prev_blank:
                            out.append('')
                        prev_blank = True
                    else:
                        out.append(l.rstrip())
                        prev_blank = False
            return "\n".join(out).rstrip('\n') + "\n"
        except Exception:
            return stmf_text

    def _extract_attr_value(self, line: str, attr_name: str) -> str:
        """从STMF行中提取属性值"""
        import re
        match = re.search(rf"\[{re.escape(attr_name)}\] : (.*)", line)
        if match:
            return match.group(1).strip()
        return ""
    
    def _merge_consecutive_paren_lines(self, lines: List[str]) -> List[str]:
        """合并连续的 [PAREN] 行"""
        if not lines:
            return lines
        
        result = []
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith('[PAREN]'):
                # 收集所有连续的 [PAREN] 行
                paren_contents = []
                while i < len(lines) and lines[i].startswith('[PAREN]'):
                    paren_line = lines[i]
                    # 提取括号内的内容
                    import re
                    match = re.search(r'\[PAREN\] \((.*?)\)', paren_line)
                    if match:
                        paren_contents.append(match.group(1))
                    i += 1
                
                # 合并所有情绪标注
                if paren_contents:
                    merged_content = '，'.join(paren_contents)
                    result.append(f'[PAREN] ({merged_content})')
                
                # 不要在这里添加下一行，让循环自然继续
            else:
                result.append(line)
                i += 1
        
        return result

    def _format_script_flow_for_prompt(self, script_flow: Dict, dialogue_turns: List[Dict]) -> str:
        """将 Step0.6 的 script_flow 压缩为可读提示片段。"""
        lines: List[str] = []
        lines.append(f"剧集: {script_flow.get('episode_id','N/A')}｜标题: {script_flow.get('title','N/A')}")
        scenes = script_flow.get('scenes') or []
        lines.append(f"场景数: {len(scenes)}")
        for sc in scenes:
            sid = sc.get('scene_id')
            setting = sc.get('setting') or ''
            lines.append(f"- 场景 {sid}: {setting}")
            els = sc.get('elements') or []
            for el in els:
                et = (el.get('element_type') or '').upper()
                if et == 'ACTION':
                    content = (el.get('content') or '').strip()
                    if content:
                        lines.append(f"  ACTION: {content}")
                elif et == 'DIALOGUE':
                    spk = (el.get('character') or 'UNKNOWN').upper()
                    txt = (el.get('content') or '').strip()
                    if txt:
                        lines.append(f"  DIALOGUE [{spk}]: {txt}")
        lines.append("")
        lines.append(f"对话轮次数: {len(dialogue_turns or [])}")
        return "\n".join(lines)

    def _create_dynamic_bracketed_example(self, script_flow: Dict) -> str:
        """基于 script_flow 第一场，构造方括号 STMF 的输入->输出示例，强化格式对齐。"""
        try:
            scenes = script_flow.get('scenes') or []
            if not scenes:
                return ""
            sc = scenes[0]
            sid = sc.get('scene_id')
            setting = (sc.get('setting') or 'SCENE').strip()
            # 简单规范：若未包含 INT./EXT. 与 DAY/NIGHT，由 Step0.6 已基本标准化
            out_lines: List[str] = []
            out_lines.append("示例输入（节选，自第一场）：")
            out_lines.append(f"- 场景 {sid}: {setting}")
            for el in (sc.get('elements') or [])[:8]:
                et = (el.get('element_type') or '').upper()
                content = (el.get('content') or '').strip()
                if not content:
                    continue
                if et == 'ACTION':
                    out_lines.append(f"  ACTION: {content}")
                elif et == 'DIALOGUE':
                    speaker = (el.get('character') or 'UNKNOWN').upper()
                    out_lines.append(f"  DIALOGUE [{speaker}]: {content}")

            out_lines.append("")
            out_lines.append("对应输出（方括号STMF示例，仅展示第一场）：")
            out_lines.append(f"[SCENE] {setting}")
            out_lines.append("")
            for el in (sc.get('elements') or [])[:8]:
                et = (el.get('element_type') or '').upper()
                content = (el.get('content') or '').strip()
                if not content:
                    continue
                if et == 'ACTION':
                    out_lines.append(f"[ACTION] {content}")
                elif et == 'DIALOGUE':
                    speaker = (el.get('character') or 'UNKNOWN').upper()
                    out_lines.append(f"[CHARACTER] {speaker}")
                    # 若上游已提供 parenthetical，则展示，强化示范
                    par = (el.get('parenthetical') or '').strip()
                    if par:
                        out_lines.append(f"[PAREN] ({par})")
                    out_lines.append(f"[DIALOG] {content}")
            return "\n".join(out_lines)
        except Exception:
            return ""

    def _render_from_script_flow_hollywood(self, script_flow: Dict, dialogue_turns: List[Dict], episode_id: str,
                                           model_config: Dict, use_video: bool, video_uri: Optional[str]) -> str:
        """使用 Gemini 将 script_flow 重写为好莱坞风格的方括号 STMF。"""
        
        # 尝试分段处理来解决截断问题
        return self._render_script_flow_in_chunks(script_flow, dialogue_turns, episode_id, model_config, use_video, video_uri)
    
    def _render_script_flow_in_chunks(self, script_flow: Dict, dialogue_turns: List[Dict], episode_id: str,
                                     model_config: Dict, use_video: bool, video_uri: Optional[str]) -> str:
        """分段处理 script_flow 以避免截断问题"""
        
        # 检查是否有多个场景，如果有，分别处理
        scenes = script_flow.get('scenes', [])
        if len(scenes) == 0:
            return ""
        
        all_results = []
        
        for scene in scenes:
            # 为每个场景创建单独的 script_flow
            single_scene_flow = {
                'title': script_flow.get('title', ''),
                'scenes': [scene]
            }
            
            # 处理单个场景
            scene_result = self._render_single_scene_hollywood(
                single_scene_flow, dialogue_turns, episode_id, model_config, use_video, video_uri
            )
            all_results.append(scene_result)
        
        return "\n\n".join(all_results)
    
    def _render_single_scene_hollywood(self, script_flow: Dict, dialogue_turns: List[Dict], episode_id: str,
                                      model_config: Dict, use_video: bool, video_uri: Optional[str]) -> str:
        """处理单个场景的格式化"""
        system_instruction = (
            "你是一位资深好莱坞编剧格式顾问。请将给定的简化剧本流(script_flow)重写为方括号STMF，"
            "严格使用以下标签：[EPISODE]/[SCENE]/[ACTION]/[CHARACTER]/[PAREN]/[DIALOG]/[TRANS]。"
            "**必须完整输出所有输入内容，不得截断或省略。**"
            "请严格依据输入内容进行改写，禁止杜撰或强化结尾效果。"
        )
        mode_header = (
            "【模式】视频+转写：以画面为真，并与台词一致。" if use_video else
            "【模式】纯转写编辑：仅基于 script_flow 与台词，禁止臆造画面细节。"
        )
        guidelines = (
            "【方括号STMF规范】\n"
            "- 首行输出 [EPISODE] Episode <数字>\n"
            "- 每个场景以 [SCENE] <INT./EXT. 地点 – DAY/NIGHT> 作为标题\n"
            "- 动作用 [ACTION] 简洁客观描述；不要导演指令\n"
            "- 角色对白：先 [CHARACTER] 角色名(大写)，可选 [PAREN] (情绪/方式)，再 [DIALOG] 台词\n"
            "- 场景末尾使用 [TRANS] CUT TO: 作为转场\n"
            "- 全文仅使用上述标签；避免多余解释\n"
        )

        strict_rules = (
            "【严格规则】\n"
            "1) **必须完整输出所有内容**：输入中每一个场景(scene)和元素(element)都必须完整输出，不得省略、合并或截断。\n"
            "2) 角色台词必须以两步输出：先 [CHARACTER] 角色名(全大写)，下一行 [DIALOG] 台词。\n"
            "3) **PAREN 标注**：必须完整保留输入中的 parenthetical 信息，主要用于情绪标注（如：愤怒、冷笑、颤抖、坚定等）。\n"
            "   每个对话最多只能有一个 [PAREN] 行，多个情绪用逗号分隔。\n"
            "4) 可按需要穿插 [ACTION] 行，但不得虚构超出输入的信息。\n"
            "5) 每个场景末尾追加一行 [TRANS] CUT TO:。\n"
            "6) **完整性检查**：确保输出的内容数量与输入的元素数量基本一致。\n"
            "7) **禁止结尾杜撰**：不得为了增强效果而新增或强化结尾的 [ACTION]/[TRANS]，严格忠实于输入内容。\n"
        )
        
        formatted_input = self._format_script_flow_for_prompt(script_flow, dialogue_turns)
        # 添加动态示例
        example_block = self._create_dynamic_bracketed_example(script_flow)
        
        prompt = (
            f"{mode_header}\n\n"
            f"请基于下述 script_flow 进行好莱坞风格重写并输出完整方括号STMF：\n\n"
            f"{formatted_input}\n\n"
            f"{guidelines}\n{strict_rules}\n\n"
            f"{example_block}\n\n"
            "【输出要求】只输出方括号STMF正文，不要任何额外说明。"
        )
        
        # 调试信息：检查 Prompt 长度
        prompt_chars = len(prompt)
        log.info(f"单场景 Prompt 总字符数: {prompt_chars}")
        log.info(f"单场景输入数据字符数: {len(formatted_input)}")

        schema = {"type": "STRING"}
        kwargs = {
            'model': model_config.get('model', 'gemini-2.5-pro'),
            'prompt': prompt,
            'system_instruction': system_instruction,
            'schema': schema,
            'max_tokens': model_config.get('max_tokens', 65535),
            'temperature': model_config.get('temperature', 0.3)
        }
        if use_video and video_uri:
            kwargs['video_uri'] = video_uri

        content = self.client.generate_content(**kwargs)
        # debug
        log.debug(f"单场景 LLM原始输出: {content}")
        try:
            text = str(content or '').strip()
            if not text:
                return ""
            if not text.endswith("\n"):
                text += "\n"
            
            # 调试信息：检查输出长度
            lines = text.split('\n')
            log.info(f"单场景 LLM原始输出行数: {len(lines)}")
            
            # 后处理：标准化并编号场景标题、注入[PAREN]、移除[TRANS]
            result = self._postprocess_stmf(script_flow, episode_id, text)
            result_lines = result.split('\n')
            log.info(f"单场景后处理输出行数: {len(result_lines)}")
            return result
        except Exception as e:
            log.info(f"单场景处理失败: {e}")
            return ""
    
    def _run_all_episodes(self) -> Dict[str, Any]:
        """处理所有剧集"""
        episodes = self.utils.get_episode_list(self.config.project_root)
        results = []
        
        log.info(f"开始Step0.7: 处理 {len(episodes)} 个剧集...")
        
        # 获取并行配置：步骤级 -> 全局 -> 默认 3
        step_conf_exact = self._get_this_step_config()
        if step_conf_exact and 'max_workers' in step_conf_exact:
            max_workers = step_conf_exact.get('max_workers')
        else:
            conc_conf = getattr(self.config, 'concurrency', {}) or {}
            max_workers = conc_conf.get('max_workers', 3)
        # 环境变量覆盖
        try:
            env_val = os.environ.get('STEP0_7_MAX_WORKERS')
            if env_val:
                max_workers = int(env_val)
        except Exception:
            pass
        # 类型与范围保护
        try:
            max_workers = int(max_workers)
        except Exception:
            max_workers = 3
        if not max_workers or max_workers < 1:
            max_workers = 1
        log.info(f"使用 {max_workers} 个并行线程处理...")
        
        # 使用并行处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_episode = {
                executor.submit(self._run_single_episode, ep): ep 
                for ep in episodes
            }
            
            # 处理完成的任务
            for future in tqdm(as_completed(future_to_episode), total=len(episodes), desc="Step0.7"):
                episode_id = future_to_episode[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    log.info(f"Step0.7 处理 {episode_id} 失败: {e}")
                    results.append({
                        "episode_id": episode_id, 
                        "status": "failed", 
                        "error": str(e)
                    })
        
        # 生成统计报告
        stats = self._generate_statistics(results)
        log.info(f"\n📊 Step0.7 处理完成统计:")
        log.info(f"   总剧集数: {stats['total_episodes']}")
        log.info(f"   成功处理: {stats['success_count']}")
        log.info(f"   已存在: {stats['already_exists_count']}")
        log.info(f"   失败: {stats['failed_count']}")
        log.info(f"   总场景数: {stats['total_scenes']}")
        log.info(f"   总对话数: {stats['total_dialogues']}")
        log.info(f"   总处理时间: {stats['total_processing_time']} 秒")
        log.info(f"   平均处理时间: {stats['avg_processing_time']} 秒/episode")
        log.info(f"   并行线程数: {max_workers}")
        
        return {
            "status": "completed",
            "total_episodes": len(episodes),
            "success_count": stats['success_count'],
            "already_exists_count": stats['already_exists_count'],
            "failed_count": stats['failed_count'],
            "statistics": stats,
            "results": results
        }
    
    def _generate_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """生成处理统计信息"""
        total_episodes = len(results)
        success_results = [r for r in results if r.get("status") == "success"]
        success_count = len(success_results)
        already_exists_count = len([r for r in results if r.get("status") == "already_exists"])
        failed_count = len([r for r in results if r.get("status") == "failed"])
        
        # 处理时间统计
        processing_times = [r.get("processing_time", 0) for r in success_results if r.get("processing_time")]
        total_processing_time = sum(processing_times)
        avg_processing_time = total_processing_time / len(processing_times) if processing_times else 0
        
        # 场景和对话统计
        total_scenes = sum(r.get("scenes_count", 0) for r in success_results)
        total_dialogues = sum(r.get("total_dialogues", 0) for r in success_results)
        
        return {
            "total_episodes": total_episodes,
            "success_count": success_count,
            "already_exists_count": already_exists_count,
            "failed_count": failed_count,
            "total_processing_time": round(total_processing_time, 2),
            "avg_processing_time": round(avg_processing_time, 2),
            "total_scenes": total_scenes,
            "total_dialogues": total_dialogues
        }
