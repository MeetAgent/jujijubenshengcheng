"""
Step0.6: 情节提取
基于Step0.5的融合对话分析结果，提取结构化情节信息
"""

from logging import config
import os
import re
import time
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pydantic import BaseModel, Field

from core import PipelineConfig, GenAIClient, PipelineUtils
from core.exceptions import StepDependencyError, ModelCallError
from . import PipelineStep

from new_pipeline.steps.commont_log import log

class PlotBeat(BaseModel):
    """情节节点"""
    beat_id: str
    start_turn: int
    end_turn: int
    beat_type: str  # setup, conflict, climax, resolution, transition
    description: str
    key_events: List[str]
    characters_involved: List[str]
    emotional_tone: str
    plot_impact: str  # high, medium, low

class Scene(BaseModel):
    """场景"""
    scene_id: str
    start_turn: int
    end_turn: int
    location: str
    characters: List[str]
    key_events: List[str]
    emotional_arc: str
    conflicts: List[str]
    # 新增：场景快照（客观场景描述，用于接戏）
    scene_snapshot: Optional[Dict[str, Any]] = None

class EpisodePlot(BaseModel):
    """单集情节结构"""
    episode_id: str
    title: str
    main_characters: List[str]
    scenes: List[Scene]
    plot_beats: List[PlotBeat]
    narrative_arc: str
    key_themes: List[str]
    emotional_journey: str

# ==== 简化版数据模型（面向剧本直出） ====
class SceneElement(BaseModel):
    """场景中的一个元素，可以是动作或对白"""
    element_type: str = Field(..., description="元素类型，必须是 'ACTION' 或 'DIALOGUE'")
    content: str = Field(..., description="动作描述或对白内容")
    character: Optional[str] = Field(None, description="说话的角色名，仅在 DIALOGUE 时有效")
    parenthetical: Optional[str] = Field(None, description="角色对白时的括号附注，描述语气/方式/伴随动作")
    turn_id: Optional[int] = Field(None, description="关联的原始对话轮次ID")

class SimpleScene(BaseModel):
    """一个简化的场景结构"""
    scene_id: int = Field(..., description="场景的顺序编号，从1开始")
    setting: str = Field(..., description="标准场景标题，如 'INT. 办公室 - 日' 或 '室内·白天·客厅'")
    narrative_device: Optional[str] = Field(None, description="特殊叙事手法：FLASHBACK/DREAM SEQUENCE/MONTAGE")
    elements: List[SceneElement] = Field(..., description="该场景的动作与对白序列")

class EpisodeScriptFlow(BaseModel):
    """为剧本创作准备的单集情节流"""
    episode_id: str
    title: str
    scenes: List[SimpleScene]



class Step0_6PlotExtraction(PipelineStep):
    """Step0.6: 情节提取"""
    
    @property
    def step_number(self) -> int:
        return 6
    
    @property
    def step_name(self) -> str:
        return "plot_extraction"

    def _get_this_step_config(self) -> Dict[str, Any]:
        """精确获取当前步骤在pipeline.yaml中的配置，避免与step6混淆"""
        try:
            steps = getattr(self.config, 'steps_config', None) or {}
            # 优先通过显式键
            for key in ['step0_6', '0_6']:
                if key in steps:
                    sc = steps[key]
                    if sc.get('name') == self.step_name:
                        return sc
            # 退化：遍历匹配name
            for sc in steps.values():
                if isinstance(sc, dict) and sc.get('name') == self.step_name:
                    return sc
        except Exception:
            pass
        # 最后兜底返回空字典
        return {}
    
    def check_dependencies(self, episode_id: str = None) -> bool:
        """检查依赖"""
        if episode_id:
            # 检查Step0.5的输出
            # 与单集处理时一致，检查 0_5_dialogue_turns.json
            ep_dir = self.utils.get_episode_output_dir(self.config.output_dir, episode_id)
            dialogue_turns_file = os.path.join(ep_dir, "0_5_dialogue_turns.json")
            return os.path.exists(dialogue_turns_file)
        else:
            # 检查所有剧集的Step0.5输出
            episodes = self.utils.get_episode_list(self.config.project_root)
            for ep in episodes:
                ep_dir = self.utils.get_episode_output_dir(self.config.output_dir, ep)
                dialogue_turns_file = os.path.join(ep_dir, "0_5_dialogue_turns.json")
                if not os.path.exists(dialogue_turns_file):
                    log.info(f"缺少依赖文件: {dialogue_turns_file}")
                    return False
            return True
    
    def get_output_files(self, episode_id: str = None) -> List[str]:
        """获取输出文件列表"""
        if episode_id:
            return [
                f"{episode_id}/0_6_script_flow.json",
                f"{episode_id}/0_6_script_draft.md"
            ]
        return []
    
    def run(self, episode_id: str = None) -> Dict[str, Any]:
        """运行情节提取步骤"""
        if episode_id:
            return self._run_single_episode(episode_id)
        else:
            return self._run_all_episodes()
    
    def _run_single_episode(self, episode_id: str) -> Dict[str, Any]:
        """处理单个剧集"""
        start_time = time.time()
        log.info(f"Step0.6: 处理 {episode_id}")
        
        # 检查是否已经存在输出文件（仅保留script_flow/script_draft）
        episode_out_dir = self.utils.get_episode_output_dir(self.config.output_dir, episode_id)
        json_output_file = os.path.join(episode_out_dir, "0_6_script_flow.json")
        md_output_file = os.path.join(episode_out_dir, "0_6_script_draft.md")
        
        if os.path.exists(json_output_file) and os.path.exists(md_output_file) and not os.environ.get("FORCE_OVERWRITE"):
            log.info(f"✅ {episode_id} 已有Step0.6输出文件，跳过处理")
            return {"status": "already_exists"}
        
        # 读取Step0.5结果
        episode_out_dir = self.utils.get_episode_output_dir(self.config.output_dir, episode_id)
        dialogue_turns_file = os.path.join(episode_out_dir, "0_5_dialogue_turns.json")
        
        if not os.path.exists(dialogue_turns_file):
            log.info(f"Warning: 未找到 {episode_id} 的对话轮次文件")
            return {"status": "failed", "error": "缺少对话轮次文件"}
        
        dialogue_data = self.utils.load_json_file(dialogue_turns_file, {})
        dialogue_turns = dialogue_data.get("dialogue_turns", [])
        
        if not dialogue_turns:
            log.info(f"Warning: {episode_id} 没有对话轮次数据")
            return {"status": "failed", "error": "没有对话轮次数据"}
        
        # 获取视频URI
        video_uri = self.utils.get_video_uri(episode_id, self.config.project_root)
        
        # 获取模型配置（避免与step6混淆）
        step_conf_exact = self._get_this_step_config()
        model_config = {
            'model': step_conf_exact.get('model', 'gemini-2.5-pro'),
            'max_tokens': step_conf_exact.get('max_tokens', 65535),
            'temperature': step_conf_exact.get('temperature', 0.1)
        }

        # 可选背景：全剧大纲与人物小传
        bg_outline = ""
        bg_chars = ""
        step_conf = step_conf_exact or self.config.get_step_config(6)
        try:
            if step_conf.get('use_outline', False):
                outline_path = os.path.join(self.config.output_dir, 'global', 'series_outline.md')
                if os.path.exists(outline_path):
                    bg_outline = self.utils.load_text_file(outline_path)
            if step_conf.get('use_character_bios', False):
                # 复用全局图谱中的角色摘要
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
 
        # 尝试从 Step0.2 读取“首次出场”提示
        first_appearance_names = set()
        try:
            clues_file = os.path.join(episode_out_dir, "0_2_clues.json")
            if os.path.exists(clues_file):
                clues = self.utils.load_json_file(clues_file, {})
                # 兼容多种字段结构：explicit_characters 或 characters
                for ch in (clues.get('explicit_characters') or clues.get('characters') or []):
                    try:
                        name = (ch.get('name') or ch.get('canonical_name') or '').strip()
                        fa_flag = bool(
                            ch.get('first_appearance') is True or
                            ch.get('is_first_appearance') is True or
                            (ch.get('first_appearance_episode') == episode_id if ch.get('first_appearance_episode') else False)
                        )
                        if name and fa_flag:
                            first_appearance_names.add(name)
                    except Exception:
                        continue
        except Exception:
            pass

        # 仅生成简化版剧本输出
        try:
            script_flow = self._extract_script_flow(
                dialogue_turns, video_uri, model_config, episode_id,
                first_appearance_names=first_appearance_names
            )
            self.utils.save_json_file(json_output_file, script_flow.dict())
            script_md = self._generate_script_markdown(script_flow)
            self.utils.save_text_file(md_output_file, script_md)

            processing_time = time.time() - start_time
            log.info(f"✅ {episode_id} Step0.6 完成 (用时: {processing_time:.2f}秒)")

            return {
                "status": "success",
                "processing_time": round(processing_time, 2),
                "scenes_count": len(script_flow.scenes or []),
                "beats_count": 0,
                "characters_count": 0
            }

        except Exception as e:
            log.exception(f"❌ {episode_id} Step0.6 失败: {e}")
            return {"status": "failed", "error": str(e)}
    
    # (已移除未使用的情节结构提取大Prompt与对应验证方法)

    def _format_dialogue_turns(self, dialogue_turns: List[Dict]) -> str:
        """格式化对话轮次数据"""
        formatted = []
        for turn in dialogue_turns:
            formatted.append(f"轮次 {turn['turn_id']}: [{turn['speaker']}] {turn['full_dialogue']}")
            formatted.append(f"  情感: {turn['emotion']}, 意图: {turn['intent']}")
            formatted.append("")
        return "\n".join(formatted)

    # ==== 简化版生成：LLM转写为剧本事件流 ====
    def _extract_script_flow(self, dialogue_turns: List[Dict], video_uri: str,
                             model_config: Dict, episode_id: str,
                             first_appearance_names: Optional[set] = None) -> EpisodeScriptFlow:
        
        """使用LLM提取面向剧本的场景事件流（增强：情感化ACTION与对白parenthetical）。"""
        # ========== 在这里添加：读取 video_fps 配置 ==========
        step_conf = self._get_this_step_config()
        video_fps = step_conf.get('video_fps', 1)  # 如果未配置则为 1fps.
        # =====================================================
        system_instruction = (
            "你是一位专业的剧本作家（Screenwriter）。你的任务是将视频和带有说话人标识的对话稿，"
            "转换成一个清晰、线性的剧本草稿（JSON）。必须严格基于【视频画面/已有台词】证据生成：\n"
            "- ACTION 行需生动但客观，不写镜头调度；禁止新增画面/道具/地点/车辆/声效；\n"
            "- PAREN（括号附注）遵循'非必要，勿添加'；仅为消歧语气或关键微动作；\n"
            "- 场景标题使用 INT./EXT. 地点 – DAY/NIGHT 标准；若为闪回/梦境/蒙太奇，在 narrative_device 中注明；\n"
            "- 语言要求：输出以中文为主，专有名词除外；\n"
            "- 证据要求：每条 ACTION 必须附 evidence（来源：video 或 turn_id 列表）；若无证据，改写为\"无明显动作\"而非臆造。\n"
        )

        user_prompt = f"""
【输入】
1) 视频：video_uri 将提供视觉参照（场景与动作的主要依据）
2) 对话轮次（含说话人）：
```
{self._format_dialogue_turns_for_prompt(dialogue_turns)}
```

【任务】
将内容转写为线性的剧本事件流。以场景为单位（setting），场景内部按时间顺序组织 elements，仅包含两类：
- ACTION：简洁但生动地描述可见动作/表情/互动/道具/走位（不做镜头指导），可带情绪色彩以增强戏剧性；
- DIALOGUE：保留说话人与必要对白，可轻微润色保证口语自然；尽可能保留 turn_id 以利后续对齐。

【语言要求】
- 所有输出内容必须以中文为主，包括 ACTION 描述、对话内容、场景标题等
- 仅在以下情况使用英文：英文人名（如 RHETT）、英文地名、品牌名、专业术语等专有名词
- 场景标题中的地点名称优先使用中文，如"INT. 办公室 - DAY"而非"INT. Office - DAY"

【网络短剧风格的 PAREN（括号附注）】
- 核心原则：服务于快节奏、强冲突、情绪化的表达，仍遵循"非必要，勿添加"。
- 何时添加：
  - 消除语气歧义（如 真诚/讽刺/命令的口吻/挑衅地）。
  - 对剧情关键的微表情/微动作（如 瞳孔地震/倒吸一口凉气/气场全开/轻蔑一笑/一字一顿地）。
  - 多人场景需明确对象（如 对顾向晴说）。
  - **情绪标注**：为每个有强烈情绪表现、表情变化或特殊语调的对话添加 [PAREN] 标注，如：(愤怒)、(冷笑)、(颤抖)、(坚定)等。每个对话最多只能有一个 [PAREN] 行，多个情绪用逗号分隔，如：(轻蔑地，得意地)。
- 何时禁止：已有邻近 ACTION 清楚表达情绪；不得写表演指令；避免冗余修饰堆砌。
- 输出格式：parenthetical 字段仅填精炼中文短语，不含括号与引号（括号由下游渲染）。情绪类≤16字。

【Casting Bio（首次出场专用）】
- 定义：按好莱坞规范，在角色首次出现在对白中时，于该角色第一条 DIALOGUE 之前的 ACTION 中写入简洁 Casting Bio，格式为"角色名（年龄，essence）"。
- essence 可来源：外貌特征、职位/头衔/外号、服装风格或能代表角色特色的要素；可借鉴屏幕人名条/下三分之一字幕的关键信息，但禁止在画面描述中写"屏幕上出现字幕/金色大字"。
- 示例：ACTION 中写"律师秦正疏（40多岁，律界阎王，自信稳重）站在法庭中央" / "助理易菲（20多岁，职场新人，紧张）跟在身后"
- 限制：仅首次对白前的 ACTION 添加，后续对白的 parenthetical 仍用于情绪/语气；若信息不足，可只给出年龄+1项最能代表角色的 essence。

【动作连续性与衔接规则】
- 同一说话人的连续两段台词之间：必须插入该说话人的微动作/表情/眼神/身体姿态变化；若视频中确无动作，则将相邻两句合并为一句，避免出现"无明显动作"。
- 不同说话人台词衔接处：必须给出"反应/转身/对视/后退/伸手"等互动性动作或情绪反应；若视频中确无反应，则合并为连续对话。
- 离场/进场/擦肩而过/起立/落座/扬手欲打等过渡性关键动作必须记录。

【动作精度与视频证据绑定】
- 动作精度：严格区分"拍打手臂/推搡/扇耳光/捶打"等强度和部位，避免泛化为"打"。
- 视频证据：每条 ACTION 必须严格基于视频画面，不得臆造车辆、道具、地点等未出现元素。
- 不确定处理：若画面不清晰，仅可使用降级词汇（如"似乎…/欲…/抬手未落"）并在 evidence.confidence 标注为 "low"。
- 禁止"补戏"：不得新增车辆启停、追车、门牌地点、未出现的道具等。

【关键动作必检清单（法院/冲突场景优先）】
- 庭审礼仪：起立、宣判时全体起立、坐下。
- 进出与结束：转身离开、与他人擦肩而过、回头、停步。
- 冲突前兆：扬手欲打、上前逼近、后退躲避、对视僵持、拍桌、指向对方。

【人物首次出场（仅限出现人名条/字幕条/Title Card/下三分之一字幕时）】
- 触发条件：屏幕出现"人名条/介绍卡"。
- 执行：仅在触发当下的相邻 ACTION 文本中添加一次"首次出场"描述，并在该元素的 introductions[] 中追加 {{name, description, is_first_appearance:true}}：
  - 若角色为英文名（如 Rhett）→ 名字必须全大写（RHETT）。
  - 若角色为中文名（如 顾向晴）→ 名字保持原样，禁止大写或加拼音。
  - 名字后紧跟括号，内含年龄（可合理猜测，如"20多岁"）与简短鲜明特征/职业（如"戴耳钉，身材健硕/外科医生/助理"）。
- 示例：ACTION content 可写为"一个男人，RHETT (20多岁，戴着耳钉，身材健硕)，逆光走来。" introductions 写入 [{{"name":"RHETT","description":"(20多岁，戴着耳钉，身材健硕)", "is_first_appearance": true}}]。
- 禁止：普通路人或无介绍卡的角色不要添加。
- 重要：不要描述"屏幕上浮现出金色大字"或"屏幕上出现字幕"等画面元素，直接将职位/身份信息整合到人物描述中。

【叙事手法】
- 识别 FLASHBACK / DREAM SEQUENCE / MONTAGE 等，若发生则在 narrative_device 字段标注相应值。

【场景标题要求】
- setting 必须为 "INT./EXT. 地点 – DAY/NIGHT"。若依据不足，请合理猜测 INT./EXT. 与 DAY/NIGHT。

【剧集ID】{episode_id}

请严格输出符合下述JSON Schema的对象。
"""

        schema = {
            "type": "OBJECT",
            "properties": {
                "episode_id": {"type": "STRING"},
                "title": {"type": "STRING"},
                "scenes": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "scene_id": {"type": "INTEGER"},
                            "setting": {"type": "STRING"},
                            "narrative_device": {"type": "STRING", "enum": ["FLASHBACK", "DREAM SEQUENCE", "MONTAGE"]},
                            "elements": {
                                "type": "ARRAY",
                                "items": {
                                    "type": "OBJECT",
                                    "properties": {
                                        "element_type": {"type": "STRING", "enum": ["ACTION", "DIALOGUE"]},
                                        "content": {"type": "STRING"},
                                        "introductions": {
                                            "type": "ARRAY",
                                            "items": {
                                                "type": "OBJECT",
                                                "properties": {
                                                    "name": {"type": "STRING"},
                                                    "description": {"type": "STRING"},
                                                    "is_first_appearance": {"type": "BOOLEAN"}
                                                },
                                                "required": ["name", "description"]
                                            }
                                        },
                                        "character": {"type": "STRING"},
                                        "parenthetical": {"type": "STRING"},
                                        "turn_id": {"type": "INTEGER"},
                                        "evidence": {
                                            "type": "OBJECT",
                                            "properties": {
                                                "source": {"type": "STRING", "enum": ["video", "dialogue"]},
                                                "turn_ids": {"type": "ARRAY", "items": {"type": "INTEGER"}},
                                                "time_hint": {"type": "STRING"},
                                                "confidence": {"type": "STRING", "enum": ["high", "medium", "low"]}
                                            },
                                            "required": ["source", "confidence"]
                                        }
                                    },
                                    "required": ["element_type", "content"]
                                }
                            }
                        },
                        "required": ["scene_id", "setting", "elements"]
                    }
                }
            },
            "required": ["episode_id", "title", "scenes"]
        }

        result = self.client.generate_content(
            model=model_config.get('model', 'gemini-2.5-pro'),
            prompt=user_prompt,
            video_uri=video_uri,
            system_instruction=system_instruction,
            schema=schema,
            max_tokens=model_config.get('max_tokens', 65535),
            temperature=model_config.get('temperature', 0.1),
            video_fps=video_fps
        )

        # debug, 记录LLM原始输出
        log.debug(f"LLM raw result for {episode_id}: {result}")

        return self._normalize_script_flow(result, episode_id)

    def _format_dialogue_turns_for_prompt(self, dialogue_turns: List[Dict]) -> str:
        """为Prompt格式化对话轮次数据（精简）。"""
        return "\n".join([
            f"轮次 {turn.get('turn_id')}: [{turn.get('speaker')}] {turn.get('full_dialogue')}" for turn in dialogue_turns
        ])

    def _normalize_script_flow(self, raw: Dict, episode_id: str,
                               first_appearance_names: Optional[set] = None) -> EpisodeScriptFlow:
        """清洗LLM返回，保证结构健壮，字段兜底。"""
        safe: Dict[str, Any] = {}
        safe["episode_id"] = episode_id or (raw.get("episode_id") if isinstance(raw, dict) else "")
        title = ""
        if isinstance(raw, dict):
            title = (raw.get("title") or "").strip()
        if not title:
            title = f"{episode_id} 剧本草稿"
        safe["title"] = title

        scenes_in = (raw.get("scenes") if isinstance(raw, dict) else None) or []
        normalized_scenes: List[Dict[str, Any]] = []
        next_id = 1
        # 跨场景的首次出场去重：仅保留每个角色第一次 is_first_appearance=True 的 introductions
        introduced_names: set = set()
        fa_names = set(first_appearance_names or [])
        for sc in scenes_in:
            try:
                setting = (sc.get("setting") or "").strip()
                narrative_device = sc.get("narrative_device")
                elements_in = sc.get("elements") or []
                norm_elements: List[Dict[str, Any]] = []
                for el in elements_in:
                    et = (el.get("element_type") or "").upper().strip()
                    if et not in {"ACTION", "DIALOGUE"}:
                        continue
                    content = (el.get("content") or "").strip()
                    if not content:
                        continue
                    # 规范 parenthetical：去括号/引号/首尾空格，限长，空则置为 None
                    _p = (el.get("parenthetical") or "").strip()
                    if _p:
                        if (_p.startswith("(") and _p.endswith(")")) or (_p.startswith("（") and _p.endswith("）")):
                            _p = _p[1:-1].strip()
                        _p = _p.strip('"“”')
                        _p = " ".join(_p.split())
                        # 情绪类 parenthetical 保持 16 字限制
                        if len(_p) > 16:
                            _p = _p[:16]
                    parenthetical = _p or None
                    # 首次出场 introductions 过滤：
                    # 1) 若上游标注 is_first_appearance=True 且未引入过，则保留
                    # 2) 若未标注，但角色在 Step0.2 提供的 first_appearance_names 中，且未引入过，也允许保留
                    introductions_in = el.get("introductions") or []
                    introductions: List[Dict[str, Any]] = []
                    for intro in introductions_in:
                        try:
                            iname = (intro.get("name") or "").strip()
                            idesc = (intro.get("description") or "").strip()
                            if not iname or not idesc:
                                continue
                            is_first = bool(intro.get("is_first_appearance") is True)
                            allow = False
                            if is_first:
                                allow = True
                            elif iname and (iname in fa_names):
                                allow = True
                            if allow and iname not in introduced_names:
                                intro_obj = {"name": iname, "description": idesc, "is_first_appearance": True}
                                introductions.append(intro_obj)
                                introduced_names.add(iname)
                        except Exception:
                            continue
                    norm_el: Dict[str, Any] = {
                        "element_type": et,
                        "content": content,
                        "character": (el.get("character") or (None if et == "ACTION" else "UNKNOWN")),
                        "turn_id": el.get("turn_id")
                    }
                    if et == "DIALOGUE":
                        norm_el["parenthetical"] = parenthetical
                    if introductions:
                        norm_el["introductions"] = introductions
                    norm_elements.append(norm_el)
                if setting and norm_elements:
                    normalized_scenes.append({
                        "scene_id": next_id,
                        "setting": setting,
                        "narrative_device": (narrative_device if narrative_device in {"FLASHBACK", "DREAM SEQUENCE", "MONTAGE"} else None),
                        "elements": norm_elements 
                    })
                    next_id += 1
            except Exception:
                continue

        safe["scenes"] = normalized_scenes
        return EpisodeScriptFlow(**safe)

    def _get_simple_elements_cap(self) -> int:
        """每场景元素数量上限，避免超长输出。"""
        try:
            step_conf_exact = self._get_this_step_config() or {}
            simple_conf = step_conf_exact.get('simple', {}) if isinstance(step_conf_exact, dict) else {}
            cap = int(simple_conf.get('max_elements_per_scene', 80))
            return cap if cap > 0 else 80
        except Exception:
            return 80

    def _generate_script_markdown(self, script_flow: EpisodeScriptFlow) -> str:
        """从简化的JSON结果生成可读的Markdown剧本草稿。"""
        lines: List[str] = [f"# {script_flow.title}\n"]
        for scene in sorted(script_flow.scenes or [], key=lambda s: s.scene_id):
            heading = self._format_scene_heading(scene.setting or "SCENE")
            lines.append(f"## {heading}")
            lines.append("")
            for element in scene.elements or []:
                if element.element_type == "ACTION":
                    if element.content:
                        lines.append(element.content)
                        lines.append("")
                elif element.element_type == "DIALOGUE":
                    speaker = (element.character or "").strip() or "UNKNOWN"
                    lines.append(f"**{speaker}**")
                    lines.append(element.content or "")
                    lines.append("")
        return "\n".join(lines)

    def _format_scene_heading(self, raw: str) -> str:
        """将场景标题标准化为 "INT./EXT. 地点 - DAY/NIGHT" 风格。
        - 识别中文/英文的室内/外与日/夜关键词
        - 保留地点描述
        - 若已是标准格式则原样返回
        """
        try:
            text = (raw or "").strip()
            if not text:
                return "SCENE"

            # 已是典型标准格式
            if re.match(r"^(INT|EXT|INT/EXT)\.\s+.+\s+-\s+(DAY|NIGHT|DAWN|DUSK)$", text, re.IGNORECASE):
                return text

            lower = text.lower()

            # 室内/室外判定
            if any(k in lower for k in ["int.", "内景", "室内", "indoor"]):
                ie = "INT."
            elif any(k in lower for k in ["ext.", "外景", "室外", "outdoor"]):
                ie = "EXT."
            else:
                ie = "INT."

            # 昼夜判定
            if any(k in lower for k in ["白天", "日", "day"]):
                tod = "DAY"
            elif any(k in lower for k in ["夜晚", "夜", "晚上", "night"]):
                tod = "NIGHT"
            elif any(k in lower for k in ["黄昏", "傍晚", "dusk"]):
                tod = "DUSK"
            elif any(k in lower for k in ["清晨", "黎明", "dawn"]):
                tod = "DAWN"
            else:
                tod = "DAY"

            # 提取地点：去掉已识别关键词后的余量
            # 常见分隔符处理
            cleaned = re.sub(r"^(int\.|ext\.|int/ext\.|内景|外景|室内|室外)\s*[:：·,，-]*\s*", "", text, flags=re.IGNORECASE)
            cleaned = re.sub(r"[\-–—]\s*(day|night|dawn|dusk|白天|夜晚|夜|日|晚上|清晨|黎明|黄昏|傍晚)\s*$", "", cleaned, flags=re.IGNORECASE)
            cleaned = cleaned.strip().strip("-·:：")
            if not cleaned:
                cleaned = "LOCATION"

            return f"{ie} {cleaned} - {tod}"
        except Exception:
            return raw or "SCENE"
    
    # (已移除未使用的情节结构提取大Prompt与对应验证方法)
    
    def _generate_plot_summary(self, plot_result: EpisodePlot) -> str:
        """生成情节摘要"""
        summary = f"""# {plot_result.title} - 情节结构分析

## 基本信息
- **剧集ID**: {plot_result.episode_id}
- **主要角色**: {', '.join(plot_result.main_characters)}
- **场景数量**: {len(plot_result.scenes)}
- **情节节点数量**: {len(plot_result.plot_beats)}

## 叙事弧线
{plot_result.narrative_arc}

## 关键主题
{chr(10).join(f"- {theme}" for theme in plot_result.key_themes)}

## 情感旅程
{plot_result.emotional_journey}

## 场景分析
"""
        
        for scene in plot_result.scenes:
            summary += f"""
### 场景 {scene.scene_id}
- **位置**: {scene.location}
- **轮次范围**: {scene.start_turn} - {scene.end_turn}
- **参与角色**: {', '.join(scene.characters)}
- **关键事件**: {chr(10).join(f"  - {event}" for event in scene.key_events)}
- **情感弧线**: {scene.emotional_arc}
- **冲突**: {chr(10).join(f"  - {conflict}" for conflict in scene.conflicts)}
"""
        
        summary += "\n## 情节节点分析\n"
        
        for beat in plot_result.plot_beats:
            summary += f"""
### 节点 {beat.beat_id} ({beat.beat_type})
- **轮次范围**: {beat.start_turn} - {beat.end_turn}
- **描述**: {beat.description}
- **关键事件**: {chr(10).join(f"  - {event}" for event in beat.key_events)}
- **参与角色**: {', '.join(beat.characters_involved)}
- **情感基调**: {beat.emotional_tone}
- **情节影响**: {beat.plot_impact}
"""
        
        return summary

    def _generate_narrative(self, plot_result: EpisodePlot) -> str:
        """生成按[情节N]分段的叙事文本：视觉优先+台词融合（概述/引用），顺序以plot_beats为准；无beats时按scenes顺序。"""
        lines: List[str] = []
        lines.append(f"# {plot_result.title} 叙事段落\n")
        # 构造按起止turn排序的beats
        beats = list(plot_result.plot_beats or [])
        beats.sort(key=lambda b: (b.start_turn if b.start_turn is not None else 10**9,
                                  b.end_turn if b.end_turn is not None else 10**9))
        if not beats:
            # 无beats，用scenes兜底
            for idx, sc in enumerate(plot_result.scenes or [], start=1):
                loc = sc.location or "场景"
                rng = f"{sc.start_turn}-{sc.end_turn}" if sc.start_turn is not None else ""
                key_events = (sc.key_events or [])
                emo = sc.emotional_arc or ""
                lines.append(f"[情节{idx}] 场景：{loc} {('（轮次'+rng+'）' if rng else '')}")
                # 视觉动作
                snap = sc.scene_snapshot or {}
                acts = (snap.get('actions') or [])
                if acts:
                    lines.append("- 关键动作：" + "；".join(acts))
                if key_events:
                    lines.append("- 事件：" + "；".join(key_events))
                if emo:
                    lines.append(f"- 氛围/情绪：{emo}")
                lines.append("")
            return "\n".join(lines).strip() + "\n"
        # 有beats：按beats渲染
        for i, bt in enumerate(beats, start=1):
            desc = (bt.description or '').strip()
            key_events = (bt.key_events or [])
            tone = (bt.emotional_tone or '').strip()
            lines.append(f"[情节{i}] {desc if desc else '（情节概述缺失）'}")
            if key_events:
                lines.append("- 事件：" + "；".join(key_events))
            if tone:
                lines.append(f"- 情绪/语气：{tone}")
            # 就近场景快照的动作/环境
            # 找包含该beat的scene
            scene = None
            for sc in plot_result.scenes or []:
                if (sc.start_turn is not None and sc.end_turn is not None and
                    bt.start_turn is not None and bt.end_turn is not None and
                    sc.start_turn <= bt.start_turn <= sc.end_turn):
                    scene = sc
                    break
            if scene:
                snap = scene.scene_snapshot or {}
                locb = (snap.get('location_brief') or scene.location or '').strip()
                if locb:
                    lines.append(f"- 场景：{locb}")
                acts = (snap.get('actions') or [])
                if acts:
                    lines.append("- 动作：" + "；".join(acts))
                props = (snap.get('props') or [])
                if props:
                    lines.append("- 道具：" + "、".join(props))
            lines.append("")
        return "\n".join(lines).strip() + "\n"

    def _generate_index(self, plot_result: EpisodePlot) -> Dict[str, Any]:
        """生成极简索引，供Step0.7精准贴台词与生成场景标题。"""
        scenes_idx = []
        for sc in plot_result.scenes or []:
            snap = sc.scene_snapshot or {}
            scenes_idx.append({
                "scene_id": sc.scene_id,
                "start_turn": sc.start_turn,
                "end_turn": sc.end_turn,
                "heading_hint": (snap.get('location_brief') or sc.location or '').strip(),
                "tod_hint": (sc.emotional_arc or ''),
                "location_hint": sc.location or ''
            })
        beats_idx = []
        for bt in plot_result.plot_beats or []:
            beats_idx.append({
                "beat_id": bt.beat_id,
                "order": bt.start_turn if bt.start_turn is not None else 10**9,
                "start_turn": bt.start_turn,
                "end_turn": bt.end_turn,
                "scene_id": None,
                "key_actions": (bt.key_events or []),
                "key_quotes": [],
                "tone_hint": bt.emotional_tone or ''
            })
        beats_idx.sort(key=lambda x: (x["order"] if x["order"] is not None else 10**9))
        return {
            "episode_id": plot_result.episode_id,
            "scenes": scenes_idx,
            "beats": beats_idx
        }
    
    def _run_all_episodes(self) -> Dict[str, Any]:
        """处理所有剧集"""
        episodes = self.utils.get_episode_list(self.config.project_root)
        results = []
        
        log.info(f"开始Step0.6: 处理 {len(episodes)} 个剧集...")
        
        # 获取并行配置：步骤级 -> 全局 -> 默认 3
        step_conf_exact = self._get_this_step_config()
        if step_conf_exact and 'max_workers' in step_conf_exact:
            max_workers = step_conf_exact.get('max_workers')
        else:
            conc_conf = getattr(self.config, 'concurrency', {}) or {}
            max_workers = conc_conf.get('max_workers', 3)
        # 环境变量覆盖
        try:
            env_val = os.environ.get('STEP0_6_MAX_WORKERS')
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
            for future in tqdm(as_completed(future_to_episode), total=len(episodes), desc="Step0.6"):
                episode_id = future_to_episode[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    log.info(f"Step0.6 处理 {episode_id} 失败: {e}")
                    results.append({
                        "episode_id": episode_id, 
                        "status": "failed", 
                        "error": str(e)
                    })
        
        # 生成统计报告
        stats = self._generate_statistics(results)
        log.info("\n📊 Step0.6 处理完成统计:")
        log.info(f"   总剧集数: {stats['total_episodes']}")
        log.info(f"   成功处理: {stats['success_count']}")
        log.info(f"   已存在: {stats['already_exists_count']}")
        log.info(f"   失败: {stats['failed_count']}")
        log.info(f"   总场景数: {stats['total_scenes']}")
        log.info(f"   总情节节点数: {stats['total_beats']}")
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
        
        # 场景和情节节点统计
        total_scenes = sum(r.get("scenes_count", 0) for r in success_results)
        total_beats = sum(r.get("beats_count", 0) for r in success_results)
        
        return {
            "total_episodes": total_episodes,
            "success_count": success_count,
            "already_exists_count": already_exists_count,
            "failed_count": failed_count,
            "total_processing_time": round(total_processing_time, 2),
            "avg_processing_time": round(avg_processing_time, 2),
            "total_scenes": total_scenes,
            "total_beats": total_beats
        }
