import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import PipelineStep
from typing import Dict, List, Any
import json
from google import genai
from google.genai import types as gat
from google.oauth2 import service_account
from new_pipeline.steps.commont_log import log


class Step0_3GlobalAlignmentLLM(PipelineStep):
    """使用LLM进行全局实体对齐与关系构建 - 直接API调用版"""

    def __init__(self, config):
        super().__init__(config)
        self._step_name = "Step0.3-LLM"
        self._description = "全局实体对齐（LLM）"
        
        # 直接初始化Google GenAI客户端，绕过client.py
        gcp_config = config.gcp_config
        self.genai_client = genai.Client(
            vertexai=True,
            project=gcp_config['project_id'],
            location=gcp_config['location'],
            credentials=service_account.Credentials.from_service_account_file(
                gcp_config['credentials_path'],
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
        )

    @property
    def step_number(self) -> int:
        return 0.31

    @property
    def step_name(self) -> str:
        return self._step_name

    @property
    def description(self) -> str:
        return self._description

    def run(self) -> Dict[str, Any]:
        # 使用真实数据的版本
        log.info("🔍 使用真实剧集数据的step0.3：直接调用Google GenAI API")
        
        # 1. 读取预处理数据（从配置的输出根目录获取，而非硬编码）
        output_root = self.config.project_root or self.config.output_dir
        preprocessed_file = os.path.join(output_root, 'global', '0_3_preprocessed_data.json')
        
        if not os.path.exists(preprocessed_file):
            log.info("❌ 预处理数据文件不存在，正在自动聚合生成...")
            base_dir = output_root
            import glob
            clues = sorted(glob.glob(os.path.join(base_dir, 'episode_*', '0_2_clues.json')))
            episodes = []
            for fp in clues:
                try:
                    with open(fp, 'r', encoding='utf-8') as f:
                        j = json.load(f)
                        ep = os.path.basename(os.path.dirname(fp))
                        episodes.append({
                            'episode_id': ep,
                            'data': j
                        })
                except Exception as e:
                    log.info(f"⚠️ 读取线索失败 {fp}: {e}")
                continue
            os.makedirs(os.path.dirname(preprocessed_file), exist_ok=True)
            with open(preprocessed_file, 'w', encoding='utf-8') as f:
                json.dump({'episodes': episodes}, f, ensure_ascii=False, indent=2)
            log.info(f"✅ 已生成预处理文件: {preprocessed_file}，共 {len(episodes)} 集")
        
        with open(preprocessed_file, 'r', encoding='utf-8') as f:
            preprocessed_data = json.load(f)
        
        log.info(f"✅ 读取预处理数据：{len(preprocessed_data.get('episodes', []))} 个剧集")
        
        # 2. 构建真实的prompt
        real_prompt = self._build_real_prompt(preprocessed_data)
        
        # 增强版 schema：在兼容基础上增加 is_high_status、supporting、结构化 hints
        simple_schema = {
            'type': 'OBJECT',
            'properties': {
                'characters': {
                    'type': 'ARRAY',
                    'items': {
                        'type': 'OBJECT',
                        'properties': {
                            'name': {'type': 'STRING'},
                            'description': {'type': 'STRING'},
                            'first_appearance_episode': {'type': 'STRING'},
                            'first_appearance_evidence': {'type': 'STRING'},
                            'is_high_status': {'type': 'BOOLEAN'},
                            'aliases': {'type': 'ARRAY', 'items': {'type': 'STRING'}}
                        }
                    }
                },
                'relationships': {
                    'type': 'ARRAY',
                    'items': {
                        'type': 'OBJECT',
                        'properties': {
                            'participants': {'type': 'ARRAY', 'items': {'type': 'STRING'}},
                            'relationship_type': {'type': 'STRING'},
                            'evidence': {'type': 'STRING'}
                        }
                    }
                },
                'per_episode': {
                    'type': 'ARRAY',
                    'items': {
                        'type': 'OBJECT',
                        'properties': {
                            'episode_id': {'type': 'STRING'},
                            'supporting': {
                                'type': 'ARRAY',
                                'items': {
                                    'type': 'OBJECT',
                                    'properties': {
                                        'label': {'type': 'STRING'},
                                        'title_or_role': {'type': 'STRING'},
                                        'affiliation_owner': {'type': 'STRING'},
                                        'affiliation_type': {'type': 'STRING'},
                                        'episode_traits': {'type': 'STRING'}
                                    }
                                }
                            },
                            'speaker_alignment_hints': {
                                'type': 'ARRAY',
                                'items': {
                                    'type': 'OBJECT',
                                    'properties': {
                                        'spk_full_id': {'type': 'STRING'},
                                        'candidate': {'type': 'STRING'},
                                        'confidence': {'type': 'NUMBER'},
                                        'sticky': {'type': 'BOOLEAN'},
                                        'role_type': {'type': 'STRING'},
                                        'affiliation_owner': {'type': 'STRING'},
                                        'rationale': {'type': 'STRING'}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        try:
            # 根据Context7文档修正的API调用方式
            raw_response = self.genai_client.models.generate_content(
                model="gemini-2.5-pro",
                contents=real_prompt,
                config=gat.GenerateContentConfig(
                    system_instruction=(
                        "你是一位顶级的角色关系分析专家。上游线索可能包含错误/不一致/遗漏。" \
                        "你的任务：构建全剧统一且可用于后续校准的全球人物图谱，并为每集生成可执行的 speaker_alignment_hints。" \
                        "\n严格规则：\n" \
                        "1) 证据优先级：人名条/明确字幕 > 跨集视觉一致性 > 稳定关系网络 > 语义/话语风格。避免过度合并，仅在多条独立证据一致时才合并别名/身份。\n" \
                        "2) 角色独立性：功能性配角（护卫/助理/司机等）如证据表明是独立个体，应保持独立。不可因称谓（小姐/总）而改写 display_name。\n" \
                        "3) 高地位约束：具有人名条+头衔且证据充足者视为高地位角色（characters[].is_high_status=true），不得降级为从属。\n" \
                        "4) 语用原则：严格区分称呼（address）与提及（mention）。address 表示当前对话对象；mention 不建立从属或直接关系。\n" \
                        "5) spk 标准：speaker_alignment_hints[].spk_full_id 必须为 spk_X_epN（如 spk_0_ep1）。\n" \
                        "6) 首次出现：给出 first_appearance_episode 与最具代表性的 first_appearance_evidence（一句话证据）。\n" \
                        "\n输出要求（严格JSON，无解释）：\n" \
                        "- characters[]: name, description, first_appearance_episode, first_appearance_evidence, is_high_status(boolean), aliases[]\n" \
                        "- relationships[]: participants[], relationship_type, evidence\n" \
                        "- per_episode[]: episode_id, supporting[]({label,title_or_role,affiliation_owner,affiliation_type,episode_traits}),\n" \
                        "  speaker_alignment_hints[]({spk_full_id,candidate,confidence(0-1),sticky,role_type,affiliation_owner,rationale})\n"
                    ),
                    max_output_tokens=65535,  # 使用最大token限制
                    temperature=0.1,
                    response_mime_type="application/json",
                    response_schema=simple_schema
                )
            )
            
            log.info(f"📥 收到原始响应，类型: {type(raw_response)}")
            # 增加日志打印全部的响应内容，用于调试。这个地方使用debug模式打印
            log.debug(f"原始响应内容: {raw_response}")
            
            # 直接解析响应
            if hasattr(raw_response, 'text') and raw_response.text:
                try:
                    result = json.loads(raw_response.text)
                    log.info(f"✅ JSON解析成功：{len(result.get('characters', []))} 个角色")
                except json.JSONDecodeError as e:
                    log.info(f"⚠️ JSON解析失败: {e}")
                    log.info(f"原始文本: {raw_response.text[:500]}...")
                    result = {
                        "characters": [],
                        "relationships": [],
                        "per_episode": []
                    }
            else:
                log.info("❌ 响应没有text内容")
                result = {
                    "characters": [],
                    "relationships": [],
                    "per_episode": []
                }
            
        except Exception as e:
            log.info(f"❌ 直接API调用失败: {e}")
            result = {
                "characters": [],
                "relationships": [],
                "per_episode": []
            }
        
        # 结果规范化，确保字段与后续步骤兼容
        result = self._normalize_result(result)

        # debug: 打印规范化后的结果
        log.debug(f"规范化后的结果: {result}")
        
        # 保存结果
        global_dir = os.path.join(output_root, 'global')
        os.makedirs(global_dir, exist_ok=True)
        output_file = os.path.join(global_dir, 'global_character_graph_llm.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        # 额外导出：每集 speaker 提示 CSV（便于人工审阅/手改），以及全局角色 CSV
        try:
            import csv
            # 1) 全局角色 CSV
            global_char_csv = os.path.join(global_dir, 'global_characters.csv')
            with open(global_char_csv, 'w', encoding='utf-8', newline='') as cf:
                writer = csv.DictWriter(cf, fieldnames=[
                    'canonical_name', 'aliases', 'traits', 'is_high_status',
                    'first_appearance_episode', 'first_appearance_evidence'
                ])
                writer.writeheader()
                for ch in result.get('characters', []) or []:
                    writer.writerow({
                        'canonical_name': ch.get('canonical_name') or ch.get('name') or '',
                        'aliases': ','.join(ch.get('aliases') or []),
                        'traits': ch.get('traits') or ch.get('description') or '',
                        'is_high_status': 'true' if ch.get('is_high_status') else 'false',
                        'first_appearance_episode': ch.get('first_appearance_episode') or '',
                        'first_appearance_evidence': ch.get('first_appearance_evidence') or ''
                    })
            # 2) 分集 speaker 提示 CSV
            for pe in result.get('per_episode', []) or []:
                ep_id = pe.get('episode_id') or ''
                if not ep_id:
                    continue
                ep_dir = os.path.join(output_root, ep_id)
                os.makedirs(ep_dir, exist_ok=True)
                ep_csv = os.path.join(ep_dir, '0_3_speaker_hints.csv')
                with open(ep_csv, 'w', encoding='utf-8', newline='') as ef:
                    writer = csv.DictWriter(ef, fieldnames=[
                        'spk_full_id', 'candidate', 'confidence', 'sticky',
                        'role_type', 'affiliation_owner', 'evidence'
                    ])
                    writer.writeheader()
                    for h in pe.get('speaker_alignment_hints') or []:
                        writer.writerow({
                            'spk_full_id': h.get('spk_full_id') or '',
                            'candidate': h.get('candidate') or '',
                            'confidence': h.get('confidence') or 0,
                            'sticky': 'true' if h.get('sticky') else 'false',
                            'role_type': h.get('role_type') or '',
                            'affiliation_owner': h.get('affiliation_owner') or '',
                            'evidence': h.get('rationale') or ''
                        })
        except Exception as e:
            log.info(f"⚠️ 导出CSV时出现问题: {e}")
        
        log.info(f"✅ {self.step_name} 完成: {output_file}")

        return {
            "status": "completed",
            "output_file": output_file,
            "characters_count": len(result.get('characters', [])),
            "relationships_count": len(result.get('relationships', [])),
        }

    def _build_real_prompt(self, preprocessed_data: Dict[str, Any]) -> str:
        """构建使用真实数据的prompt（覆盖全剧集，简洁SI+UP）"""
        lines: List[str] = []
        
        # 概要与目标
        lines.append("请基于以下分集线索，完成全局人物对齐与关系构建，并输出严格JSON（使用给定schema）：")
        lines.append("")
        lines.append("目标：")
        lines.append("1) 对齐spk与真实角色，跨集合并，同名变体归一")
        lines.append("2) 整理角色描述与首次出场信息")
        lines.append("3) 提取关键关系（父女/姐妹/护卫/爱慕/敌对等）")
        lines.append("4) 为每集提供speaker_alignment_hints")
        lines.append("")
        
        # 先验信息
        prior = preprocessed_data.get('prior_info', {})
        if prior.get('characters'):
            lines.append("先验角色信息（部分）：")
            for ch in prior['characters'][:20]:
                lines.append(f"- {ch.get('canonical_name','')}: {ch.get('traits_brief','')}")
            lines.append("")
        
        # 数据摘要
        summary = preprocessed_data.get('summary', {})
        lines.append(f"数据摘要：{summary.get('total_episodes', 0)}集，{summary.get('total_characters', 0)}个角色，{summary.get('total_speakers', 0)}个说话人")
        lines.append("")
        
        # 全集遍历（不限制集数）
        for item in preprocessed_data.get('episodes', []):
            ep = item.get('episode_id','')
            d = item.get('data', {})
            lines.append(f"[Episode {ep}]")
            
            # 角色（压缩字段）
            chars_comp = []
            for c in d.get('explicit_characters', []) or []:
                chars_comp.append({
                    'name': c.get('name',''),
                    'title_or_role': c.get('title_or_role',''),
                    'evidence_modality': c.get('evidence_modality','')
                })
            if chars_comp:
                lines.append(f"Characters: {json.dumps(chars_comp, ensure_ascii=False)}")
            
            # 说话人（压缩字段）
            spk_comp = []
            for s in d.get('speaker_profiles', []) or []:
                spk_comp.append({
                    'id': s.get('id',''),
                    'visual_description': (s.get('visual_description','') or '')[:160],
                    'key_lines': (s.get('key_lines') or [])[:2] if isinstance(s.get('key_lines'), list) else []
                })
            if spk_comp:
                lines.append(f"Speakers: {json.dumps(spk_comp, ensure_ascii=False)}")
            
            # 交互（适度压缩，扩大数量）
            inter_comp = []
            for inter in (d.get('observed_interactions', []) or [])[:20]:
                inter_comp.append({
                    'participants': inter.get('participants', []),
                    'summary': (inter.get('summary','') or '')[:160],
                    'action_type': inter.get('action_type',''),
                    'addressee': inter.get('addressee','')
                })
            if inter_comp:
                lines.append(f"Interactions: {json.dumps(inter_comp, ensure_ascii=False)}")
            
            lines.append("")
        
        # 明确输出要求
        lines.append("请确保：1) 角色独立性，避免过度合并；2) 严格JSON，无解释文本；3) speaker_alignment_hints 使用标准 spk_X_epN。")
        
        return "\n".join(lines)

    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """规范化LLM返回，修正拼写并补充Step0.4所需的关键字段。"""
        normalized: Dict[str, Any] = {
            "characters": [],
            "relationships": result.get("relationships", []) or [],
            "per_episode": []
        }
        # 1) 角色字段规范化
        for ch in result.get("characters", []) or []:
            # 修正拼写: first_appearence_evidence -> first_appearance_evidence
            if "first_appearence_evidence" in ch and "first_appearance_evidence" not in ch:
                ch["first_appearance_evidence"] = ch.get("first_appearence_evidence")
            name = ch.get("name") or ch.get("canonical_name") or ""
            description = ch.get("description") or ch.get("traits") or ""
            normalized["characters"].append({
                "name": name,
                "description": description,
                "first_appearance_episode": ch.get("first_appearance_episode") or "",
                "first_appearance_evidence": ch.get("first_appearance_evidence") or "",
                # 兼容Step0.4摘要使用
                "canonical_name": name,
                "traits": description,
                "aliases": ch.get("aliases") or [],
                "is_high_status": bool(ch.get("is_high_status") or False)
            })
        
        # 2) per_episode 结构规范化
        for pe in result.get("per_episode", []) or []:
            ep_id = pe.get("episode_id") or ""
            hints = pe.get("speaker_alignment_hints") or []
            supporting = pe.get("supporting") or []
            # 将字符串提示包装为结构化对象，便于Step0.4消费
            norm_hints = []
            for h in hints:
                if isinstance(h, dict):
                    # 保持既有结构；并确保关键键存在
                    norm_hints.append({
                        "spk_full_id": h.get("spk_full_id") or h.get("spk") or "",
                        "candidate": h.get("candidate") or "",
                        "confidence": h.get("confidence") or 0,
                        "sticky": bool(h.get("sticky") or False),
                        "role_type": h.get("role_type") or "",
                        "affiliation_owner": h.get("affiliation_owner") or "",
                        "rationale": h.get("rationale") or ""
                    })
                else:
                    # 字符串场景，尝试简单解析 spk/candidate；否则仅登记 spk_full_id
                    text = str(h)
                    cand = ""
                    spk = text
                    # 简单格式: "spk_1_ep3 -> 角色名"
                    if "->" in text:
                        parts = [p.strip() for p in text.split("->", 1)]
                        if len(parts) == 2:
                            spk, cand = parts[0], parts[1]
                    norm_hints.append({
                        "spk_full_id": spk,
                        "candidate": cand,
                        "confidence": 0,
                        "sticky": False,
                        "role_type": "",
                        "affiliation_owner": "",
                        "rationale": ""
                    })
            normalized["per_episode"].append({
                "episode_id": ep_id,
                "speaker_alignment_hints": norm_hints,
                "supporting": [
                    {
                        "label": (s or {}).get("label", ""),
                        "title_or_role": (s or {}).get("title_or_role", ""),
                        "affiliation_owner": (s or {}).get("affiliation_owner", ""),
                        "affiliation_type": (s or {}).get("affiliation_type", ""),
                        "episode_traits": (s or {}).get("episode_traits", "")
                    } for s in supporting if isinstance(s, dict)
                ]
            })
        return normalized
