"""
Step0.4: 说话人身份校准
使用全局人物关系图谱校准匿名说话人ID为具体角色名
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import PipelineStep
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Dict, List, Any
import json
import re

from new_pipeline.steps.commont_log import log

class Step0_4SpeakerCalibration(PipelineStep):
    """说话人身份校准步骤"""
    
    def __init__(self, config):
        super().__init__(config)
        self._step_name = "Step0.4"
        self._description = "说话人身份校准"
        self.global_graph = None
    
    @property
    def step_number(self) -> int:
        """步骤编号"""
        return 0.4
    
    @property
    def step_name(self) -> str:
        """步骤名称"""
        return self._step_name
    
    def run(self) -> Dict[str, Any]:
        """运行说话人身份校准"""
        # 加载全局图谱（优先LLM版，其次启发式版）——兼容两种落盘位置（根目录 /global 子目录）
        candidates = [
            f"{self.config.project_root}/global/global_character_graph_llm.json",
            f"{self.config.project_root}/global_character_graph_llm.json",
            f"{self.config.project_root}/global/global_character_graph.json",
            f"{self.config.project_root}/global_character_graph.json",
        ]
        selected = None
        for p in candidates:
            if os.path.exists(p):
                selected = p
                break
        if not selected:
            log.info("❌ 全局图谱文件不存在，请先运行Step0.3/Step0.3-LLM")
            return {"status": "failed", "error": "全局图谱文件不存在"}
        with open(selected, 'r', encoding='utf-8') as f:
            self.global_graph = json.load(f)
        
        # 兼容不同结构：LLM版无 relationships 字段时安全打印
        rels = self.global_graph.get('relationships', [])
        log.info(f"✅ 加载全局图谱: {len(self.global_graph.get('characters', []))} 个角色, {len(rels)} 个关系 | 文件: {os.path.relpath(selected, self.config.project_root)}")
        
        return self._run_all_episodes()
    
    def _run_all_episodes(self) -> Dict[str, Any]:
        """处理所有剧集"""
        episodes = self.utils.get_episode_list(self.config.project_root)
        results = []
        log.info(f"开始{self.step_name}: 处理 {len(episodes)} 个剧集...")
        
        # 并发配置：步骤级 -> 全局 -> 默认 2
        step_conf = self.config.get_step_config_by_name('step0_4') or {}
        max_workers = step_conf.get('max_workers')
        if max_workers is None:
            conc_conf = getattr(self.config, 'concurrency', {}) or {}
            max_workers = conc_conf.get('max_workers', 2)
        try:
            max_workers = int(max_workers)
        except Exception:
            max_workers = 2
        if max_workers < 1:
            max_workers = 1
        log.info(f"使用 {max_workers} 个并行线程处理...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_episode = {
                executor.submit(self._run_single_episode, ep): ep 
                for ep in episodes
            }
            
            for future in tqdm(as_completed(future_to_episode), total=len(episodes), desc=self.step_name):
                episode_id = future_to_episode[future]
                try:
                    result = future.result()
                    result["episode_id"] = episode_id
                    results.append(result)
                except Exception as e:
                    log.info(f"❌ {self.step_name} 处理 {episode_id} 失败: {e}")
                    results.append({
                        "episode_id": episode_id, 
                        "status": "failed", 
                        "error": str(e)
                    })
        
        stats = self._generate_statistics(results)
        log.info(f"\n📊 {self.step_name} 处理完成统计:")
        log.info(f"   总剧集数: {stats['total_episodes']}")
        log.info(f"   成功处理: {stats['success_count']}")
        log.info(f"   失败处理: {stats['failed_count']}")
        log.info(f"   成功率: {stats['success_rate']:.1f}%")
        log.info(f"   总校准对话数: {stats['total_calibrated_dialogues']}")
        
        return {
            "status": "completed", 
            "results": results,
            "statistics": stats
        }
    
    def _run_single_episode(self, episode_id: str) -> Dict[str, Any]:
        """处理单个剧集"""
        log.info(f"{self.step_name}: 处理 {episode_id}")
        
        # 检查是否已有输出文件
        output_file = f"{self.config.project_root}/{episode_id}/0_4_calibrated_dialogue.txt"
        if os.path.exists(output_file) and not os.getenv('FORCE_OVERWRITE'):
            log.info(f"✅ {episode_id} 已有{self.step_name}输出文件，跳过处理")
            return {"status": "already_exists"}
        
        # 检查输入文件
        # 优先使用 .srt 文件，回退到 .txt 文件
        asr_file = f"{self.config.project_root}/{episode_id}/0_1_timed_dialogue.srt"
        if not os.path.exists(asr_file):
            asr_file = f"{self.config.project_root}/{episode_id}/0_1_timed_dialogue.txt"
        if not os.path.exists(asr_file):
            log.info(f"❌ {episode_id} 缺少ASR输入文件: {asr_file}")
            return {"status": "failed", "error": "缺少ASR输入文件"}
        
        # 获取视频URI
        video_uri = self.utils.get_video_uri(episode_id, self.config.project_root)
        
        # 读取ASR对话内容
        with open(asr_file, 'r', encoding='utf-8') as f:
            asr_content = f.read()
        
        # 使用模型配置
        model_config = self.config.get_model_config_by_name("step0_4")  # 使用step0_4配置
        
        # 3a: 纯代码初步回填（不调用LLM）
        # 构建当集 spk 初始候选映射（来自 Step0.3-LLM hints）
        initial_map = self._build_initial_assignment(episode_id)
        # 解析原始分句为结构化
        original_segments = self._parse_srt_like(asr_content)
        # 先进行"回填"：用当集 initial_map 将 [spk_X] 映射为角色名，得出预填的 segments
        prefilled_segments = []
        for seg in original_segments:
            orig_speaker = seg.get('speaker','').strip()
            spk_id = orig_speaker[1:-1] if orig_speaker.startswith('[') and orig_speaker.endswith(']') else orig_speaker
            # 计算当集编号变量（支持 spk_X_epN 变体）
            ep_suffix = None
            try:
                ep_num = int(episode_id.split('_')[-1])
                ep_suffix = f"_ep{ep_num}"
            except Exception:
                ep_suffix = None
            candidates = []
            if spk_id:
                candidates.append(spk_id)
                if ep_suffix:
                    candidates.append(spk_id + ep_suffix)
            if orig_speaker:
                candidates.append(orig_speaker)
                if ep_suffix:
                    candidates.append(orig_speaker + ep_suffix)
            for k in list(candidates):
                if k and not k.startswith('['):
                    candidates.append(f"[{k}]")
            mapped = ''
            for k in candidates:
                if k in initial_map:
                    mapped = initial_map[k]
                    break
            final_speaker = f"[{mapped}]" if mapped else (orig_speaker or "")
            prefilled_segments.append({
                'index': int(seg['index']),
                'start_ms': int(seg['start_ms']),
                'end_ms': int(seg['end_ms']),
                'original_spk': orig_speaker,
                'speaker': final_speaker,
                'text': seg['text']
            })
        draft_text = self._render_dialogues_as_srt_like(prefilled_segments)
        # 仅在调试时输出草稿文件
        if os.getenv('DEBUG_DRAFT') == '1':
            draft_file = f"{self.config.project_root}/{episode_id}/0_4_dialogue_draft.txt"
            self.utils.save_text_file(draft_file, draft_text)

        # 直接输出回填结果（不调用LLM）
        output_file = f"{self.config.project_root}/{episode_id}/0_4_calibrated_dialogue.txt"
        self.utils.save_text_file(output_file, draft_text)
        log.info(f"✅ {episode_id} {self.step_name} 回填完成（无LLM）")

        # 统计
        calibrated_count = self._count_original_dialogues(draft_text)
        original_count = self._count_original_dialogues(asr_content)
        return {
            "status": "completed",
            "original_dialogues": original_count,
            "calibrated_dialogues": calibrated_count,
            "calibration_rate": calibrated_count / original_count if original_count > 0 else 0
        }
    
    def _build_character_summary(self, episode_id: str = None) -> str:
        """构建角色信息摘要，优先使用当集独立文件"""
        summary_lines = []
        
        # 1. 优先尝试读取当集独立的 0_3_speaker_hints.csv
        if episode_id:
            ep_hints_csv = os.path.join(self.config.project_root, episode_id, '0_3_speaker_hints.csv')
            
            if os.path.exists(ep_hints_csv):
                try:
                    import csv
                    summary_lines.append(f"【{episode_id} 角色白名单（当集文件）】")
                    
                    with open(ep_hints_csv, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        hints = list(reader)
                    
                    # 按角色类型分组
                    protagonists = []
                    supporting = []
                    for h in hints:
                        role_type = h.get('role_type', '').strip()
                        if role_type == 'protagonist':
                            protagonists.append(h)
                        else:
                            supporting.append(h)
                    
                    # 主角列表
                    if protagonists:
                        summary_lines.append("主角:")
                        for h in protagonists:
                            cand = h.get('candidate', '')
                            evidence = h.get('evidence', '')
                            conf = h.get('confidence', '')
                            sticky = h.get('sticky', '')
                            summary_lines.append(f"- {cand} (置信度:{conf}" + (", 锁定" if sticky.lower() in ['true', '1', 'yes'] else "") + f"): {evidence}")
                    
                    # 配角列表
                    if supporting:
                        summary_lines.append("配角:")
                        for h in supporting:
                            cand = h.get('candidate', '')
                            evidence = h.get('evidence', '')
                            owner = h.get('affiliation_owner', '')
                            conf = h.get('confidence', '')
                            sticky = h.get('sticky', '')
                            summary_lines.append(f"- {cand}" + (f" 从属于 {owner}" if owner else "") + f" (置信度:{conf}" + (", 锁定" if sticky.lower() in ['true', '1', 'yes'] else "") + f"): {evidence}")
                    
                    # 说话人映射
                    summary_lines.append("说话人映射:")
                    for h in hints:
                        spk = h.get('spk_full_id', '')
                        cand = h.get('candidate', '')
                        conf = h.get('confidence', '')
                        evidence = h.get('evidence', '')
                        summary_lines.append(f"- {spk} → {cand} (置信度:{conf}): {evidence}")
                    
                    log.info(f"✅ 从当集文件构建角色摘要: {ep_hints_csv}")
                    return "\n".join(summary_lines)
                except Exception as e:
                    log.info(f"⚠️ 读取当集文件失败: {e}")
            return False
        
        # 2. 回退到全局图谱
        if not self.global_graph:
            return "无角色信息"
        
        per_eps = self.global_graph.get('per_episode') or []
        ep_entry = None
        if episode_id:
            for e in per_eps:
                if e.get('episode_id') == episode_id:
                    ep_entry = e
                    break
        
        if ep_entry:
            summary_lines.append("[本集主角]")
            for p in ep_entry.get('protagonists', []) or []:
                summary_lines.append(f"- {p.get('canonical_name','')} ｜ {p.get('title_or_role','')} ｜ {p.get('episode_traits','')}")
            summary_lines.append("[本集配角]")
            for s in ep_entry.get('supporting', []) or []:
                summary_lines.append(f"- {s.get('label','')} ｜ {s.get('episode_traits','')}")
        else:
            for char in self.global_graph.get("characters", []):
                name = char.get("canonical_name", "")
                traits = char.get("traits", "")
                aliases = char.get("aliases", [])
                summary_lines.append(f"- {name}: {traits}")
                if aliases:
                    summary_lines.append(f"  别名: {', '.join(aliases)}")
        
        log.info(f"✅ 从全局图谱构建角色摘要")
        return "\n".join(summary_lines)
    
    def _count_calibrated_dialogues(self, content: str) -> int:
        """统计校准后的对话数量"""
        # 统计包含角色名称的对话行
        lines = content.split('\n')
        count = 0
        for line in lines:
            if re.match(r'^\d+$', line.strip()):  # 对话编号行
                count += 1
        return count
    
    def _count_original_dialogues(self, content: str) -> int:
        """统计原始对话数量"""
        # 按 SRT 三行块统计
        return len(self._parse_srt_like(content))

    def _parse_srt_like(self, content: str) -> List[Dict[str, Any]]:
        """解析 Step0.1 的SRT样式文本为结构化列表。"""
        def ts_to_ms(ts: str) -> int:
            # 格式: HH:MM:SS,mmm
            try:
                hhmmss, ms = ts.split(',')
                h, m, s = hhmmss.split(':')
                return (int(h)*3600 + int(m)*60 + int(s))*1000 + int(ms)
            except Exception:
                return 0
        lines = content.splitlines()
        i = 0
        items = []
        while i < len(lines):
            if not lines[i].strip():
                i += 1
                continue
            # index
            idx_line = lines[i].strip()
            if not idx_line.isdigit():
                i += 1
                continue
            idx = int(idx_line)
            if i+1 >= len(lines):
                break
            ts_line = lines[i+1].strip()
            start_ts, _, end_ts = ts_line.partition(' --> ')
            if i+2 >= len(lines):
                break
            txt_line = lines[i+2].rstrip('\n')
            # 解析speaker与text
            speaker = ''
            text = txt_line
            if txt_line.startswith('['):
                r = txt_line.find(']')
                if r != -1:
                    speaker = txt_line[:r+1]
                    text = txt_line[r+1:].lstrip()
            items.append({
                'index': idx,
                'start_ms': ts_to_ms(start_ts.strip()),
                'end_ms': ts_to_ms(end_ts.strip()),
                'speaker': speaker,
                'text': text
            })
            # 跳过空行分隔
            i += 4
        return items
    
    def _render_dialogues_as_srt_like(self, dialogues: List[Dict[str, Any]]) -> str:
        """将结构化 dialogues 渲染为 Step0.1 的SRT样式文本，保持完全一致格式。
        期望输出：
        index\n
        HH:MM:SS,mmm --> HH:MM:SS,mmm\n
        [Speaker] text\n
        （空行分隔）
        """
        def ms_to_ts(ms: int) -> str:
            ms = max(0, int(ms))
            s, milli = divmod(ms, 1000)
            m, sec = divmod(s, 60)
            h, minute = divmod(m, 60)
            return f"{h:02d}:{minute:02d}:{sec:02d},{milli:03d}"

        lines = []
        # 按 index 排序，保证顺序一致
        for item in sorted(dialogues, key=lambda x: int(x.get('index', 0))):
            start_ms = int(item.get('start_ms', 0))
            end_ms = int(item.get('end_ms', start_ms + 1000))
            spk = item.get('speaker', '').strip()
            text = item.get('text', '').strip()
            # 保障 speaker 以 [..] 包裹
            if spk and not spk.startswith('['):
                spk = f"[{spk}]"
            idx = int(item.get('index', 0))
            lines.append(str(idx))
            lines.append(f"{ms_to_ts(start_ms)} --> {ms_to_ts(end_ms)}")
            lines.append(f"{spk} {text}".rstrip())
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def _generate_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """生成统计信息"""
        total_episodes = len(results)
        success_count = len([r for r in results if r.get("status") == "success"])
        failed_count = len([r for r in results if r.get("status") == "failed"])
        already_exists_count = len([r for r in results if r.get("status") == "already_exists"])
        
        total_original_dialogues = sum(r.get("original_dialogues", 0) for r in results)
        total_calibrated_dialogues = sum(r.get("calibrated_dialogues", 0) for r in results)
        
        avg_calibration_rate = 0
        if success_count > 0:
            calibration_rates = [r.get("calibration_rate", 0) for r in results if r.get("status") == "success"]
            avg_calibration_rate = sum(calibration_rates) / len(calibration_rates) if calibration_rates else 0
        
        return {
            "total_episodes": total_episodes,
            "success_count": success_count,
            "failed_count": failed_count,
            "already_exists_count": already_exists_count,
            "success_rate": (success_count / total_episodes * 100) if total_episodes > 0 else 0,
            "total_original_dialogues": total_original_dialogues,
            "total_calibrated_dialogues": total_calibrated_dialogues,
            "avg_calibration_rate": avg_calibration_rate
        }

    def _build_initial_assignment(self, episode_id: str) -> Dict[str, str]:
        """优先从当集独立 CSV 文件构建 initial speaker 映射，回退到全局图谱。
        返回: { spk_full_id 或 [spk_X] : canonical_name }
        """
        mapping: Dict[str, str] = {}
        
        # 1. 优先尝试读取当集独立的 0_3_speaker_hints.csv
        ep_hints_csv = os.path.join(self.config.project_root, episode_id, '0_3_speaker_hints.csv')
        if os.path.exists(ep_hints_csv):
            try:
                import csv
                with open(ep_hints_csv, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        spk = row.get('spk_full_id', '').strip()
                        cand = row.get('candidate', '').strip()
                        try:
                            conf = float(row.get('confidence', 0) or 0)
                        except (ValueError, TypeError):
                            conf = 0
                        sticky = row.get('sticky', '').lower() in ['true', '1', 'yes']
                        
                        if not spk or not cand:
                            continue
                        
                        # 采用策略：优先 sticky；其次高置信度>=0.6；最后是来自 character_alias 的映射
                        take = False
                        if sticky:
                            take = True
                        elif conf >= 0.6 and spk not in mapping:
                            take = True
                        elif conf == 0 and spk not in mapping:  # 来自 character_alias 的映射（confidence 为空）
                            take = True
                        
                        if take:
                            # 同时登记去括号与带括号两种 key，便于回退时检索
                            mapping[spk] = cand
                            if spk.startswith('[') and spk.endswith(']'):
                                k = spk[1:-1]
                                mapping.setdefault(k, cand)
                            elif not spk.startswith('['):
                                mapping.setdefault(f'[{spk}]', cand)
                
                log.info(f"✅ 从当集文件加载映射: {ep_hints_csv} ({len(mapping)} 条)")
                return mapping
            except Exception as e:
                log.info(f"⚠️ 读取当集文件失败: {e}")
        
        # 2. 回退到全局图谱
        if not self.global_graph:
            return mapping
        per_eps = self.global_graph.get('per_episode') or []
        for e in per_eps:
            if e.get('episode_id') != episode_id:
                continue
            hints = e.get('speaker_alignment_hints') or []
            for h in hints:
                spk = h.get('spk_full_id') or ''
                cand = h.get('candidate') or ''
                conf = h.get('confidence') or 0
                sticky = bool(h.get('sticky'))
                # 采用策略：优先 sticky；其次高置信度>=0.6
                if not spk or not cand:
                    continue
                take = False
                if sticky:
                    take = True
                elif conf >= 0.6 and spk not in mapping:
                    take = True
                if take:
                    # 同时登记去括号与带括号两种 key，便于回退时检索
                    mapping[spk] = cand
                    if spk.startswith('[') and spk.endswith(']'):
                        k = spk[1:-1]
                        mapping.setdefault(k, cand)
                    elif not spk.startswith('['):
                        mapping.setdefault(f'[{spk}]', cand)
        
        log.info(f"✅ 从全局图谱加载映射: {len(mapping)} 条")
        return mapping



    def _get_episode_whitelist(self, episode_id: str) -> set:
        """从全局图谱中获取当集允许的角色白名单（protagonists + supporting 标签）。"""
        names = set()
        if not self.global_graph:
            return names
        for e in self.global_graph.get('per_episode', []) or []:
            if e.get('episode_id') != episode_id:
                continue
            for p in e.get('protagonists', []) or []:
                cn = p.get('canonical_name')
                if cn:
                    names.add(cn)
            for s in e.get('supporting', []) or []:
                label = s.get('label')
                if label:
                    names.add(label)
        return names

    def _build_episode_alias_map(self, episode_id: str) -> Dict[str, str]:
        """仅基于当集 speaker_alignment_hints 构建映射，派生去除 _epN 的 spk 变体，含带/不带括号。"""
        mapping: Dict[str, str] = {}
        if not self.global_graph:
            return mapping
        try:
            ep_num = int(episode_id.split('_')[-1])
            ep_suffix = f"_ep{ep_num}"
        except Exception:
            ep_suffix = None
        import re as _re
        for e in self.global_graph.get('per_episode', []) or []:
            if e.get('episode_id') != episode_id:
                continue
            for h in e.get('speaker_alignment_hints', []) or []:
                spk = h.get('spk_full_id') or ''
                cand = h.get('candidate') or ''
                if not spk or not cand:
                    continue
                # 原始键
                mapping[spk] = cand
                if not spk.startswith('['):
                    mapping[f'[{spk}]'] = cand
                else:
                    inner = spk[1:-1]
                    mapping[inner] = cand
                # 去除 _epN 派生
                base = spk[1:-1] if (spk.startswith('[') and spk.endswith(']')) else spk
                base_no_ep = _re.sub(r'_ep\d+$', '', base)
                if base_no_ep and base_no_ep != base:
                    mapping.setdefault(base_no_ep, cand)
                    mapping.setdefault(f'[{base_no_ep}]', cand)
        return mapping

    def _load_manual_overrides(self) -> Dict[str, Dict[str, str]]:
        """读取全局手工覆盖文件 global/manual_alias_overrides.json
        期望结构：{ "episode_001": {"spk_0": "旁白", "spk_1_ep1": "顾向晴"}, ... }
        """
        overrides_path = os.path.join(self.config.project_root, 'global', 'manual_alias_overrides.json')
        if not os.path.exists(overrides_path):
            return {}
        try:
            with open(overrides_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 统一键为字符串
            norm = {}
            for ep, m in (data or {}).items():
                if not isinstance(m, dict):
                    continue
                norm[ep] = {str(k): str(v) for k, v in m.items()}
            return norm
        except Exception:
            return {}


if __name__ == "__main__":
    from new_pipeline.core import PipelineConfig
    config = PipelineConfig()
    step = Step0_4SpeakerCalibration(config)
    result = step.run()
    log.info(f"最终结果: {result}")
