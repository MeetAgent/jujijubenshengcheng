"""
Step0.8: 最终剧本生成
基于Step0.7的剧本（0_7_script.stmf）合并并导出为多格式
"""

import os
import glob
import re
from typing import Dict, Any, List

from core import PipelineConfig, GenAIClient, PipelineUtils
from . import PipelineStep

from new_pipeline.steps.commont_log import log

class Step0_8FinalScript(PipelineStep):
    """Step0.8: 最终剧本生成"""
    
    @property
    def step_number(self) -> int:
        return 8
    
    @property
    def step_name(self) -> str:
        return "final_script"
    
    def check_dependencies(self, episode_id: str = None) -> bool:
        # 依赖所有剧集的Step0.7输出
        episodes = self.utils.get_episode_list(self.config.project_root)
        for ep in episodes:
            stmf_file = os.path.join(
                self.config.output_dir, ep, "0_7_script.stmf"
            )
            if not os.path.exists(stmf_file):
                log.info(f"缺少依赖文件: {stmf_file}")
                return False
        return True
    
    def get_output_files(self, episode_id: str = None) -> List[str]:
        # 生成全局文件
        return [
            "0_8_merged_script.stmf",
            "0_8_complete_screenplay.fountain",
            "0_8_complete_screenplay.fdx"
        ]
    
    def run(self, episode_id: str = None) -> Dict[str, Any]:
        if episode_id:
            log.info("⚠️  Step0.8不支持单剧集处理，将处理所有剧集")
        return self._run_merge_and_convert()
    
    def _run_merge_and_convert(self) -> Dict[str, Any]:
        log.info("Step0.8: 合并与导出最终剧本")
        output_dir = self.config.output_dir
        out_stmf = os.path.join(output_dir, "0_8_merged_script.stmf")
        out_fountain = os.path.join(output_dir, "0_8_complete_screenplay.fountain")
        out_fdx = os.path.join(output_dir, "0_8_complete_screenplay.fdx")
        
        if all(os.path.exists(p) for p in [out_stmf, out_fountain, out_fdx]) and not os.environ.get("FORCE_OVERWRITE"):
            log.info("✅ Step0.8输出已存在，跳过处理")
            return {"status": "already_exists"}
        
        try:
            # 1) 收集0_7产物
            stmf_files = self._collect_step07_stmf()
            if not stmf_files:
                log.info("Warning: 未找到任何0_7_script.stmf")
                return {"status": "no_files"}
            
            # 2) 合并
            merged_content = self._merge_stmf_files(stmf_files)
            
            # 3) 转换为Fountain（健壮解析器）
            fountain_script = self._convert_stmf_to_fountain(merged_content)
            # 3b) 转换为FDX（最小可用映射）
            fdx_script = self._convert_stmf_to_fdx(merged_content)
            
            # 4) 保存
            self.utils.save_text_file(out_stmf, merged_content)
            self.utils.save_text_file(out_fountain, fountain_script)
            self.utils.save_text_file(out_fdx, fdx_script)
            
            scenes_count = len([line for line in merged_content.split('\n') if line.strip().startswith('SCENE')])
            log.info("✅ Step0.8 完成（已生成 STMF、Fountain 与 FDX）")
            return {"status": "completed", "episodes_count": len(stmf_files), "scenes_count": scenes_count}
        except Exception as e:
            log.info(f"❌ Step0.8 失败: {e}")
            return {"status": "failed", "error": str(e)}
    
    def _collect_step07_stmf(self) -> List[str]:
        pattern = os.path.join(self.config.output_dir, "episode_*", "0_7_script.stmf")
        files = glob.glob(pattern)
        # 排序
        def ep_num(p):
            try:
                d = os.path.basename(os.path.dirname(p))
                if d.startswith('episode_'):
                    import re
                    n = re.findall(r'\d+', d)
                    return int(n[0]) if n else 0
                return 0
            except:
                return 0
        files.sort(key=ep_num)
        return files
    
    def _merge_stmf_files(self, stmf_files: List[str]) -> str:
        all_content = []
        for p in stmf_files:
            try:
                c = self.utils.load_text_file(p)
                if c.strip():
                    # 提取剧集编号
                    episode_dir = os.path.basename(os.path.dirname(p))
                    if episode_dir.startswith('episode_'):
                        episode_num = episode_dir.replace('episode_', '').lstrip('0') or '1'
                        # 添加 [EPISODE] 标记
                        episode_header = f"[EPISODE] Episode {episode_num}\n"
                        all_content.append(episode_header + c)
                    else:
                        all_content.append(c)
                    log.info(f"加载: {episode_dir}")
            except Exception as e:
                log.info(f"Warning: 无法加载 {p}: {e}")
        if not all_content:
            raise Exception("没有成功加载任何0_7_script.stmf")
        return "\n\n".join(all_content)
    
    # === 健壮的 STMF -> Fountain 转换器 ===
    def _convert_stmf_to_fountain(self, stmf_content: str) -> str:
        lines = stmf_content.split('\n')
        out: List[str] = []
        # 使用输出目录名作为标题（即集合名）
        try:
            title = os.path.basename(self.config.output_dir) or "完整剧本"
        except Exception:
            title = "完整剧本"
        # 如果是方括号标签，采用简单直译
        if any(ln.strip().startswith('[') for ln in lines):
            out.append(f"Title: {title}")
            out.append("")
            current_char = None
            prev_tag = None  # 'SCENE'|'ACTION'|'CHARACTER'|'PAREN'|'DIALOG'|'EPISODE'|'TRANS'

            def ensure_single_blank_between(prev_kind: str, next_kind: str):
                if out and out[-1] != "":
                    # CHARACTER -> (PAREN or DIALOG) 不加空行
                    if prev_kind == 'CHARACTER' and next_kind in ('PAREN', 'DIALOG'):
                        return
                    # PAREN -> DIALOG 不加空行
                    if prev_kind == 'PAREN' and next_kind == 'DIALOG':
                        return
                    out.append("")

            for raw in lines:
                ln = raw.strip()
                if not ln:
                    continue
                if ln.startswith('[EPISODE]'):
                    # 转为章节标题，便于Celtx识别为分集
                    import re as _re
                    m = _re.findall(r"\d+", ln)
                    ep = m[0] if m else ln[len('[EPISODE]'):].strip()
                    ensure_single_blank_between(prev_tag, 'EPISODE') if prev_tag else None
                    out.append(f"# EPISODE {ep}")
                    out.append("")
                    prev_tag = 'EPISODE'
                    current_char = None
                    continue
                if ln.startswith('[SCENE]'):
                    ensure_single_blank_between(prev_tag, 'SCENE') if prev_tag else None
                    hdr = ln[len('[SCENE]'):].strip()
                    import re as _re
                    hdr = _re.sub(r"^\s*\d+\s*-\s*\d+\s*\.\s*", "", hdr)
                    hdr = hdr.replace(' – ', ' - ').replace('—', '-').replace('–', '-')
                    out.append(hdr)
                    out.append("")
                    current_char = None
                    prev_tag = 'SCENE'
                    continue
                if ln.startswith('[ACTION]'):
                    ensure_single_blank_between(prev_tag, 'ACTION') if prev_tag else None
                    import re as _re
                    txt = ln[len('[ACTION]'):].strip().lstrip()
                    if not _re.search(r"[。！？.!?:：]$", txt):
                        txt = txt + "。"
                    # 关键：前缀 '! ' 强制声明为动作（不再清洗年龄括注，按上游控制）
                    out.append(f"! {txt}")
                    current_char = None
                    prev_tag = 'ACTION'
                    continue
                if ln.startswith('[TRANS]'):
                    ensure_single_blank_between(prev_tag, 'TRANS') if prev_tag else None
                    trans = ln[len('[TRANS]'):].strip() or 'CUT TO:'
                    out.append(f"> {trans}")
                    out.append("")
                    current_char = None
                    prev_tag = 'TRANS'
                    continue
                if ln.startswith('[CHARACTER]'):
                    ensure_single_blank_between(prev_tag, 'CHARACTER') if prev_tag else None
                    cue = ln[len('[CHARACTER]'):].strip().upper()
                    out.append(cue)
                    current_char = cue
                    prev_tag = 'CHARACTER'
                    continue
                if ln.startswith('[PAREN]'):
                    # CHARACTER -> PAREN 之间无空行
                    if prev_tag not in ('CHARACTER',):
                        ensure_single_blank_between(prev_tag, 'PAREN') if prev_tag else None
                    text = ln[len('[PAREN]'):].strip()
                    if not text.startswith('('):
                        text = f"({text})"
                    out.append(text)
                    prev_tag = 'PAREN'
                    # 不在 PAREN 后补空行，紧接 DIALOG
                    continue
                if ln.startswith('[DIALOG]'):
                    # CHARACTER/PAREN -> DIALOG 无空行；其余情况保持单空行
                    if prev_tag not in ('CHARACTER', 'PAREN'):
                        ensure_single_blank_between(prev_tag, 'DIALOG') if prev_tag else None
                    text = ln[len('[DIALOG]'):].strip()
                    if current_char is None:
                        out.append("角色")
                        current_char = "角色"
                    out.append(f"    {text}")
                    # 对白块结束后补单一空行
                    out.append("")
                    prev_tag = 'DIALOG'
                    continue
            return "\n".join(out).strip() + "\n"
        # 否则退回旧解析（兼容旧STMF）
        return f"Title: {title}\n"
    
    @staticmethod
    def _extract_attr_value(line: str, key: str) -> str:
        # 提取如 [display] : 顾向晴
        m = re.search(rf"\[{re.escape(key)}\]\s*:\s*([^\[]+)", line)
        return m.group(1).strip() if m else ""

    # === 最小可用的 STMF -> FDX 转换器 ===
    def _convert_stmf_to_fdx(self, stmf_content: str) -> str:
        import os as _os
        mode = (_os.getenv('FDX_ENCODING_MODE') or 'decimal_entities').strip().lower()
        def base_escape(text: str) -> str:
            return (
                text.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace('"', "&quot;")
                    .replace("'", "&apos;")
            )
        def xml_escape(text: str) -> str:
            escaped = base_escape(text)
            if mode.startswith('decimal'):
                return ''.join(c if ord(c) < 128 else f"&#{ord(c)};" for c in escaped)
            # plain 模式：仅基础实体，不做非 ASCII 转义
            return escaped
        lines = stmf_content.split('\n')
        out: List[str] = []
        # 使用 UTF-8，无 BOM（除非显式指定 plain_utf8_bom）
        out.append('<?xml version="1.0" encoding="UTF-8" standalone="no"?>')
        out.append('<FinalDraft DocumentType="Script" Template="No" Version="1">')
        out.append('  <Content>')
        current_char = None
        for raw in lines:
            ln = raw.strip()
            if not ln:
                continue
            if ln.startswith('['):
                if ln.startswith('[EPISODE]'):
                    ep_nums = re.findall(r"\d+", ln)
                    label = f"Episode {ep_nums[0]}" if ep_nums else ln[len('[EPISODE]'):].strip()
                    out.append(f'    <Paragraph Type="Scene Heading"><SceneProperties /><Text>{xml_escape(label)}</Text></Paragraph>')
                    current_char = None
                    continue
                if ln.startswith('[SCENE]'):
                    hdr = ln[len('[SCENE]'):].strip()
                    hdr = hdr.replace(' – ', ' - ').replace('—', '-').replace('–', '-')
                    out.append(f'    <Paragraph Type="Scene Heading"><SceneProperties /><Text>{xml_escape(hdr)}</Text></Paragraph>')
                    current_char = None
                    continue
                if ln.startswith('[ACTION]'):
                    txt = ln[len('[ACTION]'):].strip()
                    out.append(f'    <Paragraph Type="Action"><Text>{xml_escape(txt)}</Text></Paragraph>')
                    current_char = None
                    continue
                if ln.startswith('[TRANS]'):
                    txt = ln[len('[TRANS]'):].strip() or 'CUT TO:'
                    if not txt.endswith(':'):
                        txt = txt + ':'
                    out.append(f'    <Paragraph Type="Transition"><Text>{xml_escape(txt)}</Text></Paragraph>')
                    current_char = None
                    continue
                if ln.startswith('[CHARACTER]'):
                    cue = ln[len('[CHARACTER]'):].strip().upper()
                    out.append(f'    <Paragraph Type="Character"><Text>{xml_escape(cue)}</Text></Paragraph>')
                    current_char = cue
                    continue
                if ln.startswith('[PAREN]'):
                    text = ln[len('[PAREN]'):].strip()
                    if not text.startswith('('):
                        text = f'({text})'
                    out.append(f'    <Paragraph Type="Parenthetical"><Text>{xml_escape(text)}</Text></Paragraph>')
                    continue
                if ln.startswith('[DIALOG]'):
                    text = ln[len('[DIALOG]'):].strip()
                    out.append(f'    <Paragraph Type="Dialogue"><Text>{xml_escape(text)}</Text></Paragraph>')
                    continue
                continue
            out.append(f'    <Paragraph Type="Action"><Text>{xml_escape(ln)}</Text></Paragraph>')
            current_char = None
        out.append('  </Content>')
        out.append('</FinalDraft>')
        xml_text = ("\r\n" if mode.endswith('_crlf') else "\n").join(out) + ("\r\n" if mode.endswith('_crlf') else "\n")
        if mode == 'plain_utf8_bom' or mode == 'plain_utf8_bom_crlf':
            return "\ufeff" + xml_text
        return xml_text
