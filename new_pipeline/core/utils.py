"""
通用工具模块
"""

import os
import json
import glob
from typing import List, Dict, Any
from .exceptions import FileNotFoundError

class PipelineUtils:
    """流水线通用工具"""
    
    @staticmethod
    def get_episode_list(project_root: str) -> List[str]:
        """获取所有剧集列表"""
        pattern = os.path.join(project_root, "episode_*")
        episodes = []
        for path in glob.glob(pattern):
            episode_id = os.path.basename(path)
            if os.path.isdir(path):
                # 检查是否有gcs_path.txt文件（表示已上传到GCS）
                files = os.listdir(path)
                gcs_files = [f for f in files if f in ['0_gcs_path.txt', 'gcs_path.txt']]
                if gcs_files:
                    episodes.append(episode_id)
        return sorted(episodes)
    
    @staticmethod
    def get_video_uri(episode_id: str, project_root: str) -> str:
        """获取视频GCS URI"""
        # 尝试多个可能的文件名
        possible_files = [
            os.path.join(project_root, episode_id, "0_gcs_path.txt"),
            os.path.join(project_root, episode_id, "gcs_path.txt")
        ]
        
        for gcs_path_file in possible_files:
            if os.path.exists(gcs_path_file):
                with open(gcs_path_file, 'r') as f:
                    return f.read().strip()
        
        # 如果没有找到gcs_path.txt，构造默认路径
        episode_num = episode_id.split('_')[1]
        return f"gs://reel-trans-test/{episode_num}.mp4"
    
    @staticmethod
    def save_json_file(file_path: str, data: Dict, ensure_ascii: bool = False) -> None:
        """保存JSON文件"""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=ensure_ascii, indent=2)
    
    @staticmethod
    def load_json_file(file_path: str, default: Any = None) -> Any:
        """加载JSON文件"""
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: 无法读取 {file_path}: {e}")
        return default
    
    @staticmethod
    def save_text_file(file_path: str, content: str) -> None:
        """保存文本文件"""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
    
    @staticmethod
    def load_text_file(file_path: str, default: str = "") -> str:
        """加载文本文件"""
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                print(f"Warning: 无法读取 {file_path}: {e}")
        return default
    
    @staticmethod
    def get_episode_output_dir(output_dir: str, episode_id: str) -> str:
        """获取剧集输出目录"""
        return os.path.join(output_dir, episode_id)
    
    @staticmethod
    def get_global_output_dir(output_dir: str) -> str:
        """获取全局输出目录"""
        return os.path.join(output_dir, "global")
    
    @staticmethod
    def get_step_file_path(output_dir: str, episode_id: str, step_number: int, filename: str) -> str:
        """获取步骤文件路径"""
        if step_number == 2:  # Step2生成全局文件
            return os.path.join(output_dir, "global", f"{step_number}_{filename}")
        else:
            return os.path.join(output_dir, episode_id, f"{step_number}_{filename}")
    
    @staticmethod
    def check_step_dependency(output_dir: str, episode_id: str, step_number: int, required_files: List[str]) -> bool:
        """检查步骤依赖"""
        for filename in required_files:
            if step_number == 2:  # Step2生成全局文件
                file_path = os.path.join(output_dir, "global", f"{step_number-1}_{filename}")
            else:
                file_path = os.path.join(output_dir, episode_id, f"{step_number-1}_{filename}")
            
            if not os.path.exists(file_path):
                print(f"缺少依赖文件: {file_path}")
                return False
        return True
    
    @staticmethod
    def extract_between(text: str, start_token: str, end_token: str = None) -> str:
        """提取两个标记之间的内容"""
        if not text:
            return ""
        s = text.find(start_token)
        if s == -1:
            return ""
        s += len(start_token)
        if end_token:
            e = text.find(end_token, s)
            return text[s:e] if e != -1 else text[s:]
        return text[s:]
    
    @staticmethod
    def extract_json(text: str) -> str:
        """从文本中提取第一个JSON块"""
        i = text.find("{")
        if i < 0:
            raise ValueError("no json")
        brace, j = 0, i
        while j < len(text):
            if text[j] == "{":
                brace += 1
            elif text[j] == "}":
                brace -= 1
                if brace == 0:
                    return text[i:j+1]
            j += 1
        raise ValueError("json incomplete")


