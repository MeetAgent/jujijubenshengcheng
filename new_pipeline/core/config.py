"""
配置管理模块
"""

import os
import yaml
from typing import Dict, Any
from .exceptions import ConfigError

class PipelineConfig:
    """流水线配置管理"""
    
    def __init__(self, config_path: str = "pipeline.yaml"):
        # 优先使用 new_pipeline/pipeline.yaml，其次使用传入路径，最后使用仓库根 pipeline.yaml
        prefer_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "pipeline.yaml"))
        if os.path.exists(prefer_path):
            config_path = prefer_path
        elif not os.path.exists(config_path):
            alt_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline.yaml"))
            if os.path.exists(alt_root):
                config_path = alt_root
            else:
                raise ConfigError(f"配置文件不存在: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # 验证必要配置
        required_fields = ['gcp.project_id', 'gcp.location', 'gcp.credentials_path']
        for field in required_fields:
            if not self._get_nested(field):
                raise ConfigError(f"缺少必要配置: {field}")
    
    def _get_nested(self, path: str) -> Any:
        """获取嵌套配置值"""
        keys = path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value
    
    @property
    def project_root(self) -> str:
        return self._get_nested('project.root_dir')
    
    @property
    def output_dir(self) -> str:
        return self._get_nested('project.output_dir')
    
    @property
    def gcp_config(self) -> Dict:
        return {
            'project_id': self._get_nested('gcp.project_id'),
            'location': self._get_nested('gcp.location'),
            'credentials_path': self._get_nested('gcp.credentials_path'),
            'bucket_name': self._get_nested('gcp.bucket_name')
        }
    
    def get_step_config(self, step_number: int) -> Dict:
        """获取指定步骤的配置"""
        step_key = f"step{step_number}"
        return self._get_nested(f'steps.{step_key}') or {}
    
    def get_step_config_by_name(self, step_name: str) -> Dict:
        """根据步骤名称获取配置（支持带下划线的步骤名）"""
        return self._get_nested(f'steps.{step_name}') or {}
    
    def get_model_config(self, step_number: int) -> Dict:
        """获取指定步骤的模型配置"""
        step_config = self.get_step_config(step_number)
        return {
            'model': step_config.get('model'),
            'max_tokens': step_config.get('max_tokens', 4096),
            'temperature': step_config.get('temperature', 0.2),
            'top_p': step_config.get('top_p', 0.8),
            'top_k': step_config.get('top_k', 20)
        }
    
    def get_model_config_by_name(self, step_name: str) -> Dict:
        """根据步骤名称获取模型配置（支持带下划线的步骤名）"""
        step_config = self.get_step_config_by_name(step_name)
        return {
            'model': step_config.get('model'),
            'max_tokens': step_config.get('max_tokens', 4096),
            'temperature': step_config.get('temperature', 0.2),
            'top_p': step_config.get('top_p', 0.8),
            'top_k': step_config.get('top_k', 20)
        }
    
    @property
    def concurrency(self) -> Dict:
        return self._get_nested('concurrency') or {'max_workers': 4}


