"""
步骤模块
包含所有流水线步骤的实现
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import PipelineConfig, GenAIClient, PipelineUtils

class PipelineStep(ABC):
    """流水线步骤基类"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.client = GenAIClient(config)
        self.utils = PipelineUtils()
    
    @property
    @abstractmethod
    def step_number(self) -> int:
        """步骤编号"""
        pass
    
    @property
    @abstractmethod
    def step_name(self) -> str:
        """步骤名称"""
        pass
    
    @abstractmethod
    def run(self, episode_id: str = None) -> Dict[str, Any]:
        """运行步骤"""
        pass
    
    def check_dependencies(self, episode_id: str = None) -> bool:
        """检查依赖"""
        return True
    
    def get_output_files(self, episode_id: str = None) -> List[str]:
        """获取输出文件列表"""
        return []

# 导入所有步骤
from .step0_upload import Step0Upload
from .step0_1_asr import Step0_1ASR
from .step0_2_clue_extraction import Step0_2ClueExtraction
# from .step0_3_global_alignment import Step0_3GlobalAlignment
from .step0_4_speaker_calibration import Step0_4SpeakerCalibration
from .step0_3_global_alignment_llm import Step0_3GlobalAlignmentLLM
from .step0_5_integrated_analysis import Step0_5IntegratedAnalysis
from .step0_6_plot_extraction import Step0_6PlotExtraction
from .step0_7_script_writing import Step0_7ScriptWriting
from .step0_8_final_script import Step0_8FinalScript
# from .step1_summary import Step1Summary
# from .step2_consolidation import Step2Consolidation
# from .step3_asr import Step3ASR
# from .step4_story import Step4Story
# from .step5_export import Step5Export
# from .step6_merge import Step6Merge

__all__ = [
    'PipelineStep',
    'Step0Upload',
    'Step0_1ASR',
    'Step0_2ClueExtraction',
    # 'Step0_3GlobalAlignment',
    'Step0_4SpeakerCalibration',
    'Step0_3GlobalAlignmentLLM',
    'Step0_5IntegratedAnalysis',
    'Step0_6PlotExtraction',
    'Step0_7ScriptWriting',
    'Step0_8FinalScript',
    # 'Step1Summary', 
    # 'Step2Consolidation',
    # 'Step3ASR',
    # 'Step4Story',
    # 'Step5Export',
    # 'Step6Merge'
]


