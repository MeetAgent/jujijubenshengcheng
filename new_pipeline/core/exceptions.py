"""
流水线异常定义
"""

class PipelineError(Exception):
    """流水线基础异常"""
    pass

class StepDependencyError(PipelineError):
    """步骤依赖错误"""
    pass

class ModelCallError(PipelineError):
    """模型调用错误"""
    pass

class ConfigError(PipelineError):
    """配置错误"""
    pass

class FileNotFoundError(PipelineError):
    """文件未找到错误"""
    pass

class ValidationError(PipelineError):
    """数据验证错误"""
    pass


