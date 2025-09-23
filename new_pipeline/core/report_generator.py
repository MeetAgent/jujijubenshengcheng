"""
流水线运行报告生成器
"""

import os
import json
import time
from typing import Dict, List, Any
from datetime import datetime


class PipelineReportGenerator:
    """流水线运行报告生成器"""
    
    def __init__(self, config):
        self.config = config
        self.start_time = time.time()
        self.step_results = {}
        self.token_usage = {}
        
    def record_step_start(self, step_name: str):
        """记录步骤开始"""
        self.step_results[step_name] = {
            'start_time': time.time(),
            'status': 'running'
        }
        
    def record_step_end(self, step_name: str, result: Dict[str, Any], client=None):
        """记录步骤结束"""
        if step_name not in self.step_results:
            self.step_results[step_name] = {}
            
        self.step_results[step_name].update({
            'end_time': time.time(),
            'status': result.get('status', 'unknown'),
            'result': result
        })
        
        # 记录 token 使用量
        if client and hasattr(client, 'get_token_usage'):
            self.token_usage[step_name] = client.get_token_usage()
            
    def generate_report(self, output_dir: str) -> str:
        """生成运行报告"""
        total_time = time.time() - self.start_time
        
        # 计算各步骤时间
        step_times = {}
        for step_name, data in self.step_results.items():
            if 'start_time' in data and 'end_time' in data:
                step_times[step_name] = round(data['end_time'] - data['start_time'], 2)
        
        # 计算总 token 使用量
        total_tokens = {
            'input_tokens': 0,
            'output_tokens': 0,
            'total_tokens': 0,
            'call_count': 0
        }
        
        for step_tokens in self.token_usage.values():
            total_tokens['input_tokens'] += step_tokens.get('total_input_tokens', 0)
            total_tokens['output_tokens'] += step_tokens.get('total_output_tokens', 0)
            total_tokens['total_tokens'] += step_tokens.get('total_tokens', 0)
            total_tokens['call_count'] += step_tokens.get('call_count', 0)
        
        # 生成报告内容
        report_content = self._build_report_content(
            total_time, step_times, total_tokens, self.step_results
        )
        
        # 保存报告文件
        report_file = os.path.join(output_dir, "pipeline_execution_report.md")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
            
        return report_file
        
    def _build_report_content(self, total_time: float, step_times: Dict, 
                            total_tokens: Dict, step_results: Dict) -> str:
        """构建报告内容"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        content = f"""# 流水线执行报告

**生成时间**: {timestamp}  
**总执行时间**: {round(total_time, 2)} 秒 ({round(total_time/60, 2)} 分钟)

## 执行概览

| 步骤 | 状态 | 执行时间(秒) | 说明 |
|------|------|-------------|------|
"""
        
        # 步骤状态表格
        step_names = {
            'step0_1': 'Step0.1 ASR',
            'step0_2': 'Step0.2 线索提取', 
            'step0_3': 'Step0.3 全局对齐',
            'step0_4': 'Step0.4 说话人回填',
            'step0_5': 'Step0.5 对话轮次重构',
            'step0_6': 'Step0.6 情节提取',
            'step0_7': 'Step0.7 剧本撰写',
            'step0_8': 'Step0.8 合并导出'
        }
        
        for step_key, step_display in step_names.items():
            status = step_results.get(step_key, {}).get('status', '未执行')
            exec_time = step_times.get(step_key, 0)
            content += f"| {step_display} | {status} | {exec_time} | {self._get_step_description(step_key, step_results.get(step_key, {}))} |\n"
        
        # Token 使用统计
        content += f"""
## Token 使用统计

**总计**:
- 总调用次数: {total_tokens['call_count']}
- 输入 Token: {total_tokens['input_tokens']:,}
- 输出 Token: {total_tokens['output_tokens']:,}
- 总 Token: {total_tokens['total_tokens']:,}

### 各步骤 Token 使用详情

| 步骤 | 调用次数 | 输入Token | 输出Token | 总Token |
|------|----------|-----------|-----------|---------|
"""
        
        for step_key, step_display in step_names.items():
            tokens = self.token_usage.get(step_key, {})
            call_count = tokens.get('call_count', 0)
            input_tokens = tokens.get('total_input_tokens', 0)
            output_tokens = tokens.get('total_output_tokens', 0)
            total_step_tokens = tokens.get('total_tokens', 0)
            
            content += f"| {step_display} | {call_count} | {input_tokens:,} | {output_tokens:,} | {total_step_tokens:,} |\n"
        
        # 详细步骤结果
        content += "\n## 详细步骤结果\n\n"
        
        for step_key, step_display in step_names.items():
            step_data = step_results.get(step_key, {})
            if step_data:
                content += f"### {step_display}\n\n"
                content += f"- **状态**: {step_data.get('status', '未知')}\n"
                content += f"- **执行时间**: {step_times.get(step_key, 0)} 秒\n"
                
                # 添加步骤特定的统计信息
                result = step_data.get('result', {})
                if 'statistics' in result:
                    stats = result['statistics']
                    content += f"- **统计信息**:\n"
                    for key, value in stats.items():
                        if isinstance(value, (int, float)):
                            content += f"  - {key}: {value}\n"
                        else:
                            content += f"  - {key}: {value}\n"
                
                content += "\n"
        
        # 性能分析
        content += "## 性能分析\n\n"
        
        if step_times:
            slowest_step = max(step_times.items(), key=lambda x: x[1])
            fastest_step = min(step_times.items(), key=lambda x: x[1])
            
            content += f"- **最慢步骤**: {step_names.get(slowest_step[0], slowest_step[0])} ({slowest_step[1]} 秒)\n"
            content += f"- **最快步骤**: {step_names.get(fastest_step[0], fastest_step[0])} ({fastest_step[1]} 秒)\n"
            
            avg_time = sum(step_times.values()) / len(step_times)
            content += f"- **平均步骤时间**: {round(avg_time, 2)} 秒\n"
        
        # 成本估算（基于 token 使用量）
        content += "\n## 成本估算\n\n"
        content += f"- **总 Token 消耗**: {total_tokens['total_tokens']:,}\n"
        content += f"- **预估成本**: 约 ${total_tokens['total_tokens'] * 0.00001:.4f} (基于 Gemini 2.0 Flash 定价)\n"
        content += f"- **平均每次调用成本**: 约 ${(total_tokens['total_tokens'] * 0.00001) / max(total_tokens['call_count'], 1):.6f}\n"
        
        return content
        
    def _get_step_description(self, step_key: str, step_data: Dict) -> str:
        """获取步骤描述"""
        descriptions = {
            'step0_1': '语音识别转文字',
            'step0_2': '提取角色线索和关系',
            'step0_3': '全局角色对齐和关系建立',
            'step0_4': '说话人身份校准',
            'step0_5': '对话轮次重构和语义分析',
            'step0_6': '情节提取和剧本流生成',
            'step0_7': 'STMF格式剧本撰写',
            'step0_8': '合并导出Fountain/FDX格式'
        }
        
        base_desc = descriptions.get(step_key, '未知步骤')
        
        # 添加状态相关的额外信息
        result = step_data.get('result', {})
        if 'statistics' in result:
            stats = result['statistics']
            if 'total_episodes' in stats:
                base_desc += f" (处理{stats['total_episodes']}集)"
            elif 'success_count' in stats:
                base_desc += f" (成功{stats['success_count']}个)"
                
        return base_desc
