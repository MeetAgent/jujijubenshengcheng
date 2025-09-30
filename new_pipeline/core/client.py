"""
Google GenAI 客户端封装（Context7/Vertex 兼容）
"""

import json
from typing import Dict, Optional
from google import genai
from google.genai import types as gat
from google.oauth2 import service_account
from .exceptions import ModelCallError


class GenAIClient:
    def __init__(self, config):
        gcp_config = config.gcp_config
        self.client = genai.Client(
            vertexai=True,
            project=gcp_config["project_id"],
            location=gcp_config["location"],
            credentials=service_account.Credentials.from_service_account_file(
                gcp_config["credentials_path"],
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            ),
        )
        # Token 统计
        self.token_usage = {
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_tokens": 0,
            "call_count": 0,
        }

    def generate_content(self, model: str, prompt: str, video_uri: str = None,
                          schema: Dict = None, max_tokens: int = 4096,
                          temperature: float = 0.2, system_instruction: str = None,
                          thinking_budget: int = None,
                          video_fps: int = None) -> Dict:
        
        model_candidates = []
        if model:
            model_candidates.append(model)
            if not model.startswith("models/"):
                model_candidates.append(f"models/{model}")
        else:
            model_candidates.append("gemini-2.0-flash-001")
            model_candidates.append("models/gemini-2.0-flash-001")

        # 构建 parts - 使用 SDK 类型
        parts = []
        
        # 添加文本部分
        parts.append(gat.Part(text=prompt))
        
        # 添加视频部分（如果有）
        if video_uri:
            file_data = gat.FileData(
                file_uri=video_uri,
                mime_type="video/mp4"
            )
            
            # 如果指定了 FPS，添加视频元数据
            if video_fps is not None:
                video_part = gat.Part(
                    file_data=file_data,
                    video_metadata=gat.VideoMetadata(
                        fps=video_fps
                    )
                )
            else:
                # 没有指定 FPS，使用默认
                video_part = gat.Part(file_data=file_data)
            
            parts.append(video_part)

        config_kwargs = {
            "max_output_tokens": max_tokens,
            "temperature": temperature,
            "top_p": 0.8,
            "top_k": 20,
            "safety_settings": [
                gat.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
                gat.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
                gat.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
                gat.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF"),
            ],
        }
        
        if thinking_budget is not None:
            config_kwargs["thinking_config"] = gat.ThinkingConfig(
                thinking_budget=thinking_budget
            )
        
        if system_instruction:
            config_kwargs["system_instruction"] = system_instruction

        if schema:
            config_kwargs.update({
                "response_mime_type": "application/json",
                "response_schema": schema
            })

        last_err = None
        for m in model_candidates:
            try:
                # 使用 Content 对象包装 parts
                content = gat.Content(
                    role="user",
                    parts=parts
                )
                
                resp = self.client.models.generate_content(
                    model=m,
                    contents=content,  # 传递 Content 对象而不是列表
                    config=gat.GenerateContentConfig(**config_kwargs)
                )
                break
            except Exception as e:
                last_err = e
                resp = None
                continue
                
        if resp is None:
            raise ModelCallError(f"模型调用失败: {last_err}")

        # 统计 token 使用量
        if hasattr(resp, 'usage_metadata'):
            usage = resp.usage_metadata
            self.token_usage['total_input_tokens'] += getattr(usage, 'prompt_token_count', 0)
            self.token_usage['total_output_tokens'] += getattr(usage, 'candidates_token_count', 0)
            self.token_usage['total_tokens'] += getattr(usage, 'total_token_count', 0)
            self.token_usage['call_count'] += 1

        if schema:
            if resp.text:
                try:
                    parsed = json.loads(resp.text)
                    if isinstance(parsed, dict) and 'dialogues' in parsed:
                        print(f"✅ JSON解析成功: {len(parsed.get('dialogues', []))}条对话")
                    else:
                        print("✅ JSON解析成功")
                    return parsed
                except json.JSONDecodeError as e:
                    print(f"❌ JSON解析失败: {e}")
                    print(f"原始返回: {resp.text[:200]}...")
                    return self._get_default_schema_result(schema)
            print("❌ 响应为空")
            return self._get_default_schema_result(schema)
        return {"text": resp.text}


    def _get_default_schema_result(self, schema: Dict) -> Dict:
        if not schema or "properties" not in schema:
            return {}
        result = {}
        props = schema.get("properties", {})
        if "dialogues" in props:
            result["dialogues"] = []
        if "characters" in props:
            result["characters"] = []
        return result

    def get_token_usage(self) -> Dict:
        """获取 token 使用统计"""
        return self.token_usage.copy()

    def reset_token_usage(self):
        """重置 token 统计"""
        self.token_usage = {
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_tokens": 0,
            "call_count": 0,
        }
