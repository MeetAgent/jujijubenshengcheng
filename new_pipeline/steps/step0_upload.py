"""
Step0: 上传准备
将本地视频文件上传到Google Cloud Storage
"""

import os
import glob
import time
from typing import Dict, Any, List
from google.cloud import storage
from google.cloud.exceptions import NotFound

from core import PipelineConfig, GenAIClient, PipelineUtils
from core.exceptions import StepDependencyError, FileNotFoundError
from . import PipelineStep

class Step0Upload(PipelineStep):
    """Step0: 上传视频到GCS"""
    
    @property
    def step_number(self) -> int:
        return 0
    
    @property
    def step_name(self) -> str:
        return "upload"
    
    def check_dependencies(self, episode_id: str = None) -> bool:
        """检查依赖"""
        # Step0没有前置依赖
        return True
    
    def get_output_files(self, episode_id: str = None) -> list:
        """获取输出文件列表"""
        if episode_id:
            return [f"{episode_id}/0_gcs_path.txt"]
        return []
    
    def run(self, episode_id: str = None) -> Dict[str, Any]:
        """运行上传步骤"""
        if episode_id:
            return self._run_single_episode(episode_id)
        else:
            return self._run_all_episodes()
    
    def _run_single_episode(self, episode_id: str, retry_count: int = 0) -> Dict[str, Any]:
        """处理单个剧集"""
        print(f"Step0: 处理 {episode_id}")
        
        # 计算集合根（可选）：将 gcs_path.txt 写入集合/episode_XXX
        out_collection = os.environ.get('STEP0_OUT_COLLECTION') or ''
        base_output = os.path.abspath(os.path.join(self.config.output_dir, out_collection)) if out_collection else os.path.abspath(self.config.output_dir)
        ep_out_dir = os.path.join(base_output, episode_id)
        os.makedirs(ep_out_dir, exist_ok=True)
        output_file = os.path.join(ep_out_dir, "0_gcs_path.txt")
        
        if os.path.exists(output_file):
            print(f"✅ {episode_id} 已有0_gcs_path.txt，跳过上传")
            with open(output_file, 'r', encoding='utf-8') as f:
                gcs_path = f.read().strip()
            return {"status": "already_exists", "gcs_path": gcs_path}
        
        # 优先：从输出目录读取本地视频路径标记
        mark_file = os.path.join(ep_out_dir, "_local_mp4_path.txt")
        mp4_files = []
        if os.path.exists(mark_file):
            try:
                with open(mark_file, 'r', encoding='utf-8') as f:
                    p = f.read().strip()
                    if p and os.path.isfile(p):
                        mp4_files = [p]
            except Exception:
                pass
        # 回退：在 project_root/episode_xxx 下查找 mp4（老路径兼容）
        if not mp4_files:
            episode_dir = os.path.join(self.config.project_root, episode_id)
            mp4_files = glob.glob(os.path.join(episode_dir, "*.mp4"))
        
        if not mp4_files:
            print(f"Warning: {episode_id} 目录中没有找到MP4文件")
            return {"status": "no_mp4_found"}
        
        mp4_file = mp4_files[0]  # 使用第一个MP4文件
        file_name = os.path.basename(mp4_file)
        
        # 带重试的上传
        max_retries = 3
        for attempt in range(max_retries + 1):
            try:
                # 上传到GCS
                gcs_path = self._upload_to_gcs(mp4_file, episode_id, file_name)
                
                # 保存 gcs_path.txt 到集合/episode_XXX
                self.utils.save_text_file(output_file, gcs_path)
                
                print(f"✅ {episode_id} 上传完成: {gcs_path}")
                return {"status": "success", "gcs_path": gcs_path}
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = (attempt + 1) * 2  # 递增等待时间：2, 4, 6秒
                    print(f"⚠️ {episode_id} 上传失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                    print(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ {episode_id} 上传失败 (已重试 {max_retries} 次): {e}")
                    return {"status": "upload_failed", "error": str(e), "retry_count": retry_count + max_retries}
    
    def _run_all_episodes(self) -> Dict[str, Any]:
        """处理所有剧集"""
        import re
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from tqdm import tqdm
        # 读取环境参数：输入目录/输出集合/桶名
        input_dir = os.environ.get('STEP0_INPUT_DIR')
        if not input_dir:
            # 退化：使用 config.project_root 作为输入根
            input_dir = self.config.project_root
        input_dir = os.path.abspath(input_dir)

        collection_name = os.environ.get('STEP0_COLLECTION')
        if not collection_name:
            collection_name = os.path.basename(input_dir.rstrip(os.sep)) + "_output"

        # 输出根目录：在配置的 output_dir 下创建集合目录（使用绝对路径）
        output_root = os.path.abspath(os.path.join(self.config.output_dir, collection_name))
        os.makedirs(output_root, exist_ok=True)

        # 递归搜索输入目录中的视频文件
        exts = {'.mp4', '.MP4', '.mov', '.MOV', '.m4v', '.M4V', '.mkv', '.MKV', '.avi', '.AVI'}
        mp4_paths = []
        for root, dirs, files in os.walk(input_dir):
            for fn in files:
                _, ext = os.path.splitext(fn)
                if ext in exts:
                    mp4_paths.append(os.path.join(root, fn))
        mp4_paths.sort()
        if not mp4_paths:
            print(f"Warning: 在输入目录未找到视频文件: {input_dir}")
            return {"status": "no_input"}

        # 为每个 mp4 生成 episode_xxx 名称：优先使用文件名中的数字
        episodes = []
        used_ids = set()
        next_seq = 1
        import re as _re
        for p in mp4_paths:
            bn = os.path.basename(p)
            m = _re.search(r"(\d+)", bn)
            ep_num = None
            if m:
                try:
                    ep_num = int(m.group(1))
                except Exception:
                    ep_num = None
            if ep_num is None:
                # 分配顺序号（避开已占用）
                while next_seq in used_ids:
                    next_seq += 1
                ep_num = next_seq
                next_seq += 1
            # 处理冲突：若同号已占用，则找下一个空位
            while ep_num in used_ids:
                ep_num += 1
            used_ids.add(ep_num)
            ep_id = f"episode_{ep_num:03d}"
            # 在输出集合根下创建该 episode 目录，并写入占位，供 _run_single_episode 使用
            ep_out_dir = os.path.join(output_root, ep_id)
            os.makedirs(ep_out_dir, exist_ok=True)
            # 将该 episode 的本地文件路径记录到一个临时标记文件，供单集处理读取
            with open(os.path.join(ep_out_dir, "_local_mp4_path.txt"), 'w', encoding='utf-8') as f:
                f.write(p)
            episodes.append(ep_id)

        print(f"开始Step0: 上传 {len(episodes)} 个剧集到GCS -> {output_root} ...")

        # 修改配置的 project_root 指向输出集合根，以复用 _run_single_episode 逻辑
        # 并让 _run_single_episode 读取 _local_mp4_path.txt 作为上传源
        results = []
        # 将集合名传递给子调用
        os.environ['STEP0_OUT_COLLECTION'] = collection_name
        max_workers = self.config.get_step_config(0).get('max_workers', 2)  # 降低并发数，减少网络压力
        # 第一轮上传
        print("🚀 开始第一轮上传...")
        results = self._upload_episodes_batch(episodes, max_workers)
        
        # 统计结果
        success_count = len([r for r in results if r.get("status") == "success"])
        already_exists_count = len([r for r in results if r.get("status") == "already_exists"])
        failed_episodes = [r for r in results if r.get("status") in ["failed", "upload_failed", "no_mp4_found"]]
        failed_count = len(failed_episodes)

        print(f"\n第一轮上传统计:")
        print(f"- 成功上传: {success_count}")
        print(f"- 已存在: {already_exists_count}")
        print(f"- 失败: {failed_count}")

        # 如果有失败的文件，进行重试
        if failed_episodes:
            print(f"\n🔄 开始重试失败的 {len(failed_episodes)} 个文件...")
            failed_episode_ids = [r.get("episode_id") for r in failed_episodes if r.get("episode_id")]
            
            # 重试失败的文件
            retry_results = self._upload_episodes_batch(failed_episode_ids, max_workers)
            
            # 更新结果
            for retry_result in retry_results:
                episode_id = retry_result.get("episode_id")
                if episode_id:
                    # 找到原来的结果并更新
                    for i, original_result in enumerate(results):
                        if original_result.get("episode_id") == episode_id:
                            results[i] = retry_result
                            break
            
            # 重新统计
            success_count = len([r for r in results if r.get("status") == "success"])
            already_exists_count = len([r for r in results if r.get("status") == "already_exists"])
            final_failed_count = len([r for r in results if r.get("status") in ["failed", "upload_failed", "no_mp4_found"]])
            
            print(f"\n重试后最终统计:")
            print(f"- 成功上传: {success_count}")
            print(f"- 已存在: {already_exists_count}")
            print(f"- 失败: {final_failed_count}")
        else:
            print("✅ 所有文件上传成功，无需重试")

        return {
            "status": "completed",
            "total_episodes": len(episodes),
            "success_count": success_count,
            "already_exists_count": already_exists_count,
            "failed_count": len([r for r in results if r.get("status") in ["failed", "upload_failed", "no_mp4_found"]]),
            "results": results
        }
    
    def _upload_episodes_batch(self, episodes: List[str], max_workers: int) -> List[Dict[str, Any]]:
        """批量上传剧集"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from tqdm import tqdm
        
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._run_single_episode, ep): ep for ep in episodes}
            for future in tqdm(as_completed(futures), total=len(episodes), desc="Step0"):
                episode_id = futures[future]
                try:
                    result = future.result()
                    result["episode_id"] = episode_id  # 确保包含episode_id
                    results.append(result)
                except Exception as e:
                    print(f"Step0 处理 {episode_id} 失败: {e}")
                    results.append({"episode_id": episode_id, "status": "failed", "error": str(e)})
        return results
    
    def _upload_to_gcs(self, local_file: str, episode_id: str, file_name: str) -> str:
        """上传文件到GCS"""
        import requests
        from google.cloud.storage import transfer_manager
        
        # 初始化GCS客户端，设置超时
        gcp_config = self.config.gcp_config
        client = storage.Client.from_service_account_json(gcp_config['credentials_path'])
        
        # 构建GCS路径
        bucket_name = os.environ.get('STEP0_BUCKET_NAME', gcp_config.get('bucket_name', 'script-generation-videos'))
        # 若设置了集合名，则按集合名归档；不再使用 episode 子路径
        collection = os.environ.get('STEP0_COLLECTION')
        if collection:
            gcs_key = f"{collection}/{file_name}"
            gcs_path = f"gs://{bucket_name}/{collection}/{file_name}"
        else:
            gcs_key = file_name
            gcs_path = f"gs://{bucket_name}/{file_name}"
        
        try:
            # 检查bucket是否存在
            bucket = client.bucket(bucket_name)
            bucket.reload()  # 验证bucket存在
        except NotFound:
            print(f"⚠️  Bucket {bucket_name} 不存在，尝试创建...")
            try:
                bucket = client.create_bucket(bucket_name, location=gcp_config['location'])
                print(f"✅ 成功创建bucket: {bucket_name}")
            except Exception as e:
                print(f"❌ 无法创建bucket: {e}")
                # 使用默认bucket
                bucket_name = f"{gcp_config['project_id']}-script-videos"
                bucket = client.bucket(bucket_name)
                gcs_path = f"gs://{bucket_name}/{file_name}"
                print(f"🔄 使用默认bucket: {bucket_name}")
        
        # 检查文件是否已存在
        blob = bucket.blob(gcs_key)
        
        if blob.exists():
            print(f"✅ 文件已存在于GCS: {gcs_path}")
            return gcs_path
        
        # 获取文件大小，设置合适的超时时间
        file_size = os.path.getsize(local_file)
        file_size_mb = file_size / (1024 * 1024)
        
        # 根据文件大小设置超时时间（每MB 10秒，最少60秒，最多600秒）
        timeout_seconds = max(60, min(600, int(file_size_mb * 10)))
        
        print(f"📤 上传 {local_file} ({file_size_mb:.1f}MB) 到 {gcs_path} (超时: {timeout_seconds}秒)")
        
        # 使用优化的上传设置
        try:
            # 设置重试策略
            retry_strategy = storage.retry.DEFAULT_RETRY.with_deadline(timeout_seconds)
            
            # 对于大文件，使用更长的超时时间
            if file_size > 50 * 1024 * 1024:  # 50MB
                print(f"🔄 上传大文件 (文件大小: {file_size_mb:.1f}MB)")
                # 大文件使用更长的超时时间
                extended_timeout = max(timeout_seconds, 300)  # 至少5分钟
                blob.upload_from_filename(
                    local_file,
                    timeout=extended_timeout,
                    retry=retry_strategy
                )
            else:
                # 小文件直接上传
                blob.upload_from_filename(
                    local_file,
                    timeout=timeout_seconds,
                    retry=retry_strategy
                )
        except Exception as e:
            # 如果上传失败，尝试更简单的设置
            print(f"⚠️ 标准上传失败，尝试简化上传: {e}")
            try:
                blob.upload_from_filename(local_file)
            except Exception as e2:
                print(f"❌ 简化上传也失败: {e2}")
                raise e2
        
        return gcs_path


