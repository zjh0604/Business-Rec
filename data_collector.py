import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
import logging
import time
from typing import Dict, List, Any
import os
import sqlite3
from contextlib import asynccontextmanager
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class APITokenManager:
    _instance = None
    _token = None
    _lock = asyncio.Lock()
    _last_update = None
    _token_expiry = timedelta(hours=1)  # Token 有效期为1小时

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(APITokenManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    async def get_token(cls):
        if cls._token is None or cls._is_token_expired():
            async with cls._lock:
                if cls._token is None or cls._is_token_expired():
                    cls._token = await cls._fetch_new_token()
                    cls._last_update = datetime.now()
        return cls._token

    @classmethod
    def _is_token_expired(cls):
        if cls._last_update is None:
            return True
        return datetime.now() - cls._last_update > cls._token_expiry

    @classmethod
    async def _fetch_new_token(cls):
        """获取新的访问令牌"""
        login_url = "https://api.sohuglobal.com/auth/v2/login"
        login_payload = {
            "phone": "admin",
            "code": "",
            "loginType": "PASSWORD",
            "password": "sohu888888#",
            "userName": "admin"
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(login_url, json=login_payload) as response:
                data = await response.json()
                return data["data"]["accessToken"]

class DataCollector:
    def __init__(self, batch_size=1000, db_path="user.db"):
        self.base_url = "https://api.sohuglobal.com"
        self.batch_size = batch_size
        self.db_path = db_path
        self.cache_dir = "cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 初始化数据库连接
        self.engine = create_engine(f"sqlite:///{db_path}")
        self._init_database()
        
        # 清空缓存文件
        self._clear_cache()

    def _init_database(self):
        """初始化数据库表结构"""
        with self.engine.connect() as conn:
            # 创建用户行为表
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    time TIMESTAMP NOT NULL,
                    business_type TEXT,
                    business_id INTEGER,
                    title TEXT,
                    category TEXT,
                    comments_count INTEGER,
                    view_count INTEGER,
                    praise_count INTEGER,
                    collect_count INTEGER,
                    forward_count INTEGER,
                    description TEXT,
                    duration INTEGER,
                    cover_url TEXT,
                    video_url TEXT,
                    author TEXT,
                    publish_time TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()

    def _clear_cache(self):
        """清空缓存文件"""
        cache_file = os.path.join(self.cache_dir, "user_operations.json")
        if os.path.exists(cache_file):
            os.remove(cache_file)
        if os.path.exists("user_operations.json"):
            os.remove("user_operations.json")

    @asynccontextmanager
    async def _get_session(self):
        """创建和管理aiohttp会话"""
        session = aiohttp.ClientSession()
        try:
            yield session
        finally:
            await session.close()

    async def _make_request(self, endpoint: str, method: str = "GET", params: Dict = None) -> Dict:
        """发送API请求并处理响应"""
        url = f"{self.base_url}{endpoint}"
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                token = await APITokenManager.get_token()
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }

                async with self._get_session() as session:
                    async with session.request(method, url, headers=headers, params=params) as response:
                        if response.status == 401:  # Token 失效
                            APITokenManager._token = None
                            continue
                        response.raise_for_status()
                        data = await response.json()
                        if not isinstance(data, dict):
                            logger.error(f"API响应不是字典类型: {type(data)}")
                            return {}
                        return data

            except aiohttp.ClientError as e:
                logger.error(f"API请求失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                else:
                    raise

    async def get_user_behavior_list(self, user_id: int) -> List[Dict]:
        """获取用户行为记录列表"""
        endpoint = "/open/user/behavior/list"
        params = {
            "userId": user_id,
            "pageSize": 100
        }
        response = await self._make_request(endpoint, params=params)
        if not response or not isinstance(response, dict):
            logger.error(f"获取用户 {user_id} 行为列表时返回无效响应")
            return []
        data = response.get("data", [])
        if not isinstance(data, list):
            logger.error(f"行为列表数据不是列表类型: {type(data)}")
            return []
        return data

    async def get_user_behavior_detail(self, record_id: int) -> Dict:
        """获取用户行为记录详细信息"""
        endpoint = f"/open/user/behavior/{record_id}"
        response = await self._make_request(endpoint)
        if not response or not isinstance(response, dict):
            logger.error(f"获取行为记录 {record_id} 详情时返回无效响应")
            return {}
        
        if "data" not in response:
            logger.error(f"行为记录 {record_id} 的响应中没有 data 字段")
            return {}
        
        data = response["data"]
        if not isinstance(data, dict):
            try:
                if isinstance(data, str):
                    data = json.loads(data)
                elif isinstance(data, list):
                    data = {"items": data}
                else:
                    data = {"value": data}
                response["data"] = data
            except Exception as e:
                logger.error(f"转换 data 字段失败: {str(e)}")
                return {}
        
        return response

    async def get_comments(self, busy_code: int, busy_type: str) -> List[Dict]:
        """获取评论列表"""
        endpoint = "/app/api/common/comment/list"
        params = {
            "busyCode": busy_code,
            "busyType": busy_type,
            "pageNum": 1,
            "pageSize": 50
        }
        response = await self._make_request(endpoint, params=params)
        if not response or not isinstance(response, dict):
            logger.error(f"获取评论列表时返回无效响应")
            return []
        
        data = response.get("data", {})
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            comments = data.get("list", [])
            if isinstance(comments, list):
                return comments
            else:
                logger.error(f"评论数据不是列表类型: {type(comments)}")
                return []
        else:
            logger.error(f"评论列表数据不是预期的类型: {type(data)}")
            return []

    async def get_intro_detail(self, busy_code: int, busy_type: str) -> Dict:
        """获取简介详情"""
        endpoint = f"/app/api/content/{busy_type.lower()}/{busy_code}"
        response = await self._make_request(endpoint)
        if not response or not isinstance(response, dict):
            logger.error(f"获取简介详情时返回无效响应")
            return {}
        
        data = response.get("data", {})
        if not isinstance(data, dict):
            logger.error(f"简介详情数据不是字典类型: {type(data)}")
            return {}
            
        intro_info = {
            "view_count": data.get("viewCount", 0),
            "comment_count": data.get("commentCount", 0),
            "praise_count": data.get("praiseCount", 0),
            "collect_count": data.get("collectCount", 0),
            "forward_count": data.get("forwardCount", 0)
        }
        
        if busy_type == "Video":
            intro_info.update({
                "description": data.get("description", ""),
                "duration": data.get("duration", 0),
                "cover_url": data.get("coverUrl", ""),
                "video_url": data.get("videoUrl", "")
            })
        elif busy_type == "Article":
            intro_info.update({
                "content": data.get("content", ""),
                "cover_url": data.get("coverUrl", ""),
                "author": data.get("author", ""),
                "publish_time": data.get("publishTime", "")
            })
            
        return intro_info

    async def _process_batch(self, batch: List[Dict]):
        """处理一批数据并保存到数据库"""
        if not batch:
            return

        # 准备批量插入的数据
        records = []
        for record in batch:
            records.append({
                'user_id': record['user_id'],
                'action': record['action'],
                'time': record['time'],
                'business_type': record['detail']['business_type'],
                'business_id': record['detail']['business_id'],
                'title': record['detail']['title'],
                'category': record['detail']['category'],
                'comments_count': record['detail']['comments_count'],
                'view_count': record['detail']['intro_stats'].get('view_count', 0),
                'praise_count': record['detail']['intro_stats'].get('praise_count', 0),
                'collect_count': record['detail']['intro_stats'].get('collect_count', 0),
                'forward_count': record['detail']['intro_stats'].get('forward_count', 0),
                'description': record['detail'].get('description', ''),
                'duration': record['detail'].get('duration', 0),
                'cover_url': record['detail'].get('cover_url', ''),
                'video_url': record['detail'].get('video_url', ''),
                'author': record['detail'].get('author', ''),
                'publish_time': record['detail'].get('publish_time', '')
            })

        # 使用pandas进行批量插入
        df = pd.DataFrame(records)
        df.to_sql('user_operations', self.engine, if_exists='append', index=False)

    async def process_user_behavior(self, user_id: int) -> None:
        """处理用户行为数据并保存到数据库"""
        logger.info(f"开始处理用户 {user_id} 的行为数据")
        
        try:
            # 获取用户行为列表
            behaviors = await self.get_user_behavior_list(user_id)
            if not behaviors:
                logger.warning(f"用户 {user_id} 没有行为记录")
                return

            logger.info(f"找到 {len(behaviors)} 条用户行为记录")

            # 使用信号量限制并发请求数
            semaphore = asyncio.Semaphore(5)
            tasks = []
            current_batch = []

            async def process_behavior(behavior):
                async with semaphore:
                    try:
                        # 获取行为详情
                        detail = await self.get_user_behavior_detail(behavior["id"])
                        if not detail or not isinstance(detail, dict):
                            logger.debug(f"行为记录 {behavior['id']} 没有详情数据，跳过")
                            return None

                        if "data" not in detail or not isinstance(detail["data"], dict):
                            logger.debug(f"行为记录 {behavior['id']} 的 data 字段无效，跳过")
                            return None

                        behavior_data = detail["data"]
                        oper_result_str = behavior_data.get("operResult", "")
                        
                        if not oper_result_str:
                            logger.debug(f"行为记录 {behavior['id']} 的 operResult 为空，跳过")
                            return None

                        try:
                            oper_result = json.loads(oper_result_str)
                            if isinstance(oper_result, list):
                                if not oper_result:
                                    logger.debug(f"行为记录 {behavior['id']} 的 operResult 是空列表，跳过")
                                    return None
                                oper_result = oper_result[0]

                            if not isinstance(oper_result, dict):
                                logger.error(f"解析后的 operResult 不是字典类型: {type(oper_result)}")
                                return None

                        except json.JSONDecodeError:
                            logger.error(f"解析 operResult 失败: {oper_result_str}")
                            return None

                        # 获取评论和简介详情
                        comments = []
                        intro = {}
                        description = None

                        if oper_result.get("businessId") and behavior_data.get("businessType"):
                            comments = await self.get_comments(
                                oper_result["businessId"],
                                behavior_data["businessType"]
                            )
                            
                            intro = await self.get_intro_detail(
                                oper_result["businessId"],
                                behavior_data["businessType"]
                            )
                            
                            if behavior_data["businessType"] == "Video":
                                description = intro.get("description", "")
                            elif behavior_data["businessType"] == "Article":
                                description = intro.get("content", "")

                        # 构建操作记录
                        record = {
                            "user_id": str(user_id),
                            "action": self._get_action_type(behavior_data["operaType"]),
                            "time": behavior_data["createTime"],
                            "detail": {
                                "business_type": behavior_data["businessType"],
                                "business_id": oper_result.get("businessId"),
                                "title": oper_result.get("title"),
                                "category": oper_result.get("categoryName"),
                                "comments_count": len(comments),
                                "intro_stats": self._extract_intro_stats(intro),
                                "description": description
                            }
                        }

                        # 添加特定类型的额外信息
                        if behavior_data["businessType"] == "Video":
                            record["detail"].update({
                                "duration": intro.get("duration", 0),
                                "cover_url": intro.get("cover_url", ""),
                                "video_url": intro.get("video_url", "")
                            })
                        elif behavior_data["businessType"] == "Article":
                            record["detail"].update({
                                "cover_url": intro.get("cover_url", ""),
                                "author": intro.get("author", ""),
                                "publish_time": intro.get("publish_time", "")
                            })

                        return record

                    except Exception as e:
                        logger.error(f"处理行为记录时出错: {str(e)}")
                        logger.error(f"错误发生时的行为数据: {behavior}")
                        return None

            # 创建所有任务
            for behavior in behaviors:
                tasks.append(asyncio.create_task(process_behavior(behavior)))

            # 等待所有任务完成并处理结果
            for task in asyncio.as_completed(tasks):
                result = await task
                if result:
                    current_batch.append(result)
                    if len(current_batch) >= self.batch_size:
                        await self._process_batch(current_batch)
                        current_batch = []

            # 处理剩余的记录
            if current_batch:
                await self._process_batch(current_batch)

            logger.info(f"成功处理用户 {user_id} 的行为数据")

        except Exception as e:
            logger.error(f"处理用户 {user_id} 的行为数据时出错: {str(e)}")

    def _get_action_type(self, opera_type: int) -> str:
        """转换操作类型为可读字符串"""
        action_types = {
            1: "view_list",
            2: "view_detail",
            3: "like",
            4: "comment",
            5: "collect",
            6: "follow",
            7: "reward",
            8: "create",
            9: "edit",
            10: "delete",
            11: "offline",
            12: "forward"
        }
        return action_types.get(opera_type, "unknown")

    def _extract_intro_stats(self, intro: Dict) -> Dict:
        """提取简介统计数据"""
        if not intro:
            return {}
        
        return {
            "view_count": intro.get("view_count", 0),
            "comment_count": intro.get("comment_count", 0),
            "praise_count": intro.get("praise_count", 0),
            "collect_count": intro.get("collect_count", 0),
            "forward_count": intro.get("forward_count", 0)
        }

    def save_to_file(self, filename: str = "user_operations.json") -> None:
        """保存数据到JSON文件"""
        try:
            # 从数据库读取数据
            with self.engine.connect() as conn:
                df = pd.read_sql("SELECT * FROM user_operations", conn)
                
                # 转换为JSON格式
                operations = []
                for _, row in df.iterrows():
                    operation = {
                        "user_id": row['user_id'],
                        "action": row['action'],
                        "time": row['time'],
                        "detail": {
                            "business_type": row['business_type'],
                            "business_id": row['business_id'],
                            "title": row['title'],
                            "category": row['category'],
                            "comments_count": row['comments_count'],
                            "intro_stats": {
                                "view_count": row['view_count'],
                                "comment_count": row['comments_count'],
                                "praise_count": row['praise_count'],
                                "collect_count": row['collect_count'],
                                "forward_count": row['forward_count']
                            }
                        }
                    }
                    
                    # 添加特定类型的额外信息
                    if row['business_type'] == "Video":
                        operation["detail"].update({
                            "duration": row['duration'],
                            "cover_url": row['cover_url'],
                            "video_url": row['video_url']
                        })
                    elif row['business_type'] == "Article":
                        operation["detail"].update({
                            "cover_url": row['cover_url'],
                            "author": row['author'],
                            "publish_time": row['publish_time']
                        })
                    
                    operations.append(operation)
                
                data = {
                    "operations": operations,
                    "last_update": datetime.now().isoformat()
                }
                
                # 保存到文件
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.info(f"数据已保存到 {filename}")
                
                # 保存到缓存目录
                cache_file = os.path.join(self.cache_dir, filename)
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.info(f"数据已保存到 {cache_file}")
                
        except Exception as e:
            logger.error(f"保存文件时出错: {str(e)}")

async def main():
    # 创建数据采集器
    collector = DataCollector(batch_size=1000)

    # 要采集的用户ID列表
    user_ids = [1, 8]  # 要采集的用户ID列表

    # 处理每个用户的数据
    for user_id in user_ids:
        try:
            await collector.process_user_behavior(user_id)
        except Exception as e:
            logger.error(f"处理用户 {user_id} 数据时出错: {str(e)}")
            continue

    # 保存数据到文件
    collector.save_to_file()

if __name__ == "__main__":
    asyncio.run(main()) 