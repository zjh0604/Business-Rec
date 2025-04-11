import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
import logging
import time
from typing import Dict, List, Any
import os
import threading
from queue import Queue

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class APITokenManager:
    _instance = None
    _token = None
    _lock = threading.Lock()
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
    def __init__(self):
        self.base_url = "https://api.sohuglobal.com"
        self.user_operations = {
            "operations": [],
            "last_update": None
        }
        self.cache_dir = "cache"
        os.makedirs(self.cache_dir, exist_ok=True)

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

                async with aiohttp.ClientSession() as session:
                    async with session.request(method, url, headers=headers, params=params) as response:
                        if response.status == 401:  # Token 失效
                            APITokenManager._token = None
                            continue
                        response.raise_for_status()
                        return await response.json()

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
        return response.get("data", [])

    async def get_user_behavior_detail(self, record_id: int) -> Dict:
        """获取用户行为记录详细信息"""
        endpoint = f"/open/user/behavior/{record_id}"
        return await self._make_request(endpoint)

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
        return response.get("data", {}).get("list", [])

    async def get_intro_detail(self, busy_code: int, busy_type: str) -> Dict:
        """获取简介详情"""
        endpoint = f"/app/api/content/{busy_type.lower()}/{busy_code}"
        return await self._make_request(endpoint)

    async def process_user_behavior(self, user_id: int) -> None:
        """处理用户行为数据并保存到user_operations"""
        logger.info(f"开始处理用户 {user_id} 的行为数据")
        
        try:
            # 获取用户行为列表
            behaviors = await self.get_user_behavior_list(user_id)
            logger.info(f"找到 {len(behaviors)} 条用户行为记录")

            # 使用信号量限制并发请求数
            semaphore = asyncio.Semaphore(5)
            tasks = []

            async def process_behavior(behavior):
                async with semaphore:
                    try:
                        # 获取行为详情
                        detail = await self.get_user_behavior_detail(behavior["id"])
                        if not detail.get("data"):
                            return None

                        behavior_data = detail["data"]
                        oper_result = json.loads(behavior_data.get("operResult", "{}"))

                        # 获取评论和简介详情（如果适用）
                        comments = []
                        intro = {}
                        if oper_result.get("businessId") and behavior_data.get("businessType"):
                            if behavior_data["operaType"] in [3, 4, 5]:  # 点赞、评论、收藏
                                comments = await self.get_comments(
                                    oper_result["businessId"],
                                    behavior_data["businessType"]
                                )
                                intro = await self.get_intro_detail(
                                    oper_result["businessId"],
                                    behavior_data["businessType"]
                                )

                        # 构建操作记录
                        return {
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
                                "ip": behavior_data.get("operIp")
                            }
                        }

                    except Exception as e:
                        logger.error(f"处理行为记录时出错: {str(e)}")
                        return None

            # 创建所有任务
            for behavior in behaviors:
                tasks.append(asyncio.create_task(process_behavior(behavior)))

            # 等待所有任务完成
            results = await asyncio.gather(*tasks)

            # 过滤掉None值并添加到操作列表
            self.user_operations["operations"].extend([r for r in results if r is not None])

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
        if not intro or not intro.get("data"):
            return {}
        
        data = intro["data"]
        return {
            "view_count": data.get("viewCount", 0),
            "comment_count": data.get("commentCount", 0),
            "praise_count": data.get("praiseCount", 0),
            "collect_count": data.get("collectCount", 0),
            "forward_count": data.get("forwardCount", 0)
        }

    def save_to_file(self, filename: str = "user_operations.json") -> None:
        """保存数据到JSON文件"""
        self.user_operations["last_update"] = datetime.now().isoformat()
        
        try:
            # 保存到缓存目录
            cache_file = os.path.join(self.cache_dir, filename)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.user_operations, f, ensure_ascii=False, indent=2)
            logger.info(f"数据已保存到 {cache_file}")

            # 复制到主目录
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.user_operations, f, ensure_ascii=False, indent=2)
            logger.info(f"数据已复制到 {filename}")
        except Exception as e:
            logger.error(f"保存文件时出错: {str(e)}")

async def main():
    # 创建数据采集器
    collector = DataCollector()

    # 要采集的用户ID列表
    user_ids = [1, 2, 3]  # 替换为要采集的用户ID列表

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