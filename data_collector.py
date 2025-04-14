import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
import logging
import time
from typing import Dict, List, Any
import os

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
    def __init__(self):
        self.base_url = "https://api.sohuglobal.com"
        self.user_operations = {
            "operations": [],
            "last_update": None
        }
        self.cache_dir = "cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 清空缓存文件
        cache_file = os.path.join(self.cache_dir, "user_operations.json")
        if os.path.exists(cache_file):
            os.remove(cache_file)
        if os.path.exists("user_operations.json"):
            os.remove("user_operations.json")

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
        
        # 检查响应中是否包含 data 字段
        if "data" not in response:
            logger.error(f"行为记录 {record_id} 的响应中没有 data 字段")
            return {}
        
        # 记录 data 字段的类型和内容
        data = response["data"]
        logger.debug(f"行为记录 {record_id} 的 data 字段类型: {type(data)}")
        logger.debug(f"行为记录 {record_id} 的 data 字段内容: {data}")
        
        # 如果 data 不是字典类型，尝试将其转换为字典
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
        if not isinstance(data, dict):
            logger.error(f"评论列表数据不是字典类型: {type(data)}")
            return []
        comments = data.get("list", [])
        if not isinstance(comments, list):
            logger.error(f"评论数据不是列表类型: {type(comments)}")
            return []
        return comments

    async def get_intro_detail(self, busy_code: int, busy_type: str) -> Dict:
        """获取简介详情"""
        endpoint = f"/app/api/content/{busy_type.lower()}/{busy_code}"
        response = await self._make_request(endpoint)
        if not response or not isinstance(response, dict):
            logger.error(f"获取简介详情时返回无效响应")
            return {}
        return response

    async def process_user_behavior(self, user_id: int) -> None:
        """处理用户行为数据并保存到user_operations"""
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

            async def process_behavior(behavior):
                async with semaphore:
                    try:
                        # 获取行为详情
                        detail = await self.get_user_behavior_detail(behavior["id"])
                        if not detail or not isinstance(detail, dict):
                            logger.debug(f"行为记录 {behavior['id']} 没有详情数据，跳过")
                            return None

                        # 检查 data 字段是否存在且为字典
                        if "data" not in detail or not isinstance(detail["data"], dict):
                            logger.debug(f"行为记录 {behavior['id']} 的 data 字段无效，跳过")
                            return None

                        behavior_data = detail["data"]
                        oper_result_str = behavior_data.get("operResult", "")
                        
                        # 如果 operResult 为空，跳过这条记录
                        if not oper_result_str:
                            logger.debug(f"行为记录 {behavior['id']} 的 operResult 为空，跳过")
                            return None

                        try:
                            oper_result = json.loads(oper_result_str)
                            logger.debug(f"解析后的 operResult 类型: {type(oper_result)}")
                            logger.debug(f"解析后的 operResult 内容: {oper_result}")

                            # 如果 operResult 是列表，取第一个元素
                            if isinstance(oper_result, list):
                                if not oper_result:
                                    logger.debug(f"行为记录 {behavior['id']} 的 operResult 是空列表，跳过")
                                    return None
                                oper_result = oper_result[0]
                                logger.debug(f"使用列表中的第一个元素: {oper_result}")

                            if not isinstance(oper_result, dict):
                                logger.error(f"解析后的 operResult 不是字典类型: {type(oper_result)}")
                                return None

                        except json.JSONDecodeError:
                            logger.error(f"解析 operResult 失败: {oper_result_str}")
                            return None

                        # 获取评论和简介详情（如果适用）
                        comments = []
                        intro = {}
                        description = None  # 新增：用于存储简介信息

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
                                
                                # 从 intro 中提取简介信息
                                if intro and isinstance(intro, dict) and intro.get("data"):
                                    intro_data = intro["data"]
                                    # 根据不同的业务类型提取简介
                                    if behavior_data["businessType"] == "Video":
                                        description = intro_data.get("description")
                                    elif behavior_data["businessType"] == "Article":
                                        description = intro_data.get("content")
                                    # 可以添加其他业务类型的简介提取逻辑

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
                                "intro_stats": self._extract_intro_stats(intro)
                            }
                        }

                        # 如果有简介信息，添加到记录中
                        if description:
                            record["detail"]["description"] = description

                        return record

                    except Exception as e:
                        logger.error(f"处理行为记录时出错: {str(e)}")
                        logger.error(f"错误发生时的行为数据: {behavior}")
                        return None

            # 创建所有任务
            for behavior in behaviors:
                tasks.append(asyncio.create_task(process_behavior(behavior)))

            # 等待所有任务完成
            results = await asyncio.gather(*tasks)

            # 过滤掉None值并添加到操作列表
            valid_results = [r for r in results if r is not None]
            self.user_operations["operations"].extend(valid_results)
            logger.info(f"成功处理 {len(valid_results)} 条有效行为记录")

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
    user_ids = [1, 5]  # 要采集的用户ID列表

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