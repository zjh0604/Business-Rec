import chromadb
from chromadb.config import Settings
import json
import logging
from typing import List, Dict, Any, Tuple
import numpy as np
from sentence_transformers import SentenceTransformer
import traceback
from my_qianfan_llm import llm  # 导入千帆模型


# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FIELD_MAP = {
    "user_id": ["user_id", "User ID"],
    "wish_title": ["wish_title", "Wish title"],
    "corresponding_role": ["corresponding_role", "Corresponding role"],
    "classification": ["classification", "Classification of wishes"],
    "wish_details": ["wish_details", "Details of the wish"],
}

def _get_field(order, key):
    for k in FIELD_MAP.get(key, [key]):
        if k in order:
            return order[k]
    return None

class BusinessVectorDB:
    def __init__(self, collection_name: str = "business_orders"):
        """初始化向量数据库"""
        self.client = chromadb.PersistentClient(path="business_vector_db")
        self.collection_name = collection_name
        self.model = SentenceTransformer('./text2vec-large-chinese')
        
        try:
            self.collection = self.client.get_collection(name=collection_name)
        except:
            self.collection = self.client.create_collection(name=collection_name)
            logger.info(f"Created new collection: {collection_name}")

    def _get_embedding(self, text: str) -> List[float]:
        """获取文本的向量表示"""
        return self.model.encode(text).tolist()

    def _prepare_order_text(self, order: Dict[str, Any]) -> str:
        """将商单信息转换为文本格式"""
        text_parts = []
        
        # 角色信息（如果有）
        role = _get_field(order, 'corresponding_role')
        if role:
            text_parts.append(f"角色: {role}")
        
        # 分类信息（如果有）
        classification = _get_field(order, 'classification')
        if classification:
            text_parts.append(f"分类: {classification}")
        
        # 标题信息（如果有）
        title = _get_field(order, 'wish_title')
        if title:
            text_parts.append(f"标题: {title}")
        
        # 详情信息（如果有）
        details = _get_field(order, 'wish_details')
        if details:
            text_parts.append(f"详情: {details}")
        
        return "\n".join(text_parts)

    def _get_role_prompt(self, role: str) -> str:
        """获取角色相关的提示词"""
        return f"""
        这是一个{role}的商单推荐系统。
        请分析该角色的核心业务需求和痛点：
        1. 重点关注该角色最迫切的需求和痛点
        2. 考虑该角色的业务特点和行业特性
        3. 优先推荐与该角色核心业务相关的商单
        """

    def _analyze_with_llm(self, role: str, orders: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], float]]:
        """使用千帆模型分析商单并返回带权重的商单列表"""
        try:
            # 准备提示词
            prompt = f"""
            作为商单推荐系统的分析专家，请分析以下{role}的商单，并按照业务相关性和需求重要性进行评分（0-1分）。
            评分标准：
            1. 业务相关性：该商单与{role}的核心业务相关程度
            2. 需求重要性：该商单反映的需求对{role}的重要程度
            3. 实现可行性：该商单的实现难度和可行性
            4. 发展潜力：该商单对{role}未来发展的潜在价值

            商单列表：
            {json.dumps(orders, ensure_ascii=False, indent=2)}

            请以JSON格式返回分析结果，格式如下：
            {{
                "analysis": [
                    {{
                        "order_id": "商单ID",
                        "score": 0.85,
                        "reason": "评分理由"
                    }}
                ]
            }}
            """

            # 调用千帆模型
            response = llm.invoke(prompt)
            
            # 解析响应
            analysis = json.loads(response)
            
            # 将分析结果与原始商单合并
            scored_orders = []
            for item in analysis["analysis"]:
                order = next((o for o in orders if str(o.get("id", "")) == item["order_id"]), None)
                if order:
                    scored_orders.append((order, item["score"]))
            
            return scored_orders
        except Exception as e:
            logger.error(f"Error in LLM analysis: {str(e)}")
            return [(order, 0.5) for order in orders]  # 发生错误时返回默认分数

    def add_orders(self, orders: List[Dict[str, Any]]):
        """添加商单到向量数据库"""
        try:
            # 获取当前集合中的最大ID
            current_ids = self.collection.get()['ids']
            start_id = len(current_ids) if current_ids else 0
            
            # 准备数据
            ids = [str(i + start_id) for i in range(len(orders))]
            texts = [self._prepare_order_text(order) for order in orders]
            embeddings = [self._get_embedding(text) for text in texts]
            metadatas = orders  # 存储完整的商单信息

            # 添加到集合
            self.collection.add(
                embeddings=embeddings,
                documents=texts,
                metadatas=metadatas,
                ids=ids
            )
            logger.info(f"Successfully added {len(orders)} orders to vector database")
            return True
        except Exception as e:
            logger.error(f"Error adding orders to vector database: {str(e)}")
            return False

    def find_similar_orders(self, order: Dict[str, Any], n_results: int = 5) -> List[Dict[str, Any]]:
        """查找相似的商单"""
        logger.info(f"find_similar_orders input order: {order}")
        
        try:
            # 检查输入数据完整性
            has_role = bool(_get_field(order, 'corresponding_role'))
            has_title = bool(_get_field(order, 'wish_title'))
            has_details = bool(_get_field(order, 'wish_details'))
            has_classification = bool(_get_field(order, 'classification'))
            
            # 如果只有角色信息，使用角色匹配策略
            if has_role and not (has_title or has_details or has_classification):
                logger.info("Using role-based matching strategy")
                role = _get_field(order, 'corresponding_role')
                role_prompt = self._get_role_prompt(role)
                query_text = f"{role_prompt}\n角色: {role}"
            else:
                # 使用完整的文本匹配策略
                query_text = self._prepare_order_text(order)
            
            logger.info(f"prepared text: {query_text}")
            query_embedding = self._get_embedding(query_text)
            
            # 获取相似商单
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results * 2  # 获取更多结果用于后续分析
            )
            
            similar_orders = []
            if results and results['metadatas']:
                orders = results['metadatas'][0]
                
                if has_role:
                    # 使用千帆模型进行深度分析
                    scored_orders = self._analyze_with_llm(_get_field(order, 'corresponding_role'), orders)
                    # 按LLM分析分数排序
                    scored_orders.sort(key=lambda x: x[1], reverse=True)
                    similar_orders = [order for order, _ in scored_orders[:n_results]]
                else:
                    # 使用向量相似度排序
                    similar_orders = orders[:n_results]
            
            return similar_orders
        except Exception as e:
            logger.error(f"Error finding similar orders: {str(e)}")
            return []

    def load_orders_from_json(self, json_file: str = "user_orders.json"):
        """从JSON文件加载商单到向量数据库"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                new_orders = json.load(f)
            
            # 获取现有商单
            existing_orders = self.get_all_orders()
            
            # 找出新增的商单
            existing_order_ids = set(f"{_get_field(order, 'user_id')}_{_get_field(order, 'wish_title')}" for order in existing_orders)
            orders_to_add = [
                order for order in new_orders 
                if f"{_get_field(order, 'user_id')}_{_get_field(order, 'wish_title')}" not in existing_order_ids
            ]
            
            if not orders_to_add:
                logger.info("No new orders to add")
                return True
            
            # 添加新商单
            success = self.add_orders(orders_to_add)
            if success:
                logger.info(f"Successfully added {len(orders_to_add)} new orders to vector database")
            return success
        except Exception as e:
            logger.error(f"Error loading orders from JSON: {str(e)}")
            return False

    def get_all_orders(self) -> List[Dict[str, Any]]:
        """获取所有商单"""
        try:
            results = self.collection.get()
            return results['metadatas'] if results and results['metadatas'] else []
        except Exception as e:
            logger.error(f"Error getting all orders: {str(e)}")
            return []

    def get_orders_by_role(self, role: str, n_results: int = 5) -> List[Dict[str, Any]]:
        """根据角色获取相关商单"""
        try:
            # 创建一个只包含角色信息的查询对象
            query_order = {"corresponding_role": role}
            return self.find_similar_orders(query_order, n_results)
        except Exception as e:
            logger.error(f"Error getting orders by role: {str(e)}")
            return []

def init_business_vector_db():
    """初始化商单向量数据库，依次加载 orders.json 和 user_orders.json"""
    try:
        vector_db = BusinessVectorDB()
        logger.info("开始从 orders.json 加载商单到向量库...")
        success_orders = vector_db.load_orders_from_json("orders.json")
        logger.info(f"orders.json 加载结果: {success_orders}")
        logger.info("开始从 user_orders.json 加载商单到向量库...")
        success_user_orders = vector_db.load_orders_from_json("user_orders.json")
        logger.info(f"user_orders.json 加载结果: {success_user_orders}")
        return vector_db
    except Exception as e:
        logger.error(f"Error initializing business vector database: {str(e)}")
        logger.error(traceback.format_exc())
        print("Error initializing business vector database:", str(e))
        print(traceback.format_exc())
        return None 