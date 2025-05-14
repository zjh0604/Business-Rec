import chromadb
from chromadb.config import Settings
import json
import logging
from typing import List, Dict, Any
import numpy as np
from sentence_transformers import SentenceTransformer
import traceback


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
        # 使用新的客户端构造方法
        self.client = chromadb.PersistentClient(path="business_vector_db")
        self.collection_name = collection_name
        self.model = SentenceTransformer('./text2vec-base-chinese')
        
        # 创建或获取集合
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
        return f"""
        角色: {_get_field(order, 'corresponding_role')}
        分类: {_get_field(order, 'classification')}
        标题: {_get_field(order, 'wish_title')}
        详情: {_get_field(order, 'wish_details')}
        """

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
        logger.info(f"prepared text: {self._prepare_order_text(order)}")
        try:
            query_text = self._prepare_order_text(order)
            query_embedding = self._get_embedding(query_text)
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results
            )
            similar_orders = []
            if results and results['metadatas']:
                for metadata in results['metadatas'][0]:
                    similar_orders.append(metadata)
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