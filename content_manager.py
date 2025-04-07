import json
import logging
from datetime import datetime
from sohu_api import SohuGlobalAPI
import chromadb
from my_qianfan_llm import llm  # 导入模型

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ContentManager:
    def __init__(self):
        self.sohu_api = SohuGlobalAPI("https://api.sohuglobal.com")
        # 使用已有的 ChromaDB 配置
        self.chroma_client = chromadb.PersistentClient(path="./dbs")
        
        # 创建或获取商品集合
        self.product_collection = self.chroma_client.get_or_create_collection(
            name="products",
            metadata={"description": "商品数据集合"}
        )
        
        # 创建或获取内容集合
        self.content_collection = self.chroma_client.get_or_create_collection(
            name="contents",
            metadata={"description": "图文和视频内容集合"}
        )

    def fetch_and_store_products(self):
        """获取商品数据并存储到向量数据库"""
        try:
            # 登录获取 token
            if not self.sohu_api.login(phone="admin", password="U9xbHDJUH1pmx9hk7nXbQQ=="):
                logger.error("Failed to login to Sohu API")
                return False

            # 获取商品列表
            page_num = 1
            page_size = 50
            total_products = 0
            
            while True:
                response = self.sohu_api.get_products(page_size=page_size, page_num=page_num, type=1)
                if not response or response.get('code') != 200 or not response.get('data'):
                    break

                products = response['data']
                for product in products:
                    # 构建商品描述文本
                    product_text = f"{product.get('storeName', '')}\n{product.get('storeInfo', '')}\n类别：{product.get('categoryId', '')}\n价格：{product.get('price', 0)}"
                    
                    # 确保所有 metadata 值都是有效的类型
                    metadata = {
                        "id": str(product.get('id', '')),
                        "name": str(product.get('storeName', '')),
                        "description": str(product.get('storeInfo', '')),
                        "category": str(product.get('categoryId', '')),
                        "price": float(product.get('price', 0)),
                        "image_url": str(product.get('image', '')),
                        "type": "product",
                        "update_time": datetime.now().isoformat()
                    }
                    
                    # 存储到向量数据库
                    self.product_collection.add(
                        documents=[product_text],
                        metadatas=[metadata],
                        ids=[f"product_{product.get('id', '')}"]
                    )
                    total_products += 1

                page_num += 1
                if page_num > response.get('total', 1):
                    break

            logger.info(f"Successfully stored {total_products} products")
            return True
        except Exception as e:
            logger.error(f"Error fetching and storing products: {str(e)}")
            return False

    def fetch_and_store_contents(self):
        """获取图文和视频内容并存储到向量数据库"""
        try:
            # 确保已登录
            if not self.sohu_api.login(phone="admin", password="U9xbHDJUH1pmx9hk7nXbQQ=="):
                logger.error("Failed to login to Sohu API")
                return False

            total_contents = 0
            # 获取图文内容
            total_contents += self._fetch_and_store_content_type("Article")
            # 获取视频内容
            total_contents += self._fetch_and_store_content_type("Video")

            logger.info(f"Successfully stored {total_contents} contents")
            return True
        except Exception as e:
            logger.error(f"Error fetching and storing contents: {str(e)}")
            return False

    def _fetch_and_store_content_type(self, content_type):
        """获取并存储特定类型的内容"""
        page_num = 1
        page_size = 50
        total_contents = 0
        
        while True:
            response = self.sohu_api.get_content_list(
                page_size=page_size,
                page_num=page_num,
                state="OnShelf",
                busy_type=content_type
            )
            
            if not response or response.get('code') != 200 or not response.get('data'):
                break

            contents = response['data']
            for content in contents:
                # 获取内容详情
                details_response = self.sohu_api.get_content_details(content.get('id'))
                details = details_response.get('data', {}) if details_response and details_response.get('code') == 200 else {}

                # 构建内容描述文本
                content_text = f"{content.get('title', '')}\n{content.get('intro', '')}\n"
                if details:
                    content_text += f"阅读数：{details.get('viewCount', 0)}\n"
                    content_text += f"点赞数：{details.get('praiseCount', 0)}\n"
                    content_text += f"评论数：{details.get('commentCount', 0)}\n"

                # 确保所有 metadata 值都是有效的类型
                metadata = {
                    "id": str(content.get('id', '')),
                    "title": str(content.get('title', '')),
                    "description": str(content.get('intro', '')),
                    "type": content_type,
                    "image_url": str(content.get('coverImage', '')),
                    "video_url": str(content.get('sohuVideoVos', '')) if content_type == "Video" else "",
                    "view_count": int(content.get('viewCount', 0)),
                    "praise_count": int(content.get('praiseCount', 0)),
                    "comment_count": int(content.get('commentCount', 0)),
                    "update_time": datetime.now().isoformat()
                }
                
                # 存储到向量数据库
                self.content_collection.add(
                    documents=[content_text],
                    metadatas=[metadata],
                    ids=[f"{content_type.lower()}_{content.get('id', '')}"]
                )
                total_contents += 1

            page_num += 1
            if page_num > response.get('total', 1):
                break
                
        return total_contents

    def _generate_recommendation_reason(self, item_type, item_info, personality_data):
        """使用模型生成个性化的推荐理由"""
        try:
            # 构建提示词
            prompt = f"""
            请根据用户的性格特征和{item_type}信息，生成一个个性化的推荐理由。
            
            用户性格特征：
            {json.dumps(personality_data, ensure_ascii=False, indent=2)}
            
            {item_type}信息：
            {json.dumps(item_info, ensure_ascii=False, indent=2)}
            
            请分析用户的性格特征，说明为什么这个{item_type}适合该用户。
            要求：
            1. 分析要具体，结合用户的性格特征
            2. 语言要自然流畅
            3. 字数在50-100字之间
            4. 直接给出推荐理由，不要有其他解释
            """
            
            # 调用模型生成推荐理由
            response = llm(prompt)
            return response.strip()
        except Exception as e:
            logger.error(f"Error generating recommendation reason: {str(e)}")
            return f"根据您的性格特征，这个{item_type}很适合您"

    def get_recommendations(self, personality_data, limit=3):
        """根据用户性格特征获取推荐内容"""
        try:
            # 构建查询文本
            query_text = "寻找适合以下性格特征的内容：\n"
            for trait, score in personality_data.items():
                query_text += f"{trait}: {score}\n"

            # 从商品集合中查询
            product_results = self.product_collection.query(
                query_texts=[query_text],
                n_results=limit
            )

            # 从内容集合中查询
            content_results = self.content_collection.query(
                query_texts=[query_text],
                n_results=limit
            )

            # 合并结果
            recommendations = []
            
            # 处理商品推荐
            if product_results and product_results['metadatas']:
                for metadata in product_results['metadatas'][0]:
                    item_info = {
                        "name": str(metadata.get("name", "")),
                        "description": str(metadata.get("description", "")),
                        "category": str(metadata.get("category", "")),
                        "price": float(metadata.get("price", 0))
                    }
                    
                    # 生成个性化的推荐理由
                    reason = self._generate_recommendation_reason("商品", item_info, personality_data)
                    
                    recommendations.append({
                        "name": item_info["name"],
                        "description": item_info["description"],
                        "reason": reason,
                        "score": 0.9,  # 这里可以根据实际相似度计算分数
                        "image_url": str(metadata.get("image_url", "")),
                        "type": "product",
                        "price": item_info["price"]
                    })

            # 处理内容推荐
            if content_results and content_results['metadatas']:
                for metadata in content_results['metadatas'][0]:
                    item_info = {
                        "title": str(metadata.get("title", "")),
                        "description": str(metadata.get("description", "")),
                        "type": str(metadata.get("type", "")),
                        "view_count": int(metadata.get("view_count", 0)),
                        "praise_count": int(metadata.get("praise_count", 0))
                    }
                    
                    # 生成个性化的推荐理由
                    reason = self._generate_recommendation_reason("内容", item_info, personality_data)
                    
                    recommendations.append({
                        "name": item_info["title"],
                        "description": item_info["description"],
                        "reason": reason,
                        "score": 0.85,  # 这里可以根据实际相似度计算分数
                        "image_url": str(metadata.get("image_url", "")),
                        "type": item_info["type"].lower(),
                        "view_count": item_info["view_count"],
                        "praise_count": item_info["praise_count"]
                    })

            # 按分数排序
            recommendations.sort(key=lambda x: x["score"], reverse=True)
            return recommendations[:limit]

        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            return []

# 创建单例实例
content_manager = ContentManager()