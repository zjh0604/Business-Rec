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

    def get_recommendations(self, personality_data, limit=5):
        """根据用户性格特征获取推荐内容"""
        try:
            logger.debug(f"Received personality data: {personality_data}")
            
            # 检查数据库中的内容数量
            product_count = self.product_collection.count()
            content_count = self.content_collection.count()
            logger.debug(f"Database content count - Products: {product_count}, Contents: {content_count}")
            
            if product_count == 0 and content_count == 0:
                logger.warning("No content found in database")
                return []
            
            # 找出分数较高的性格特征（取前5-10个）
            valid_traits = {trait: score for trait, score in personality_data.items() 
                          if score is not None}
            if not valid_traits:
                logger.warning("No valid personality scores found")
                return []
            
            # 按分数排序并选择前5-10个特征
            sorted_traits = sorted(valid_traits.items(), key=lambda x: x[1], reverse=True)
            top_traits = sorted_traits[:min(10, len(sorted_traits))]  # 最多取前10个特征
            logger.debug(f"Selected top traits: {top_traits}")
            
            if not top_traits:
                logger.warning("No significant traits found")
                return []
            
            recommendations = []
            seen_items = set()  # 用于追踪已推荐的商品/内容
            
            # 为每个性格特征获取推荐，直到达到目标数量
            for trait, score in top_traits:
                # 如果已经收集到足够的推荐，就停止
                if len(recommendations) >= limit:
                    break
                    
                logger.debug(f"Processing trait: {trait} with score {score}")
                
                # 构建针对单个特征的查询文本
                query_text = f"适合{trait}分数为{score}的用户喜欢的内容和商品"
                logger.debug(f"Query text for trait {trait}: {query_text}")
                
                # 从商品集合中查询
                product_results = self.product_collection.query(
                    query_texts=[query_text],
                    n_results=2  # 每个特征取少量结果
                )
                
                # 从内容集合中查询
                content_results = self.content_collection.query(
                    query_texts=[query_text],
                    n_results=2  # 每个特征取少量结果
                )
                
                # 处理商品推荐
                if product_results and product_results['metadatas']:
                    for metadata in product_results['metadatas'][0]:
                        # 检查是否已达到推荐数量限制
                        if len(recommendations) >= limit:
                            break
                            
                        try:
                            item_key = (str(metadata.get("name", "")), "product")
                            # 跳过已推荐的商品
                            if item_key in seen_items:
                                continue
                                
                            item_info = {
                                "name": str(metadata.get("name", "")),
                                "description": str(metadata.get("description", "")),
                                "category": str(metadata.get("category", "")),
                                "price": float(metadata.get("price", 0))
                            }
                            
                            # 生成推荐理由，使用当前特征
                            reason = self._generate_recommendation_reason("商品", item_info, {trait: score})
                            
                            recommendation = {
                                "id": str(metadata.get("id", "")),
                                "name": item_info["name"],
                                "description": item_info["description"],
                                "reason": reason,
                                "score": score,  # 使用性格特征分数作为推荐分数
                                "image_url": str(metadata.get("image_url", "")),
                                "type": "product",
                                "price": item_info["price"],
                                "significant_traits": [trait]  # 使用当前特征
                            }
                            logger.debug(f"Adding product recommendation with trait {trait}: {recommendation['name']}")
                            recommendations.append(recommendation)
                            seen_items.add(item_key)
                            
                        except Exception as e:
                            logger.error(f"Error processing product metadata: {str(e)}")
                            continue
                
                # 处理内容推荐
                if content_results and content_results['metadatas']:
                    for metadata in content_results['metadatas'][0]:
                        # 检查是否已达到推荐数量限制
                        if len(recommendations) >= limit:
                            break
                            
                        try:
                            item_key = (str(metadata.get("title", "")), "content")
                            # 跳过已推荐的内容
                            if item_key in seen_items:
                                continue
                                
                            item_info = {
                                "title": str(metadata.get("title", "")),
                                "description": str(metadata.get("description", "")),
                                "type": str(metadata.get("type", "")),
                                "view_count": int(metadata.get("view_count", 0)),
                                "praise_count": int(metadata.get("praise_count", 0))
                            }
                            
                            # 生成推荐理由，使用当前特征
                            reason = self._generate_recommendation_reason("内容", item_info, {trait: score})
                            
                            recommendation = {
                                "id": str(metadata.get("id", "")),
                                "name": item_info["title"],
                                "description": item_info["description"],
                                "reason": reason,
                                "score": score,  # 使用性格特征分数作为推荐分数
                                "image_url": str(metadata.get("image_url", "")),
                                "type": item_info["type"].lower(),
                                "view_count": item_info["view_count"],
                                "praise_count": item_info["praise_count"],
                                "significant_traits": [trait]  # 使用当前特征
                            }
                            logger.debug(f"Adding content recommendation with trait {trait}: {recommendation['name']}")
                            recommendations.append(recommendation)
                            seen_items.add(item_key)
                            
                        except Exception as e:
                            logger.error(f"Error processing content metadata: {str(e)}")
                            continue
                
                # 如果这个特征没有找到任何推荐，继续下一个特征
                if len(recommendations) == 0:
                    continue
            
            # 按分数排序
            sorted_recommendations = sorted(recommendations, key=lambda x: x["score"], reverse=True)
            logger.debug("Final recommendations:")
            for rec in sorted_recommendations[:limit]:
                logger.debug(f"- {rec['name']} (trait: {rec['significant_traits'][0]})")
            
            return sorted_recommendations[:limit]

        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            return []

# 创建单例实例
content_manager = ContentManager()