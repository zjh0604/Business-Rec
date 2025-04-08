import json
import sqlite3
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class UserFeedback:
    def __init__(self):
        self.behavior_data = []
        self.score_changes = {
            'view': 1,
            'like': 2,
            'comment': 3,
            'add_to_cart': 4,
            'purchase': 5,
            'no_interaction': -5
        }
        self.db_path = "user.db"

    def record_interaction(self, user_id, content_id, interaction_type, content_type):
        """记录用户交互行为"""
        try:
            logger.debug(f"Recording interaction - User: {user_id}, Content: {content_id}, "
                        f"Type: {interaction_type}, Content Type: {content_type}")
            
            if not all([user_id, content_id, interaction_type, content_type]):
                missing_fields = [field for field, value in {
                    'user_id': user_id,
                    'content_id': content_id,
                    'interaction_type': interaction_type,
                    'content_type': content_type
                }.items() if not value]
                
                error_msg = f"Missing required parameters: {', '.join(missing_fields)}"
                logger.error(error_msg)
                return False
                
            if interaction_type not in self.score_changes:
                logger.error(f"Invalid interaction type: {interaction_type}")
                return False
                
            interaction = {
                'user_id': user_id,
                'content_id': content_id,
                'interaction_type': interaction_type,
                'content_type': content_type,
                'timestamp': datetime.now().isoformat()
            }
            self.behavior_data.append(interaction)
            logger.info(f"Successfully recorded interaction: {interaction}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording interaction: {str(e)}", exc_info=True)
            return False

    def update_personality_scores(self):
        """根据用户交互行为更新性格分数"""
        try:
            # 获取所有用户
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM personality")
            users = cursor.fetchall()
            
            # 为每个用户更新分数
            for user_id, in users:
                # 获取用户当前分数
                cursor.execute("SELECT * FROM personality WHERE id = ?", (user_id,))
                columns = [description[0] for description in cursor.description]
                current_scores = dict(zip(columns[1:], cursor.fetchone()[1:]))
                
                # 获取用户的所有交互
                user_interactions = [i for i in self.behavior_data if i['user_id'] == user_id]
                
                # 计算新的分数
                new_scores = current_scores.copy()
                for trait in new_scores:
                    # 获取该特征相关的交互
                    trait_interactions = [i for i in user_interactions 
                                        if self._is_trait_related(i['content_type'], trait)]
                    
                    if trait_interactions:
                        # 计算交互得分
                        interaction_score = sum(self.score_changes[i['interaction_type']] 
                                             for i in trait_interactions)
                        new_scores[trait] = min(100, max(0, current_scores[trait] + interaction_score))
                    else:
                        # 没有交互，扣分
                        new_scores[trait] = max(0, current_scores[trait] + self.score_changes['no_interaction'])
                
                # 更新数据库
                update_query = f"UPDATE personality SET {', '.join(f'{col} = ?' for col in columns[1:])} WHERE id = ?"
                cursor.execute(update_query, list(new_scores.values()) + [user_id])
                
                # 记录变化
                changes = {trait: new_scores[trait] - current_scores[trait] 
                         for trait in new_scores 
                         if new_scores[trait] != current_scores[trait]}
                
                if changes:
                    logger.info(f"Updated scores for user {user_id}: {changes}")
            
            conn.commit()
            conn.close()
            
            # 清空行为数据
            self.behavior_data = []
            
            return True
        except Exception as e:
            logger.error(f"Error updating personality scores: {str(e)}")
            return False

    def _is_trait_related(self, content_type, trait):
        """判断内容类型是否与性格特征相关"""
        # 定义内容类型与性格特征的关联关系
        trait_content_mapping = {
            '外向性': ['content', 'product'],
            '宜人性': ['content', 'product'],
            '尽责性': ['content', 'product'],
            '神经质': ['content', 'product'],
            '开放性': ['content', 'product']
        }
        return content_type in trait_content_mapping.get(trait, [])

    def get_user_behavior_history(self, user_id, limit=10):
        """获取用户行为历史"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT interaction_type, content_type, score_change, timestamp
                FROM user_behavior
                WHERE user_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (user_id, limit))

            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting user behavior history: {str(e)}")
            return []
        finally:
            conn.close()

    def get_score_changes(self):
        """获取所有用户的分数变化"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 获取所有用户ID
            cursor.execute("SELECT id FROM personality")
            users = cursor.fetchall()
            
            changes = {}
            for user_id, in users:
                # 获取用户的所有交互
                user_interactions = [i for i in self.behavior_data if i['user_id'] == user_id]
                
                if not user_interactions:
                    continue
                
                # 获取所有性格特征
                cursor.execute("SELECT * FROM personality WHERE id = ?", (user_id,))
                columns = [description[0] for description in cursor.description]
                traits = [col for col in columns if col != 'id']
                
                # 计算每个性格特征的变化
                trait_changes = {}
                for trait in traits:
                    # 获取该特征相关的交互
                    trait_interactions = [i for i in user_interactions 
                                        if self._is_trait_related(i['content_type'], trait)]
                    
                    if trait_interactions:
                        # 计算交互得分
                        interaction_score = sum(self.score_changes[i['interaction_type']] 
                                             for i in trait_interactions)
                        trait_changes[trait] = interaction_score
                    else:
                        # 没有交互，扣分
                        trait_changes[trait] = self.score_changes['no_interaction']
                
                if trait_changes:
                    changes[user_id] = trait_changes
            
            conn.close()
            return changes
            
        except Exception as e:
            logger.error(f"Error getting score changes: {str(e)}")
            return {}

# 创建单例实例
user_feedback = UserFeedback()