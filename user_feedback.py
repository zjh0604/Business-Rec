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

    def record_interaction(self, user_id, content_id, interaction_type, content_type, significant_traits=None):
        """记录用户交互行为"""
        try:
            logger.debug(f"Recording interaction: user_id={user_id}, content_id={content_id}, interaction_type={interaction_type}, content_type={content_type}, significant_traits={significant_traits}")
            
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
                
            # 将交互数据存储到数据库
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建user_behavior表（如果不存在）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_behavior (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    content_id TEXT NOT NULL,
                    interaction_type TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    significant_traits TEXT,
                    timestamp TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES personality (id)
                )
            ''')
            
            # 检查表是否创建成功
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_behavior'")
            if not cursor.fetchone():
                logger.error("Failed to create user_behavior table")
                return False
            
            # 插入交互数据
            cursor.execute('''
                INSERT INTO user_behavior 
                (user_id, content_id, interaction_type, content_type, significant_traits, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                user_id,
                content_id,
                interaction_type,
                content_type,
                json.dumps(significant_traits or []),
                datetime.now().isoformat()
            ))
            
            # 验证数据是否插入成功
            cursor.execute("SELECT COUNT(*) FROM user_behavior WHERE user_id = ?", (user_id,))
            count = cursor.fetchone()[0]
            logger.debug(f"Number of interactions for user {user_id}: {count}")
            
            conn.commit()
            conn.close()
            
            # 同时更新内存中的behavior_data
            interaction = {
                'user_id': user_id,
                'content_id': content_id,
                'interaction_type': interaction_type,
                'content_type': content_type,
                'timestamp': datetime.now().isoformat(),
                'significant_traits': significant_traits or []
            }
            self.behavior_data.append(interaction)
            
            logger.info(f"Successfully recorded interaction: {interaction}")
            logger.debug(f"Behavior data before score changes: {self.behavior_data}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording interaction: {str(e)}", exc_info=True)
            return False

    def update_personality_scores(self):
        """根据用户交互行为更新性格分数"""
        conn = None
        try:
            logger.debug("Starting personality score update...")
            # 获取所有用户
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM personality")
            users = cursor.fetchall()
            logger.debug(f"Found {len(users)} users in database")
            
            # 为每个用户更新分数
            for user_id, in users:
                logger.debug(f"Processing user {user_id}")
                # 获取用户当前分数
                cursor.execute("SELECT * FROM personality WHERE id = ?", (user_id,))
                columns = [description[0] for description in cursor.description]
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"No data found for user {user_id}")
                    continue
                    
                current_scores = dict(zip(columns[1:], row[1:]))
                logger.debug(f"Current scores for user {user_id}: {current_scores}")
                
                # 从数据库获取用户的所有交互
                cursor.execute("""
                    SELECT user_id, content_id, interaction_type, content_type, significant_traits, timestamp
                    FROM user_behavior
                    WHERE user_id = ?
                """, (user_id,))
                
                rows = cursor.fetchall()
                logger.debug(f"Found {len(rows)} interactions for user {user_id} in database")
                
                user_interactions = []
                for row in rows:
                    # 处理性格特征名称，移除"旧的"前缀
                    traits = json.loads(row[4]) if row[4] else []
                    processed_traits = [trait.replace('旧的', '') for trait in traits]
                    
                    interaction = {
                        'user_id': row[0],
                        'content_id': row[1],
                        'interaction_type': row[2],
                        'content_type': row[3],
                        'significant_traits': processed_traits,
                        'timestamp': row[5]
                    }
                    user_interactions.append(interaction)
                    logger.debug(f"Loaded interaction for user {user_id}: {interaction}")
                
                if not user_interactions:
                    logger.debug(f"No interactions found for user {user_id}")
                    continue
                
                # 记录已处理的性格特征
                processed_traits = set()
                
                # 获取推荐内容关联的性格特征
                recommended_traits = set()
                for interaction in user_interactions:
                    if 'significant_traits' in interaction:
                        recommended_traits.update(interaction['significant_traits'])
                logger.debug(f"Recommended traits for user {user_id}: {recommended_traits}")
                
                # 计算新的分数
                new_scores = current_scores.copy()
                
                # 处理有交互的内容
                for interaction in user_interactions:
                    if 'significant_traits' in interaction:
                        for trait in interaction['significant_traits']:
                            if trait not in processed_traits and trait in current_scores:
                                # 获取交互得分
                                interaction_score = self.score_changes.get(interaction['interaction_type'], 0)
                                current_score = current_scores[trait] or 0
                                new_scores[trait] = min(100, max(0, current_score + interaction_score))
                                processed_traits.add(trait)
                                logger.debug(f"User {user_id} - Trait {trait} updated due to {interaction['interaction_type']}: {current_score} -> {new_scores[trait]}")
                
                # 处理推荐但没有交互的性格特征
                for trait in recommended_traits:
                    if trait not in processed_traits and trait in current_scores:
                        current_score = current_scores[trait] or 0
                        new_scores[trait] = max(0, current_score + self.score_changes['no_interaction'])
                        processed_traits.add(trait)
                        logger.debug(f"User {user_id} - Trait {trait} decreased due to no interaction: {current_score} -> {new_scores[trait]}")
                
                # 更新数据库
                try:
                    update_cols = []
                    update_vals = []
                    for col in columns[1:]:  # 跳过id列
                        if col in new_scores:
                            update_cols.append(f"{col} = ?")
                            update_vals.append(new_scores[col])
                    
                    if update_cols:
                        update_query = f"UPDATE personality SET {', '.join(update_cols)} WHERE id = ?"
                        update_vals.append(user_id)
                        logger.debug(f"Executing update query: {update_query}")
                        logger.debug(f"Update values: {update_vals}")
                        cursor.execute(update_query, update_vals)
                        
                        # 验证更新
                        cursor.execute("SELECT * FROM personality WHERE id = ?", (user_id,))
                        updated_row = cursor.fetchone()
                        updated_scores = dict(zip(columns[1:], updated_row[1:]))
                        logger.debug(f"Updated scores in database for user {user_id}: {updated_scores}")
                        
                        # 记录变化
                        changes = {trait: {'original': current_scores.get(trait, 0), 'updated': new_scores[trait]} 
                                for trait in processed_traits 
                                if new_scores[trait] != current_scores.get(trait, 0)}
                        
                        if changes:
                            logger.info(f"Updated scores for user {user_id}: {changes}")
                except Exception as e:
                    logger.error(f"Error updating database for user {user_id}: {str(e)}", exc_info=True)
                    raise
            
            conn.commit()
            logger.debug("Database changes committed")
            return True
            
        except Exception as e:
            logger.error(f"Error updating personality scores: {str(e)}", exc_info=True)
            if conn:
                conn.rollback()
                logger.debug("Database changes rolled back")
            return False
        finally:
            if conn:
                conn.close()
                logger.debug("Database connection closed")

    def _is_trait_related(self, content_type, trait):
        """判断内容类型是否与性格特征相关"""
        # 所有性格特征都与内容和商品相关
        return content_type in ['content', 'product']

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
        """获取所有用户的分数变化，返回原始和更新后的分数"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_behavior'")
            if not cursor.fetchone():
                logger.error("user_behavior table does not exist")
                return {}
            
            # 获取所有用户ID
            cursor.execute("SELECT id FROM personality")
            users = cursor.fetchall()
            logger.debug(f"Found {len(users)} users in personality table")
            
            changes = {}
            for user_id, in users:
                # 从数据库获取用户的所有交互
                cursor.execute("""
                    SELECT user_id, content_id, interaction_type, content_type, significant_traits, timestamp
                    FROM user_behavior
                    WHERE user_id = ?
                """, (user_id,))
                
                rows = cursor.fetchall()
                logger.debug(f"Found {len(rows)} interactions for user {user_id}")
                
                user_interactions = []
                for row in rows:
                    # 处理性格特征名称，移除"旧的"前缀
                    traits = json.loads(row[4]) if row[4] else []
                    processed_traits = [trait.replace('旧的', '') for trait in traits]
                    
                    interaction = {
                        'user_id': row[0],
                        'content_id': row[1],
                        'interaction_type': row[2],
                        'content_type': row[3],
                        'significant_traits': processed_traits,
                        'timestamp': row[5]
                    }
                    user_interactions.append(interaction)
                    logger.debug(f"Interaction for user {user_id}: {interaction}")
                
                if not user_interactions:
                    logger.debug(f"No interactions found for user {user_id}")
                    continue
                
                # 获取用户当前分数
                cursor.execute("SELECT * FROM personality WHERE id = ?", (user_id,))
                columns = [description[0] for description in cursor.description]
                current_scores = dict(zip(columns[1:], cursor.fetchone()[1:]))
                logger.debug(f"Current scores for user {user_id}: {current_scores}")
                
                # 收集所有推荐内容中出现的性格特征
                recommended_traits = set()
                for interaction in user_interactions:
                    traits = interaction.get('significant_traits', [])
                    recommended_traits.update(traits)
                logger.debug(f"Recommended traits for user {user_id}: {recommended_traits}")
                
                # 计算每个交互对性格特征的影响
                trait_changes = {}
                processed_traits = set()
                
                # 处理有交互的特征
                for interaction in user_interactions:
                    traits = interaction.get('significant_traits', [])
                    if not traits:
                        continue
                        
                    # 计算每个特征的变化
                    for trait in traits:
                        if trait not in current_scores:
                            continue
                            
                        original_score = current_scores[trait] or 0
                        interaction_score = self.score_changes.get(interaction['interaction_type'], 0)
                        updated_score = min(100, max(0, original_score + interaction_score))
                        processed_traits.add(trait)
                        
                        if original_score != updated_score:
                            trait_changes[trait] = {
                                'original': original_score,
                                'updated': updated_score
                            }
                            logger.debug(f"Trait {trait} changed for user {user_id}: {original_score} -> {updated_score}")
                
                # 处理推荐但没有交互的特征
                for trait in recommended_traits:
                    if trait not in processed_traits and trait in current_scores:
                        original_score = current_scores[trait] or 0
                        updated_score = max(0, original_score + self.score_changes['no_interaction'])
                        
                        if original_score != updated_score:
                            trait_changes[trait] = {
                                'original': original_score,
                                'updated': updated_score
                            }
                            logger.debug(f"Trait {trait} decreased for user {user_id} due to no interaction: {original_score} -> {updated_score}")
                
                if trait_changes:
                    changes[user_id] = trait_changes
                    logger.debug(f"Changes for user {user_id}: {trait_changes}")
            
            conn.close()
            logger.debug(f"Score changes calculated: {changes}")
            return changes
            
        except Exception as e:
            logger.error(f"Error getting score changes: {str(e)}", exc_info=True)
            return {}

# 创建单例实例
user_feedback = UserFeedback()