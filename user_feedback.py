import json
import sqlite3
import logging
from datetime import datetime, timedelta
import numpy as np

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

    def _calculate_time_weight(self, timestamp):
        """计算时间衰减权重，借鉴personality_score.py"""
        try:
            op_time = datetime.fromisoformat(timestamp)
            current_time = datetime.now()
            time_diff = (current_time - op_time).days
            time_decay_factor = 0.1
            base_time_window = 30
            weight = np.exp(-time_decay_factor * time_diff / base_time_window)
            return float(weight)
        except Exception as e:
            logger.error(f"计算时间权重时出错: {str(e)}")
            return 0.0

    def update_personality_scores(self):
        """根据用户交互行为更新性格分数（引入时间衰减和行为频率非线性增强）"""
        conn = None
        try:
            logger.debug("Starting personality score update...")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM personality")
            users = cursor.fetchall()
            logger.debug(f"Found {len(users)} users in database")
            for user_id, in users:
                logger.debug(f"Processing user {user_id}")
                cursor.execute("SELECT * FROM personality WHERE id = ?", (user_id,))
                columns = [description[0] for description in cursor.description]
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"No data found for user {user_id}")
                    continue
                current_scores = dict(zip(columns[1:], row[1:]))
                logger.debug(f"Current scores for user {user_id}: {current_scores}")
                cursor.execute("""
                    SELECT user_id, content_id, interaction_type, content_type, significant_traits, timestamp
                    FROM user_behavior
                    WHERE user_id = ? AND datetime(timestamp) >= datetime('now', '-30 days')
                """, (user_id,))
                rows = cursor.fetchall()
                logger.debug(f"Found {len(rows)} recent interactions for user {user_id}")
                trait_interactions = {}
                recommended_traits = set()
                for row in rows:
                    traits = json.loads(row[4]) if row[4] else []
                    interaction_type = row[2]
                    timestamp = row[5]
                    for trait in traits:
                        recommended_traits.add(trait)
                        if trait not in trait_interactions:
                            trait_interactions[trait] = {}
                        if interaction_type not in trait_interactions[trait]:
                            trait_interactions[trait][interaction_type] = []
                        trait_interactions[trait][interaction_type].append(timestamp)
                new_scores = current_scores.copy()
                for trait in recommended_traits:
                    if trait not in current_scores:
                        continue
                    trait_score = current_scores[trait] or 0
                    # 对每种行为类型分别处理
                    for interaction_type, timestamps in trait_interactions.get(trait, {}).items():
                        # 计算每次行为的加权分数
                        weighted_scores = []
                        for ts in timestamps:
                            base = self.score_changes.get(interaction_type, 0)
                            time_weight = self._calculate_time_weight(ts)
                            weighted_scores.append(base * time_weight)
                        # 非线性增强：同一行为多次发生，影响增强（如log加成）
                        if weighted_scores:
                            freq = len(weighted_scores)
                            total = sum(weighted_scores)
                            nonlinear_total = total * np.log1p(freq)  # log1p(x) = log(1+x)
                            trait_score += nonlinear_total
                    # 惩罚：如果最近7天没有任何行为，应用no_interaction
                    recent = False
                    for timestamps in trait_interactions.get(trait, {}).values():
                        if any(datetime.fromisoformat(ts) >= datetime.now() - timedelta(days=7) for ts in timestamps):
                            recent = True
                            break
                    if not recent:
                        trait_score += self.score_changes['no_interaction']
                        logger.debug(f"Applying no_interaction penalty to {trait} for user {user_id}")
                    # 保证分数在0-100区间
                    new_scores[trait] = min(100, max(0, trait_score))
                    if new_scores[trait] != current_scores[trait]:
                        logger.debug(f"User {user_id} - Trait {trait} updated: {current_scores[trait]} -> {new_scores[trait]}")
                # 更新数据库
                try:
                    update_cols = []
                    update_vals = []
                    for col in columns[1:]:
                        if col in new_scores:
                            update_cols.append(f"{col} = ?")
                            update_vals.append(new_scores[col])
                    if update_cols:
                        update_query = f"UPDATE personality SET {', '.join(update_cols)} WHERE id = ?"
                        update_vals.append(user_id)
                        cursor.execute(update_query, update_vals)
                        changes = {trait: {'original': current_scores.get(trait, 0), 'updated': new_scores[trait]} 
                                for trait in recommended_traits 
                                if new_scores[trait] != current_scores.get(trait, 0)}
                        if changes:
                            logger.info(f"Updated scores for user {user_id}: {changes}")
                except Exception as e:
                    logger.error(f"Error updating database for user {user_id}: {str(e)}")
                    raise
            conn.commit()
            try:
                logger.debug("Clearing processed behavior records...")
                cursor.execute("DELETE FROM user_behavior WHERE datetime(timestamp) < datetime('now', '-30 days')")
                conn.commit()
                logger.info("Successfully cleared old behavior records")
            except Exception as e:
                logger.error(f"Error clearing old behavior records: {str(e)}")
                raise
            return True
        except Exception as e:
            logger.error(f"Error updating personality scores: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

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