import sqlite3
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class UserFeedback:
    def __init__(self):
        self.db_path = "user.db"
        self.score_changes = {
            "like": 1.5,
            "collect": 2.0,
            "comment": 2.5,
            "add_to_cart": 2.0,
            "purchase": 3.0
        }

    def update_personality_score(self, user_id, interaction_type, content_type):
        """更新用户性格分数"""
        try:
            # 连接数据库
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 获取当前分数
            cursor.execute("SELECT * FROM personality WHERE id = ?", (user_id,))
            columns = [description[0] for description in cursor.description]
            row = cursor.fetchone()

            if not row:
                logger.error(f"User {user_id} not found in database")
                return False

            # 创建分数字典
            scores = dict(zip(columns[1:], row[1:]))  # 跳过id列

            # 根据交互类型和内容类型更新分数
            score_change = self.score_changes.get(interaction_type, 0)
            
            # 根据内容类型确定要更新的性格特征
            if content_type == "product":
                # 商品交互主要影响外向性和开放性
                scores["外向性"] = min(100, scores.get("外向性", 0) + score_change)
                scores["开放性"] = min(100, scores.get("开放性", 0) + score_change * 0.5)
            else:
                # 内容交互主要影响宜人性和尽责性
                scores["宜人性"] = min(100, scores.get("宜人性", 0) + score_change)
                scores["尽责性"] = min(100, scores.get("尽责性", 0) + score_change * 0.5)

            # 构建更新语句
            update_columns = ", ".join([f"{col} = ?" for col in scores.keys()])
            update_values = list(scores.values())
            update_values.append(user_id)  # 添加 WHERE 条件的值

            # 执行更新
            cursor.execute(f"""
                UPDATE personality 
                SET {update_columns}
                WHERE id = ?
            """, update_values)

            # 记录用户行为
            cursor.execute("""
                INSERT INTO user_behavior 
                (user_id, interaction_type, content_type, score_change, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, interaction_type, content_type, score_change, datetime.now().isoformat()))

            conn.commit()
            logger.info(f"Updated personality scores for user {user_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating personality score: {str(e)}")
            return False
        finally:
            conn.close()

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

# 创建单例实例
user_feedback = UserFeedback()