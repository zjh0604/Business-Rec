import sqlite3
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_tables():
    """创建数据库表"""
    try:
        conn = sqlite3.connect('user.db')
        cursor = conn.cursor()

        # 创建用户行为表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_behavior (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                interaction_type TEXT NOT NULL,
                content_type TEXT NOT NULL,
                score_change REAL NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES personality (id)
            )
        ''')

        conn.commit()
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    create_tables()