import sqlite3
import json
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_business_db():
    """初始化商单相关的数据库表"""
    try:
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        
        # 创建商单表
        c.execute('''
            CREATE TABLE IF NOT EXISTS business_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                corresponding_role TEXT NOT NULL,
                classification TEXT NOT NULL,
                wish_title TEXT NOT NULL,
                wish_details TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Business database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing business database: {str(e)}")

def save_business_order(order_data):
    """保存商单信息到数据库"""
    try:
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        
        c.execute('''
            INSERT INTO business_orders 
            (user_id, corresponding_role, classification, wish_title, wish_details)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            order_data['user_id'],
            order_data['Corresponding role'],
            order_data['Classification of wishes'],
            order_data['Wish title'],
            order_data['Details of the wish']
        ))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error saving business order: {str(e)}")
        return False

def get_all_business_orders():
    """获取所有商单信息"""
    try:
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        
        c.execute('SELECT * FROM business_orders ORDER BY created_at DESC')
        orders = c.fetchall()
        
        # 将查询结果转换为字典列表
        columns = [description[0] for description in c.description]
        orders_list = []
        for order in orders:
            order_dict = dict(zip(columns, order))
            orders_list.append(order_dict)
        
        conn.close()
        return orders_list
    except Exception as e:
        logger.error(f"Error getting business orders: {str(e)}")
        return []

def get_business_orders_by_user(user_id):
    """获取指定用户的商单信息"""
    try:
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        
        c.execute('SELECT * FROM business_orders WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        orders = c.fetchall()
        
        # 将查询结果转换为字典列表
        columns = [description[0] for description in c.description]
        orders_list = []
        for order in orders:
            order_dict = dict(zip(columns, order))
            orders_list.append(order_dict)
        
        conn.close()
        return orders_list
    except Exception as e:
        logger.error(f"Error getting business orders for user {user_id}: {str(e)}")
        return []

def load_orders_from_json(json_file="user_orders.json"):
    """从JSON文件加载商单数据到数据库"""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            orders = json.load(f)
            
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        
        for order in orders:
            c.execute('''
                INSERT INTO business_orders 
                (user_id, corresponding_role, classification, wish_title, wish_details)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                order['user_id'],
                order['Corresponding role'],
                order['Classification of wishes'],
                order['Wish title'],
                order['Details of the wish']
            ))
        
        conn.commit()
        conn.close()
        logger.info(f"Successfully loaded orders from {json_file}")
        return True
    except Exception as e:
        logger.error(f"Error loading orders from JSON: {str(e)}")
        return False 