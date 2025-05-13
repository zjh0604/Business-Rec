import os
import logging
import socket
import json
from quart import Quart, render_template, request, jsonify
from business_db import init_business_db, save_business_order, get_all_business_orders, get_business_orders_by_user, load_orders_from_json
from business_vector_db import init_business_vector_db

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建Quart应用
app = Quart(__name__)

# 确保输出目录存在
os.makedirs("output", exist_ok=True)

# 初始化数据库
init_business_db()
vector_db = init_business_vector_db()

@app.route('/')
async def index():
    """首页"""
    return await render_template('index.html')

@app.route('/business')
async def business():
    """商单管理页面"""
    # 获取所有商单中的用户ID
    orders = get_all_business_orders()
    user_ids = list(set(order['user_id'] for order in orders))
    return await render_template('business.html', user_ids=user_ids)

@app.route('/api/business/orders', methods=['GET'])
async def get_orders():
    """获取所有商单"""
    try:
        orders = get_all_business_orders()
        return jsonify({"success": True, "orders": orders})
    except Exception as e:
        logger.error(f"Error getting orders: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/business/orders/<user_id>', methods=['GET'])
async def get_user_orders(user_id):
    """获取指定用户的商单并返回推荐"""
    try:
        # 获取用户的商单
        user_orders = get_business_orders_by_user(user_id)
        if not user_orders:
            return jsonify({"success": False, "error": "未找到该用户的商单"})

        # 获取推荐商单
        recommended_orders = []
        for order in user_orders:
            similar_orders = vector_db.find_similar_orders(order)
            # 过滤掉用户自己的商单
            similar_orders = [o for o in similar_orders if o['user_id'] != user_id]
            recommended_orders.extend(similar_orders)

        # 去重并限制数量
        seen = set()
        unique_orders = []
        for order in recommended_orders:
            order_id = f"{order['user_id']}_{order['wish_title']}"
            if order_id not in seen:
                seen.add(order_id)
                unique_orders.append(order)
                if len(unique_orders) >= 5:  # 限制返回5个推荐
                    break

        return jsonify({
            "success": True,
            "user_orders": user_orders,
            "recommended_orders": unique_orders
        })
    except Exception as e:
        logger.error(f"Error getting user orders: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/business/orders', methods=['POST'])
async def create_order():
    """创建新商单"""
    try:
        data = await request.get_json()
        if save_business_order(data):
            # 更新向量数据库
            vector_db.add_orders([data])
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "保存商单失败"})
    except Exception as e:
        logger.error(f"Error creating order: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/business/load-orders', methods=['POST'])
async def load_orders():
    """从JSON文件加载商单数据"""
    try:
        if load_orders_from_json():
            # 重新初始化向量数据库
            global vector_db
            vector_db = init_business_vector_db()
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "加载商单数据失败"})
    except Exception as e:
        logger.error(f"Error loading orders: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/business/user_ids_from_json', methods=['GET'])
async def get_user_ids_from_json():
    try:
        with open('user_orders.json', 'r', encoding='utf-8') as f:
            orders = json.load(f)
        user_ids = list({order['user_id'] for order in orders})
        return jsonify({"success": True, "user_ids": user_ids})
    except Exception as e:
        logger.error(f"Error reading user_orders.json: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

def get_server_info():
    """获取服务器信息"""
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    return {
        "hostname": hostname,
        "ip_address": ip_address
    }

if __name__ == '__main__':
    server_info = get_server_info()
    logger.info(f"Starting server on {server_info['ip_address']}")
    app.run(host='0.0.0.0', port=5000) 