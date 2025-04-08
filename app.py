import sqlite3
import re
import json
import os
from datetime import datetime
from db_operation import create_chroma_db, answer_without_chroma_db, answer_user_query
from visualization import create_heatmap, create_comparison_heatmap
from sohu_api import SohuGlobalAPI
from my_qianfan_llm import llm
from flask import Blueprint, request, jsonify, render_template, current_app
import logging
from content_manager import content_manager
from user_feedback import user_feedback
# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# 创建蓝图
api = Blueprint('api', __name__)

# [1] rag准备

# 检查是否存在rag向量数据库
create_chroma_db("rag_shqp", "shqp_rag .txt")

# 初始化搜狐API
sohu_api = SohuGlobalAPI("https://api.sohuglobal.com")
try:
    print("Attempting to login to Sohu API...")
    if not sohu_api.login(phone="admin", password="U9xbHDJUH1pmx9hk7nXbQQ=="):
        print("Failed to login to Sohu API")
        raise Exception("Failed to login to Sohu API")
except Exception as e:
    print(f"Error during Sohu API initialization: {str(e)}")
    raise e

def extract_scores_from_text(text):
    """Extract scores from the text using regex"""
    scores = {}
    # Match patterns like "xxx = xx" or "xxx=xx"
    pattern = r'(\w+)\s*=\s*(\d+)'
    matches = re.finditer(pattern, text)
    for match in matches:
        key = match.group(1)
        value = int(match.group(2))
        scores[key] = value
    return scores

def query_personality_score(user_id):
    """Query user personality scores from database"""
    try:
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        cursor = c.execute("SELECT * from personality where id = ?", (user_id,))
        columns = [description[0] for description in cursor.description]
        row = cursor.fetchone()
        conn.close()

        if row is None:
            print(f"Warning: No personality data found for user ID {user_id}")
            return None

        return row
    except Exception as e:
        print(f"Error querying personality score: {str(e)}")
        return None

def query_columns():
    """Query column names from database"""
    try:
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        cursor = c.execute("SELECT * from personality")
        columns = [description[0] for description in cursor.description]
        conn.close()
        print(f"Database columns: {columns}")  # 添加日志
        return columns
    except Exception as e:
        print(f"Error querying columns: {str(e)}")
        return []

def query_user_data(user_id):
    """查询用户的行为数据

    Args:
        user_id: 用户ID

    Returns:
        list: 用户行为数据列表
    """
    try:
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM user_behavior WHERE user_id = ?", (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"Error querying user data: {str(e)}")
        return []

def analyze_user_behavior(user_data, initial_scores):
    """分析用户行为并更新性格分数

    Args:
        user_data: 用户行为数据
        initial_scores: 初始性格分数

    Returns:
        list: 更新后的性格分数
    """
    try:
        # 这里可以添加更复杂的行为分析逻辑
        # 目前简单返回初始分数
        return initial_scores
    except Exception as e:
        print(f"Error analyzing user behavior: {str(e)}")
        return initial_scores

# 模拟商品数据
MOCK_PRODUCTS = [
    {
        "id": 1,
        "name": "智能手表",
        "description": "多功能智能手表，支持心率监测、运动追踪等功能",
        "category_id": 3,
        "category_name": "电子产品",
        "price": 999.00,
        "image_url": "https://via.placeholder.com/300x200?text=智能手表"
    },
    {
        "id": 2,
        "name": "运动水壶",
        "description": "大容量运动水壶，保温保冷，适合户外运动",
        "category_id": 1,
        "category_name": "饮品",
        "price": 89.00,
        "image_url": "https://via.placeholder.com/300x200?text=运动水壶"
    },
    {
        "id": 3,
        "name": "有机坚果礼盒",
        "description": "精选优质坚果，营养丰富，适合作为健康零食",
        "category_id": 2,
        "category_name": "食品",
        "price": 199.00,
        "image_url": "https://via.placeholder.com/300x200?text=有机坚果"
    },
    {
        "id": 4,
        "name": "休闲运动套装",
        "description": "舒适透气的运动套装，适合日常穿着和运动",
        "category_id": 4,
        "category_name": "服装",
        "price": 299.00,
        "image_url": "https://via.placeholder.com/300x200?text=运动套装"
    },
    {
        "id": 5,
        "name": "智能台灯",
        "description": "可调节亮度和色温的智能台灯，护眼设计",
        "category_id": 5,
        "category_name": "家居",
        "price": 199.00,
        "image_url": "https://via.placeholder.com/300x200?text=智能台灯"
    }
]

def generate_recommendations(user_id, personality_data):
    """生成个性化推荐"""
    try:
        # 使用 ContentManager 获取推荐
        recommendations = content_manager.get_recommendations(personality_data, limit=5)
        
        if not recommendations:
            logger.warning("No recommendations found")
            return []
            
        return recommendations
            
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        return []

def get_user_operations(user_id):
    """从JSON文件中获取指定用户的操作日志

    Args:
        user_id: 用户ID

    Returns:
        dict: 用户操作日志
    """
    try:
        # 读取JSON文件
        with open('user_operations.json', 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 过滤出指定用户的操作
        user_operations = [
            op for op in data['operations']
            if op['user_id'] == str(user_id)
        ]

        # 如果没有找到用户操作，返回空列表
        if not user_operations:
            return {"operations": []}

        # 返回用户操作日志
        return {"operations": user_operations}

    except Exception as e:
        print(f"Error reading user operations: {str(e)}")
        # 如果出错，返回空操作日志
        return {"operations": []}

def process_user_analysis(user_id):
    """Process user analysis and return results"""
    try:
        profile = query_personality_score(user_id)

        if profile is None:
            return {
                'success': False,
                'error': f'未找到用户ID {user_id} 的性格数据，请确保该用户已存在于数据库中。'
            }

        # Create initial personality scores dictionary
        initial_scores = {}
        columns = query_columns()
        print(f"Processing profile: {profile}")  # 添加日志
        print(f"Using columns: {columns}")  # 添加日志

        for col, val in zip(columns, profile):
            if col != 'id':  # Skip the id column
                initial_scores[col] = val

        # 获取用户特定的操作日志
        user_operation_diary = json.dumps(get_user_operations(user_id), ensure_ascii=False)

        # 构建用户画像字符串
        user_profile = f"用户的三魂六魄画像：id = {user_id}"
        for col, val in zip(columns, profile):
            user_profile += f", {col} = {val}"
        user_profile += "."

        # 生成操作总结
        prompt_diary = "以上是该用户的操作日志，请按以下格式进行总结：'用户xx天内操作了xx次xx'...注意不要生成其他的信息，不需要解释你的回答。"
        result_summary = ''.join(answer_without_chroma_db(user_operation_diary + prompt_diary))

        # 生成新的画像
        prompt_profile = """
        请根据用户的操作日志，和旧的用户画像，覆盖用户画像分数。你必须严格按照参考内容的打分标准进行打分。
        输出格式为：用户的三魂七魄画像：\n
        id = xxx\n
        旧的xxx = xxx, 旧的xxx = xxx,
        新的xxx = xxx, 新的xxx = xxx,
        xxx变化了xxx分, xxx变化了xxx分.
        """

        result_profile = ''.join(
            answer_user_query("rag_shqp",
                            "用户id：" + user_id + "\n用户画像：" +
                            user_profile + result_summary + prompt_profile))

        # 提取新分数
        new_scores = extract_scores_from_text(result_profile)

        # 确保新分数包含所有原始特征
        for trait in initial_scores.keys():
            if trait not in new_scores:
                new_scores[trait] = initial_scores[trait]

        # 生成热力图
        create_heatmap(initial_scores, f"Initial Personality Heatmap for User {user_id}",
                    f"initial_heatmap_{user_id}.png")
        create_heatmap(new_scores, f"Updated Personality Heatmap for User {user_id}",
                    f"updated_heatmap_{user_id}.png")
        create_comparison_heatmap(initial_scores, new_scores,
                                f"Personality Scores Comparison for User {user_id}",
                                f"comparison_heatmap_{user_id}.png")

        # 获取推荐内容
        recommended_items = generate_recommendations(user_id, new_scores)

        return {
            'success': True,
            'summary': result_summary,
            'profile': result_profile,
            'recommendations': recommended_items,
            'images': {
                'initial': f"initial_heatmap_{user_id}.png",
                'updated': f"updated_heatmap_{user_id}.png",
                'comparison': f"comparison_heatmap_{user_id}.png"
            }
        }
    except Exception as e:
        logger.error(f"Error in process_user_analysis: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

@api.route('/')
def index():
    """显示主页"""
    logger.debug("Accessing index page")
    return render_template('index.html')

@api.route('/users')
def users():
    """显示用户管理页面"""
    logger.debug("Accessing user management page")
    return render_template('users.html')

@api.route('/analyze', methods=['POST'])
def analyze():
    """处理用户分析请求"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        logger.info(f"Received analysis request for user_id: {user_id}")

        if not user_id:
            logger.warning("No user_id provided in request")
            return jsonify({
                'success': False,
                'error': 'No user_id provided'
            })

        result = process_user_analysis(user_id)
        logger.info(f"Analysis completed for user_id: {user_id}")
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error processing analysis: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@api.route('/feedback', methods=['POST'])
def handle_feedback():
    """处理用户交互反馈"""
    try:
        data = request.get_json()
        if not data:
            logger.error("No JSON data received")
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400
            
        logger.debug(f"Received feedback data: {data}")
        
        user_id = data.get('user_id')
        content_id = data.get('item_id')
        interaction_type = data.get('interaction_type')
        content_type = data.get('content_type')
        
        logger.debug(f"Extracted data - user_id: {user_id}, content_id: {content_id}, "
                    f"interaction_type: {interaction_type}, content_type: {content_type}")
        
        if not all([user_id, content_id, interaction_type, content_type]):
            missing_fields = [field for field, value in {
                'user_id': user_id,
                'item_id': content_id,
                'interaction_type': interaction_type,
                'content_type': content_type
            }.items() if not value]
            
            error_msg = f"Missing required parameters: {', '.join(missing_fields)}"
            logger.warning(error_msg)
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        success = user_feedback.record_interaction(
            user_id=user_id,
            content_id=content_id,
            interaction_type=interaction_type,
            content_type=content_type
        )
        
        if success:
            response_data = {
                'success': True,
                'message': 'Interaction recorded successfully'
            }
            logger.info(f"Successfully recorded interaction for user {user_id}")
            return jsonify(response_data)
        else:
            error_msg = f"Failed to record interaction for user {user_id}"
            logger.error(error_msg)
            return jsonify({
                'success': False,
                'error': error_msg
            }), 500
            
    except Exception as e:
        error_msg = f"Error handling feedback: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({
            'success': False,
            'error': error_msg
        }), 500

@api.route('/data_update')
def data_update():
    """显示数据更新页面"""
    return render_template('data_update.html')

@api.route('/update_user_data', methods=['POST'])
def update_user_data():
    """更新用户数据"""
    try:
        logger.debug("Starting user data update")
        
        # 更新性格分数
        if user_feedback.update_personality_scores():
            # 获取分数变化
            changes = user_feedback.get_score_changes()
            logger.info(f"User data updated successfully. Changes: {changes}")
            
            return jsonify({
                'success': True,
                'changes': changes,
                'message': 'User data updated successfully'
            })
        else:
            error_msg = "Failed to update user data"
            logger.error(error_msg)
            return jsonify({
                'success': False,
                'error': error_msg
            }), 500
            
    except Exception as e:
        logger.error(f"Error updating user data: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api.route('/user_behavior/<user_id>', methods=['GET'])
def get_user_behavior(user_id):
    """获取用户行为历史"""
    try:
        history = user_feedback.get_user_behavior_history(user_id)
        return jsonify({
            'success': True,
            'history': history
        })
    except Exception as e:
        logger.error(f"Error getting user behavior: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })
