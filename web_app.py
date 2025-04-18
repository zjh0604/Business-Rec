import os
from app import process_user_analysis, query_personality_score, query_columns, get_user_operations
from db_operation import create_chroma_db, answer_user_query
import socket
import logging
import sqlite3
import json
from user_feedback import user_feedback
from behavior_analyzer import BehaviorAnalyzer
from personality_score import PersonalityScoreCalculator
from visualization import create_heatmap, create_comparison_heatmap
from content_manager import ContentManager
import asyncio
from quart import Quart, render_template, jsonify, send_from_directory, request

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# 使用 Quart 替代 Flask 以支持异步操作
app = Quart(__name__)

# 创建内容管理器实例
content_manager = ContentManager()

# 确保输出目录存在
if not os.path.exists('output'):
    os.makedirs('output')

# 检查是否存在rag向量数据库
create_chroma_db("rag_shqp", "shqp_rag.txt")

def init_database():
    """初始化数据库表"""
    try:
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        
        # 创建商品分类表
        c.execute('''
            CREATE TABLE IF NOT EXISTS product_category (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pid INTEGER DEFAULT 0,
                name TEXT,
                icon TEXT,
                level INTEGER DEFAULT 1,
                sort INTEGER DEFAULT 999,
                is_show INTEGER DEFAULT 1,
                is_del INTEGER DEFAULT 0,
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sys_source TEXT DEFAULT 'sohuglobal'
            )
        ''')
        
        # 检查是否有数据
        c.execute("SELECT COUNT(*) FROM product_category")
        if c.fetchone()[0] == 0:
            # 插入一些初始分类
            categories = [
                (0, '饮品', 'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/09/20/e948541315a74d1f96e0f57ee79e867b.png', 1, 1, 1, 0),
                (0, '食品', 'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/09/20/food.png', 1, 2, 1, 0),
                (0, '电子产品', 'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/09/20/electronics.png', 1, 3, 1, 0),
                (0, '服装', 'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/09/20/clothing.png', 1, 4, 1, 0),
                (0, '家居', 'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/09/20/home.png', 1, 5, 1, 0)
            ]
            c.executemany("""
                INSERT INTO product_category (pid, name, icon, level, sort, is_show, is_del)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, categories)
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")

# 在应用启动时初始化数据库
init_database()

@app.route('/')
async def index():
    logger.debug("Accessing index page")
    return await render_template('index.html')

@app.route('/users')
async def user_management():
    logger.debug("Accessing user management page")
    try:
        # 连接数据库
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        # 查询所有用户ID
        cursor = c.execute("SELECT DISTINCT id FROM personality")
        user_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        return await render_template('users.html', user_ids=user_ids)
    except Exception as e:
        logger.error(f"Error accessing user management: {str(e)}")
        return await render_template('users.html', user_ids=[], error=str(e))

@app.route('/analyze', methods=['POST'])
async def analyze():
    try:
        # 获取JSON数据
        data = await request.get_json()
        user_id = data.get('user_id')
        
        if not user_id:
            logger.error("No user_id provided in request")
            return jsonify({
                'success': False,
                'error': '请提供用户ID'
            })
            
        logger.debug(f"Processing analysis for user_id: {user_id}")
        
        # 处理用户分析
        results = await process_user_analysis(user_id)
        
        # 确保推荐内容格式正确
        if 'recommendations' in results:
            if isinstance(results['recommendations'], str):
                # 如果推荐内容是字符串，转换为数组格式
                recommendations = []
                lines = results['recommendations'].split('\n')
                current_item = {}
                for line in lines:
                    if line.strip():
                        if line.startswith('   描述：'):
                            current_item['description'] = line.replace('   描述：', '').strip()
                        elif line.startswith('   推荐理由：'):
                            current_item['reason'] = line.replace('   推荐理由：', '').strip()
                        elif line.startswith('   推荐分数：'):
                            current_item['score'] = float(line.replace('   推荐分数：', '').strip())
                        elif line.startswith('   图片链接：'):
                            current_item['image_url'] = line.replace('   图片链接：', '').strip()
                        elif not line.startswith('   '):
                            if current_item:
                                recommendations.append(current_item)
                            current_item = {'name': line.strip()}
                if current_item:
                    recommendations.append(current_item)
                results['recommendations'] = recommendations
        
        return jsonify({
            'success': True,
            **results
        })
        
    except Exception as e:
        logger.error(f"Error processing analysis: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/output/<path:filename>')
async def serve_image(filename):
    logger.debug(f"Serving image: {filename}")
    return await send_from_directory('output', filename)

@app.route('/get_personality/<user_id>')
async def get_personality(user_id):
    logger.debug(f"Getting personality data for user_id: {user_id}")
    try:
        # 连接数据库
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        
        # 查询用户性格数据
        cursor = c.execute("SELECT * FROM personality WHERE id = ?", (user_id,))
        columns = [description[0] for description in cursor.description]
        row = cursor.fetchone()
        
        if row:
            # 创建特征和分数的字典，过滤掉None值
            personality_data = {}
            for col, val in zip(columns[1:], row[1:]):  # 跳过id列
                if val is not None:  # 只添加非None值
                    personality_data[col] = val
            
            if personality_data:  # 确保有数据
                # 按分数排序并获取前10个特征
                sorted_traits = sorted(personality_data.items(), key=lambda x: x[1], reverse=True)
                top_traits = sorted_traits[:10]
                
                # 分离特征名和分数
                traits = [trait for trait, _ in top_traits]
                scores = [score for _, score in top_traits]
                
                return jsonify({
                    'success': True,
                    'traits': traits,
                    'scores': scores
                })
            else:
                return jsonify({
                    'success': False,
                    'error': '用户性格数据为空'
                })
        else:
            return jsonify({
                'success': False,
                'error': '用户不存在'
            })
            
    except Exception as e:
        logger.error(f"Error getting personality data: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })
    finally:
        conn.close()

@app.route('/get_recent_changes/<user_id>')
async def get_recent_changes(user_id):
    logger.debug(f"Getting recent changes for user_id: {user_id}")
    try:
        # 首先尝试从数据库获取数据
        conn = sqlite3.connect("user.db")
        c = conn.cursor()
        
        try:
            # 查询用户操作日志
            cursor = c.execute("""
                SELECT operation_type, operation_time, details 
                FROM user_behavior 
                WHERE user_id = ? 
                ORDER BY operation_time DESC 
                LIMIT 10
            """, (user_id,))
            operations = cursor.fetchall()
            
            # 查询用户性格变化
            cursor = c.execute("""
                SELECT trait_name, old_value, new_value, change_time 
                FROM personality_changes 
                WHERE user_id = ? 
                ORDER BY change_time DESC 
                LIMIT 10
            """, (user_id,))
            changes = cursor.fetchall()
            
            if operations or changes:
                # 格式化操作日志
                formatted_operations = []
                for op in operations:
                    formatted_operations.append({
                        'type': op[0],
                        'time': op[1],
                        'details': op[2]
                    })
                
                # 格式化性格变化
                formatted_changes = []
                for change in changes:
                    formatted_changes.append({
                        'trait': change[0],
                        'old_value': change[1],
                        'new_value': change[2],
                        'time': change[3]
                    })
                
                return jsonify({
                    'success': True,
                    'has_data': True,
                    'operations': formatted_operations,
                    'changes': formatted_changes
                })
                
        except sqlite3.OperationalError:
            # 如果表不存在，从JSON文件获取数据
            logger.info(f"Database tables not found, using JSON file for user {user_id}")
            
            # 读取JSON文件
            with open('user_operations.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 过滤出指定用户的操作
            user_operations = [
                op for op in data['operations']
                if op['user_id'] == str(user_id)
            ]
            
            if user_operations:
                # 格式化操作日志
                formatted_operations = []
                for op in user_operations:
                    formatted_operations.append({
                        'type': op['action'],
                        'time': op['time'],
                        'details': op['detail']
                    })
                
                return jsonify({
                    'success': True,
                    'has_data': True,
                    'operations': formatted_operations,
                    'changes': []  # JSON文件中没有性格变化数据
                })
            
        # 如果没有找到任何数据
        return jsonify({
            'success': True,
            'has_data': False,
            'message': '暂无用户操作记录和性格变化数据'
        })
            
    except Exception as e:
        logger.error(f"Error getting recent changes: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })
    finally:
        conn.close()

@app.route('/feedback', methods=['POST'])
async def handle_feedback():
    """处理用户交互反馈"""
    try:
        data = await request.get_json()
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
            content_type=content_type,
            significant_traits=data.get('significant_traits', [])
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

@app.route('/update_user_data', methods=['POST'])
async def update_user_data():
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

def get_server_info():
    try:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        return hostname, local_ip
    except Exception as e:
        logger.error(f"Error getting server info: {str(e)}")
        return "unknown", "unknown"

async def process_user_analysis(user_id):
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
        user_operations = get_user_operations(user_id)
        
        # 限制操作日志的长度
        max_operations = 50  # 减少处理的操作记录数量
        if len(user_operations['ops']) > max_operations:
            user_operations['ops'] = user_operations['ops'][:max_operations]
            logger.warning(f"操作日志超过{max_operations}条，只处理前{max_operations}条")
        
        # 使用行为分析器分析用户行为
        behavior_analyzer = BehaviorAnalyzer()
        behavior_summary = await behavior_analyzer.analyze_user_behavior(user_id, user_operations['ops'])
        
        # 使用性格分数计算器计算新分数
        calculator = PersonalityScoreCalculator()
        new_scores = calculator.update_personality_scores(behavior_summary, initial_scores)
        logger.debug(f"Updated scores: {new_scores}")
        
        # 生成更新原因
        update_reasons = {}
        for trait in new_scores:
            # 计算分数变化，确保处理 None 值
            old_score = float(initial_scores.get(trait, 0) or 0)
            new_score = float(new_scores.get(trait, 0) or 0)
            score_change = new_score - old_score
            
            # 获取变化原因
            reasons = calculator.get_score_change_reasons(behavior_summary, trait)
            if reasons:
                update_reasons[trait] = {
                    'old_score': old_score,
                    'new_score': new_score,
                    'change': score_change,
                    'reasons': reasons
                }
        logger.debug(f"Update reasons: {update_reasons}")

        # 构建用户画像字符串
        user_profile = f"用户的性格特征画像：id = {user_id}"
        for trait, scores in update_reasons.items():
            user_profile += f"\n{trait}: {scores['old_score']:.1f} -> {scores['new_score']:.1f} (变化: {scores['change']:.1f})"
        user_profile += "."
        logger.debug(f"User profile string: {user_profile}")

        # 生成新的画像
        prompt_profile = """
        请分析以下用户信息并生成更新后的用户画像：

        [用户基本信息]
        {user_profile}

        [用户行为分析]
        {trait_analysis}

        [性格特征更新]
        {score_changes}

        请按以下步骤分析：

        1. 行为分析总结：
        - 总结用户的主要行为模式
        - 分析行为反映的性格特征
        - 解释行为与性格特征的关联

        2. 分数变化分析：
        - 解释每个性格特征分数变化的原因
        - 说明变化的具体依据
        - 分析变化的合理性

        3. 用户画像更新：
        - 生成更新后的用户画像描述
        - 突出重要的性格特征变化
        - 预测用户未来的行为趋势

        请按以下格式输出分析结果：

        === 用户画像分析 ===
        用户ID：{user_id}

        [行为分析]
        {trait_analysis}

        [性格特征更新]
        {score_changes}

        [更新后的用户画像]
        请根据以上分析生成更新后的用户画像。
        """

        # 格式化prompt
        logger.debug("Formatting prompt with trait analysis")
        try:
            # 确保 trait_analysis 存在且格式正确
            trait_analysis = behavior_summary.get('trait_analysis', {})
            logger.debug(f"Retrieved trait_analysis: {trait_analysis}")
            
            # 确保 trait_analysis 可以被序列化为 JSON
            try:
                json.dumps(trait_analysis, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Error serializing trait_analysis: {str(e)}")
                trait_analysis = {'error': '无法序列化特征分析'}
            
            formatted_prompt = prompt_profile.format(
                user_id=user_id,
                user_profile=user_profile,
                trait_analysis=json.dumps(trait_analysis, ensure_ascii=False),
                score_changes=json.dumps(update_reasons, ensure_ascii=False)
            )
            logger.debug(f"Formatted prompt: {formatted_prompt}")
        except Exception as e:
            logger.error(f"Error formatting prompt: {str(e)}", exc_info=True)
            # 使用默认值格式化 prompt
            formatted_prompt = prompt_profile.format(
                user_id=user_id,
                user_profile=user_profile,
                trait_analysis=json.dumps({'error': '无法格式化特征分析'}, ensure_ascii=False),
                score_changes=json.dumps(update_reasons, ensure_ascii=False)
            )

        # 使用RAG进行分析
        result_profile = ''.join(
            answer_user_query("rag_shqp", formatted_prompt))

        # 生成热力图
        create_heatmap(initial_scores, f"Initial Personality Heatmap for User {user_id}",
                    f"initial_heatmap_{user_id}.png")
        create_heatmap(new_scores, f"Updated Personality Heatmap for User {user_id}",
                    f"updated_heatmap_{user_id}.png")
        create_comparison_heatmap(initial_scores, new_scores,
                                f"Personality Scores Comparison for User {user_id}",
                                f"comparison_heatmap_{user_id}.png")

        # 获取推荐内容
        recommended_items = content_manager.get_recommendations(new_scores)

        return {
            'success': True,
            'summary': behavior_summary,
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

if __name__ == '__main__':
    try:
        # 获取本机IP地址
        hostname, local_ip = get_server_info()
        
        print("\n=== Server Information ===")
        print(f"Hostname: {hostname}")
        print(f"Local IP address: {local_ip}")
        print("\n=== Available URLs ===")
        print(f"  - http://localhost:8080")
        print(f"  - http://127.0.0.1:8080")
        print(f"  - http://{local_ip}:8080")
        print("\nStarting server...")
        
        # 使用0.0.0.0作为host，允许所有网络接口的连接
        app.run(debug=True, host='0.0.0.0', port=8080)
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        raise 