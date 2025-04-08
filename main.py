# 这是一个示例 Python 脚本。

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。

from flask import Flask, render_template, jsonify, request
from app import api
import logging
import os

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__, 
                static_folder='static',
                static_url_path='/static',
                template_folder='templates')
    
    # 确保静态文件目录存在
    os.makedirs('static/output', exist_ok=True)
    
    # 注册蓝图
    app.register_blueprint(api, url_prefix='')
    
    # 添加错误处理
    @app.errorhandler(404)
    def not_found_error(error):
        logger.error(f"404 error: {error}, Path: {request.path}")
        if request.path.startswith('/static/'):
            return jsonify({
                'success': False,
                'error': 'Static file not found'
            }), 404
        return jsonify({
            'success': False,
            'error': f'Resource not found: {request.path}'
        }), 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"500 error: {error}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500
        
    @app.before_request
    def log_request_info():
        logger.debug('Headers: %s', request.headers)
        logger.debug('Body: %s', request.get_data())
    
    return app

# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    app = create_app()
    logger.info("Starting application...")
    app.run(debug=True, host='0.0.0.0', port=5000)

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
