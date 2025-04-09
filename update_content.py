from content_manager import content_manager
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """更新内容数据"""
    try:
        # 获取并存储商品数据
        logger.info("开始获取并存储商品数据...")
        if content_manager.fetch_and_store_products():
            logger.info("商品数据更新成功")
        else:
            logger.error("商品数据更新失败")

        # 获取并存储内容数据
        logger.info("开始获取并存储内容数据...")
        if content_manager.fetch_and_store_contents():
            logger.info("内容数据更新成功")
        else:
            logger.error("内容数据更新失败")

        # 检查数据库中的内容数量
        product_count = content_manager.product_collection.count()
        content_count = content_manager.content_collection.count()
        logger.info(f"数据库内容统计 - 商品数量: {product_count}, 内容数量: {content_count}")

    except Exception as e:
        logger.error(f"更新内容时发生错误: {str(e)}")

if __name__ == '__main__':
    main()