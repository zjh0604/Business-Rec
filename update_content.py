from content_manager import content_manager
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    """更新内容数据"""
    try:
        # 获取并存储商品数据
        logger.info("Fetching and storing products...")
        if content_manager.fetch_and_store_products():
            logger.info("Successfully updated product data")
        else:
            logger.error("Failed to update product data")

        # 获取并存储内容数据
        logger.info("Fetching and storing contents...")
        if content_manager.fetch_and_store_contents():
            logger.info("Successfully updated content data")
        else:
            logger.error("Failed to update content data")

    except Exception as e:
        logger.error(f"Error updating content: {str(e)}")

if __name__ == '__main__':
    main()