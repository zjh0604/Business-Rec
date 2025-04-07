from sohu_api import SohuGlobalAPI
import json
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_api():
    api = SohuGlobalAPI("https://api.sohuglobal.com")
    
    # 测试登录
    if api.login(phone="admin", password="U9xbHDJUH1pmx9hk7nXbQQ=="):
        print("登录成功")
        
        # 测试获取商品列表
        products = api.get_products(page_size=1, page_num=1, type=1)
        print("\n商品列表响应结构:")
        print(json.dumps(products, ensure_ascii=False, indent=2))
        
        # 测试获取内容列表
        contents = api.get_content_list(page_size=1, page_num=1, state="OnShelf", busy_type="Article")
        print("\n内容列表响应结构:")
        print(json.dumps(contents, ensure_ascii=False, indent=2))
        
        # 测试获取内容详情
        if contents and contents.get('data', {}).get('list'):
            content_id = contents['data']['list'][0].get('id')
            if content_id:
                details = api.get_content_details(content_id)
                print("\n内容详情响应结构:")
                print(json.dumps(details, ensure_ascii=False, indent=2))
    else:
        print("登录失败")

if __name__ == "__main__":
    test_api()