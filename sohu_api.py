import requests
import json
import logging

class SohuGlobalAPI:
    def __init__(self, base_url):
        self.base_url = base_url
        self.access_token = None
        self.logger = logging.getLogger(__name__)

    def login(self, phone, password):
        """登录获取 token"""
        try:
            url = f"{self.base_url}/auth/v2/login"
            data = {
                "phone": phone,
                "code": "",
                "loginType": "PASSWORD",
                "password": password,
                "userName": phone
            }
            response = requests.post(url, json=data)
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 200 and result.get('data', {}).get('accessToken'):
                    self.access_token = f"Bearer {result['data']['accessToken']}"
                    return True
            return False
        except Exception as e:
            self.logger.error(f"Login error: {str(e)}")
            return False

    def get_products(self, page_size=10, page_num=1, type=1):
        """获取商品列表"""
        try:
            if not self.access_token:
                return None

            url = f"{self.base_url}/shop-goods/product/pc/list"
            params = {
                "pageSize": page_size,
                "pageNum": page_num,
                "type": type
            }
            headers = {
                "Authorization": self.access_token,
                "version": "1.5.2"
            }
            
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Get products error: {str(e)}")
            return None

    def get_content_list(self, page_size=10, page_num=1, state="OnShelf", busy_type=None):
        """获取内容列表"""
        try:
            if not self.access_token:
                return None

            url = f"{self.base_url}/admin/playlet/list"
            params = {
                "pageSize": page_size,
                "pageNum": page_num,
                "state": state
            }
            if busy_type:
                params["busyType"] = busy_type

            headers = {
                "Authorization": self.access_token,
                "version": "1.5.2"
            }
            
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Get content list error: {str(e)}")
            return None

    def get_content_details(self, busy_code):
        """获取内容详情"""
        try:
            if not self.access_token:
                return None

            url = f"{self.base_url}/app/api/content/video/{busy_code}"
            headers = {
                "Authorization": self.access_token,
                "version": "1.5.2"
            }
            
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Get content details error: {str(e)}")
            return None

    def get_user_behavior(self, user_id, start_time=None, business_type=None, opera_type=None):
        """获取用户行为记录"""
        try:
            if not self.access_token:
                return None

            url = f"{self.base_url}/open/user/behavior/list"
            params = {"userId": user_id}
            
            if start_time:
                params["startTime"] = start_time
            if business_type:
                params["businessType"] = business_type
            if opera_type:
                params["operaType"] = opera_type

            headers = {
                "Authorization": self.access_token,
                "version": "1.5.2"
            }
            
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Get user behavior error: {str(e)}")
            return None