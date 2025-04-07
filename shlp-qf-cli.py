import os
import sqlite3

import qianfan

# 【不推荐】使用应用AK/SK调用流程
os.environ["QIANFAN_AK"] = "5W9dFLCns7XBPFMlaYn65ebp"
os.environ["QIANFAN_SK"] = "1pqLwJPyl7aDczuD2yCmS1uydBqbzvr5"

chat_completion = qianfan.ChatCompletion()


# 输入id查询用户信息
def query_id(user_id):
    # 连接数据库
    conn = sqlite3.connect("user.db")
    print("Opened database successfully")
    c = conn.cursor()
    # 查询数据
    cursor = c.execute("SELECT * from user_profile where id = " + user_id)
    for row in cursor:
        print("ID = ", row[0])
        print("趋炎附势 = ", row[1])
        print("喜新厌旧 = ", row[2])
    return row


# 输入id
user_id = input("请输入用户id：")
profile = query_id(user_id)

# 输入操作日志
user_operation_diary = """用户的操作日志：
[
  {
    "timestamp": "2024-07-29T10:00:00Z",
    "userId": "1",
    "action": "ClickedBack",
    "object": "BeautyVideo",
    "objectId": "video001",
    "details": ""
  },
  {
    "timestamp": "2024-07-29T10:05:00Z",
    "userId": "1",
    "action": "ClickedProduct",
    "object": "MenClothing",
    "objectId": "product101",
    "details": ""
  },
  {
    "timestamp": "2024-07-29T10:06:00Z",
    "userId": "1",
    "action": "ClickedBack",
    "objectID": "product101",
    "details": ""
  },
  {
    "timestamp": "2024-07-29T10:07:00Z",
    "userId": "1",
    "action": "ClickedProduct",
    "object": "MenClothing",
    "objectId": "product102",
    "details": ""
  }
]
"""

user_profile = (
        "用户的三魂六魄画像：id = "
        + user_id
        + ", 趋炎附势 = "
        + str(profile[1])
        + ", 喜新厌旧 = "
        + str(profile[2])
        + "."
)

prompt_diary = "以上是该用户的操作日志，请按以下格式进行总结：“用户x点击了x次xx”...注意不要生成其他的信息，不需要解释你的回答。"

# 指定特定模型
resp = chat_completion.do(
    model="ERNIE-Lite-8K-0922",
    messages=[{"role": "user", "content": user_operation_diary + prompt_diary}],
)

# print(resp["body"])
result_summary = resp["result"]
print("***********第1步***********")
print(result_summary)

prompt_profile = "请根据用户的操作和画像，生成一个新的用户画像，该用户画像应该包含用户的id，趋炎附势，喜新厌旧的更新的分数（不需要写出计算过程，不是相对原有分数的相对变化，而是直接给出绝对数字结果），范围为0~100，相较原有的分数的变化幅度为0~10，以及你认为的用户的特点。输出格式为：id=xx\n 趋炎附势=xx\n 喜新厌旧=xx\n 新的趋炎附势分数=xx\n 新的喜新厌旧分数=xx\n 用户特点=?"

# 指定特定模型
resp = chat_completion.do(
    model="ERNIE-Lite-8K-0922",
    messages=[
        {
            "role": "user",
            "content": "用户画像：" + user_profile + result_summary + prompt_profile,
        }
    ],
)

result_profile = resp["result"]
print("***********第2步***********")
print(result_profile)

# 生成推荐列表
recommended_projects = [
    "4K超高清智能电视",
    "节能型智能洗衣机",
    "多功能电动剃须刀",
    "有机冷压橄榄油",
    "智能跑步机",
    "环保材料制成的休闲鞋",
    "天然成分护肤套装",
    "教育互动机器人",
    "便携式太阳能帐篷灯",
    "可定制的智能壁挂画",
]

prompt_item_list = "请根据用户的操作和画像，生成一个排好序的推荐列表，推荐列表中应该包含3个产品，每个产品的名称应该是一个字符串，输出格式为：产品1\n 产品2\n 产品3\n ..."

# 指定特定模型
resp = chat_completion.do(
    model="ERNIE-Lite-8K-0922",
    messages=[
        {
            "role": "user",
            "content": "用户画像："
                       + result_profile
                       + "待推荐的项目列表："
                       + str(recommended_projects)
                       + prompt_item_list,
        }
    ],
)

result_item_list = resp["result"]
print("***********第3步***********")
print(result_item_list)
