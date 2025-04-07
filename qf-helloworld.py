import os

import qianfan

# 【不推荐】使用应用AK/SK调用流程
os.environ["QIANFAN_AK"] = "5W9dFLCns7XBPFMlaYn65ebp"
os.environ["QIANFAN_SK"] = "1pqLwJPyl7aDczuD2yCmS1uydBqbzvr5"

chat_completion = qianfan.ChatCompletion()

# 指定特定模型
resp = chat_completion.do(model="ERNIE-Lite-8K-0922", messages=[{
    "role": "user",
    "content": "ni hao"
}])

print(resp["body"])
