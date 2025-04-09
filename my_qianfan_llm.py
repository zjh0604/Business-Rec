import os

from langchain_community.llms.baidu_qianfan_endpoint import QianfanLLMEndpoint

# Set environment variables for Qianfan authentication
os.environ["QIANFAN_AK"] = "5W9dFLCns7XBPFMlaYn65ebp"
os.environ["QIANFAN_SK"] = "1pqLwJPyl7aDczuD2yCmS1uydBqbzvr5"

llm = QianfanLLMEndpoint(model="ERNIE-4.0-Turbo-8K")


