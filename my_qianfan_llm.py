import os
from dotenv import load_dotenv
from langchain_community.llms.baidu_qianfan_endpoint import QianfanLLMEndpoint

# Load environment variables
load_dotenv()

# Get credentials from environment variables
QIANFAN_AK = os.getenv("QIANFAN_AK")
QIANFAN_SK = os.getenv("QIANFAN_SK")

# Initialize Qianfan LLM
llm = QianfanLLMEndpoint(model="ERNIE-4.0-Turbo-8K")


