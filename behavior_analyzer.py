import logging
from typing import Dict, List, Any
from db_operation import answer_user_query

logger = logging.getLogger(__name__)

class BehaviorAnalyzer:
    def __init__(self):
        self.rag_db = "rag_shqp"
        
    async def analyze_user_behavior(self, user_id: int, operations: List[Dict]) -> Dict:
        """分析用户行为并生成行为总结"""
        try:
            # 构建分析提示词
            prompt = f"""
            请分析以下用户行为数据，并生成详细的行为总结：

            [用户行为数据]
            {self._format_operations(operations)}

            请按以下步骤进行分析：

            1. 行为分类：
            - 将用户行为按类型分类（如内容消费、社交互动、内容创作等）
            - 统计每种类型的行为频率和强度

            2. 行为模式识别：
            - 识别用户的行为习惯和偏好
            - 分析用户在不同时间段的行为特点
            - 发现用户的行为变化趋势

            3. 性格特征关联：
            - 根据知识库中的标准，将行为与性格特征关联
            - 为每个重要行为标注其反映的性格特征
            - 说明行为与性格特征之间的关联依据

            请按以下格式输出分析结果：

            === 用户行为分析 ===
            用户ID：{user_id}

            [行为分类统计]
            1. 内容消费：
               - 行为类型：[具体行为]
                 次数：[具体数字]
                 反映的性格特征：[特征1, 特征2...]
                 关联依据：[知识库中的具体标准]

            2. 社交互动：
               - 行为类型：[具体行为]
                 次数：[具体数字]
                 反映的性格特征：[特征1, 特征2...]
                 关联依据：[知识库中的具体标准]

            [行为模式总结]
            1. 时间分布：
               - [具体时间段]：[行为特点]
               - [具体时间段]：[行为特点]

            2. 内容偏好：
               - 偏好类型：[具体类型]
                 具体表现：[具体行为]
                 反映的性格特征：[特征1, 特征2...]

            [性格特征分析]
            1. [性格特征1]：
               - 相关行为：[具体行为列表]
               - 行为强度：[具体数值]
               - 影响程度：[高/中/低]

            2. [性格特征2]：
               - 相关行为：[具体行为列表]
               - 行为强度：[具体数值]
               - 影响程度：[高/中/低]
            """
            
            # 使用RAG进行分析
            analysis_result = await answer_user_query(self.rag_db, prompt)
            
            # 解析分析结果
            behavior_summary = self._parse_analysis_result(analysis_result)
            
            return behavior_summary
            
        except Exception as e:
            logger.error(f"分析用户行为时出错: {str(e)}")
            raise

    def _format_operations(self, operations: List[Dict]) -> str:
        """格式化用户操作数据"""
        formatted_ops = []
        for op in operations:
            formatted_op = f"""
            时间：{op['time']}
            行为：{op['action']}
            内容类型：{op['detail']['business_type']}
            内容标题：{op['detail'].get('title', 'N/A')}
            交互次数：{op['detail'].get('comments_count', 0)}
            """
            formatted_ops.append(formatted_op)
        return "\n".join(formatted_ops)

    def _parse_analysis_result(self, analysis_result: str) -> Dict:
        """解析分析结果"""
        # 这里需要实现具体的解析逻辑
        # 将文本分析结果转换为结构化的数据
        # 返回包含行为总结、性格特征关联等信息的字典
        pass 