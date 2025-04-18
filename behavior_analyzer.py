import logging
from typing import Dict, List, Any
from db_operation import answer_user_query
import re
import json
import sqlite3

logger = logging.getLogger(__name__)

class BehaviorAnalyzer:
    def __init__(self):
        self.rag_db = "rag_shqp"
        
    def _get_valid_traits(self) -> List[str]:
        """从数据库获取有效的性格特征列表"""
        try:
            conn = sqlite3.connect("user.db")
            c = conn.cursor()
            # 获取personality表的所有列名
            cursor = c.execute("SELECT * FROM personality LIMIT 1")
            columns = [description[0] for description in cursor.description]
            conn.close()
            # 移除'id'列
            if 'id' in columns:
                columns.remove('id')
            return columns
        except Exception as e:
            logger.error(f"获取性格特征列表时出错: {str(e)}")
            return []
        
    def _normalize_trait_name(self, trait: str) -> str:
        """标准化特征名称，移除括号内的中文注释等"""
        # 移除括号及其内容
        trait = re.sub(r'\（.*?\）|\(.*?\)', '', trait)
        # 移除空格
        trait = trait.strip()
        return trait

    async def analyze_user_behavior(self, user_id: int, operations: List[Dict]) -> Dict:
        """分析用户行为并生成行为总结"""
        try:
            logger.debug(f"Starting behavior analysis for user {user_id}")
            logger.debug(f"Operations to analyze: {operations}")
            
            # 获取有效的性格特征列表
            valid_traits = self._get_valid_traits()
            logger.debug(f"Valid personality traits: {valid_traits}")
            
            # 统计用户行为
            behavior_stats = {
                'content_consumption': {},  # 内容消费统计
                'interaction_stats': {},    # 交互统计
                'time_distribution': {},    # 时间分布
                'category_preference': {}   # 内容类别偏好
            }
            
            # 分析用户行为
            for op in operations:
                # 获取操作时间
                operation_time = op.get('t', '')
                time_key = operation_time[:10]  # 只取日期部分
                
                # 获取操作类型
                action = op.get('a', '')
                
                # 获取业务类型和类别
                business_type = op.get('b', '')
                category = op.get('c', '')
                
                # 更新内容消费统计
                if business_type not in behavior_stats['content_consumption']:
                    behavior_stats['content_consumption'][business_type] = {
                        'count': 0,
                        'categories': {}
                    }
                behavior_stats['content_consumption'][business_type]['count'] += 1
                
                # 更新类别偏好
                if category not in behavior_stats['content_consumption'][business_type]['categories']:
                    behavior_stats['content_consumption'][business_type]['categories'][category] = 0
                behavior_stats['content_consumption'][business_type]['categories'][category] += 1
                
                # 更新时间分布
                if time_key not in behavior_stats['time_distribution']:
                    behavior_stats['time_distribution'][time_key] = 0
                behavior_stats['time_distribution'][time_key] += 1
            
            logger.debug(f"Behavior stats collected: {behavior_stats}")
            
            # 构建分析提示词
            prompt = f"""
            请分析以下用户行为数据，并生成详细的行为总结。特别注意：性格特征分析必须严格使用以下列出的性格特征，不能添加任何额外说明或括号注释。

            [可用的性格特征列表]
            {', '.join(valid_traits)}

            [用户行为统计]
            用户ID：{user_id}
            
            [内容消费统计]
            {json.dumps(behavior_stats['content_consumption'], ensure_ascii=False, indent=2)}
            
            [时间分布]
            {json.dumps(behavior_stats['time_distribution'], ensure_ascii=False, indent=2)}
            
            请按以下步骤进行分析：

            1. 行为分类统计：
            - 统计每种类型的行为频率
            - 特别关注用户对不同类型内容的偏好

            2. 性格特征关联（必须严格使用上述列出的性格特征，不要添加任何注释）：
            - 将用户行为与已有的性格特征关联
            - 每个特征必须完全匹配上述列表中的名称
            - 如果某个行为无法与已有特征关联，请忽略该行为
            - 用户偏好内容的类型可以与性格特征关联
            - 尽可能关联1-3个性格特征

            请按以下格式输出分析结果：

            === 用户行为分析 ===
            用户ID：{user_id}

            [行为分类统计]
            1. 内容消费：
               - 行为类型：[具体行为类型]
                 次数：[具体数字]

            2. 社交互动：
               - 行为类型：[具体行为]
                 次数：[具体数字]

            [性格特征分析]
            1. [特征名称]：
               - 相关行为：[具体行为列表]
               - 行为强度：[具体数值]
               - 影响程度：[高/中/低]

            注意：特征名称必须完全匹配提供的列表，不要添加任何额外说明或括号注释。
            """
            
            logger.debug("Starting RAG analysis")
            # 使用RAG进行分析
            analysis_result = ''.join(answer_user_query(self.rag_db, prompt))
            logger.debug(f"RAG analysis result: {analysis_result}")
            
            # 解析分析结果
            behavior_summary = self._parse_analysis_result(analysis_result)
            logger.debug(f"Parsed behavior summary: {behavior_summary}")
            
            # 验证并过滤性格特征分析结果
            filtered_trait_analysis = {}
            for trait, analysis in behavior_summary.get('trait_analysis', {}).items():
                normalized_trait = self._normalize_trait_name(trait)
                if normalized_trait in valid_traits:
                    filtered_trait_analysis[normalized_trait] = analysis
                else:
                    logger.warning(f"Removing invalid trait from analysis: {trait} (normalized: {normalized_trait})")
            
            behavior_summary['trait_analysis'] = filtered_trait_analysis
            
            # 添加原始统计数据
            behavior_summary['raw_stats'] = behavior_stats
            
            logger.debug(f"Final behavior summary: {behavior_summary}")
            return behavior_summary
            
        except Exception as e:
            logger.error(f"分析用户行为时出错: {str(e)}", exc_info=True)
            raise

    def _parse_analysis_result(self, analysis_result: str) -> Dict:
        """解析分析结果，将文本分析结果转换为结构化的数据"""
        try:
            result = {
                'behavior_categories': {},
                'trait_analysis': {},
                'raw_stats': {}
            }
            
            # 解析行为分类统计
            category_pattern = r'(\d+)\.\s*([^：]+)：\s*- 行为类型：([^\n]+)\s*次数：(\d+)'
            for match in re.finditer(category_pattern, analysis_result):
                category_name = match.group(2).strip()
                result['behavior_categories'][category_name] = {
                    'behavior_type': match.group(3).strip(),
                    'count': int(match.group(4))
                }
            
            # 解析性格特征分析
            trait_pattern = r'(\d+)\.\s*([^：]+)：\s*- 相关行为：([^\n]+)\s*- 行为强度：([^\n]+)\s*- 影响程度：([^\n]+)'
            for match in re.finditer(trait_pattern, analysis_result):
                trait_name = match.group(2).strip()
                result['trait_analysis'][trait_name] = {
                    'related_behaviors': [b.strip() for b in match.group(3).split(',')],
                    'behavior_strength': match.group(4).strip(),
                    'impact_level': match.group(5).strip()
                }
            
            return result
            
        except Exception as e:
            logger.error(f"解析分析结果时出错: {str(e)}")
            return {
                'behavior_categories': {},
                'trait_analysis': {},
                'raw_stats': {}
            } 