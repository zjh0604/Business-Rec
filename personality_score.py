import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any
import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class PersonalityScoreCalculator:
    def __init__(self, 
                 time_decay_factor: float = 0.1,  # 时间衰减因子
                 base_time_window: int = 30,      # 基础时间窗口（天）
                 min_score: float = 0.0,          # 最小分数
                 max_score: float = 100.0):       # 最大分数
        self.time_decay_factor = time_decay_factor
        self.base_time_window = base_time_window
        self.min_score = min_score
        self.max_score = max_score

    def calculate_time_weight(self, operation_time: str) -> float:
        """计算时间权重，使用指数衰减函数"""
        try:
            # 解析操作时间
            op_time = datetime.strptime(operation_time, "%Y-%m-%d")
            current_time = datetime.now()
            time_diff = (current_time - op_time).days
            
            # 使用指数衰减函数计算权重
            weight = np.exp(-self.time_decay_factor * time_diff / self.base_time_window)
            return float(weight)
        except Exception as e:
            logger.error(f"计算时间权重时出错: {str(e)}")
            return 0.0

    def calculate_behavior_score(self, 
                               behavior: Dict,
                               operation_time: str) -> float:
        """计算单个行为的得分"""
        try:
            # 获取行为强度
            behavior_strength = behavior.get('behavior_strength', '中')
            strength_weights = {'高': 1.5, '中': 1.0, '低': 0.5, '较高': 1.3, '较低': 0.7}
            strength_weight = strength_weights.get(behavior_strength, 1.0)
            
            # 获取影响程度
            impact_level = behavior.get('impact_level', '中')
            impact_weights = {'高': 1.5, '中': 1.0, '低': 0.5}
            impact_weight = impact_weights.get(impact_level, 1.0)
            
            # 计算时间权重
            time_weight = self.calculate_time_weight(operation_time)
            
            # 计算基础分数
            base_score = 50.0  # 基础分数设为50
            
            # 综合计算得分
            score = base_score * strength_weight * impact_weight * time_weight
            return min(self.max_score, max(self.min_score, score))
            
        except Exception as e:
            logger.error(f"计算行为得分时出错: {str(e)}")
            return 0.0

    def calculate_trait_score(self, 
                            trait: str, 
                            behavior_summary: Dict, 
                            current_score: float) -> float:
        """计算特定性格特征的得分"""
        try:
            # 修复：如果 current_score 为 None，赋默认值 0.0
            if current_score is None:
                current_score = 0.0
            # 获取该性格特征相关的所有行为
            trait_behaviors = behavior_summary.get('trait_analysis', {}).get(trait, {})
            
            if not trait_behaviors:
                return current_score
            
            # 获取相关行为列表
            related_behaviors = trait_behaviors.get('related_behaviors', [])
            if not related_behaviors:
                return current_score
            
            # 从 raw_stats 中获取时间信息
            time_distribution = behavior_summary.get('raw_stats', {}).get('time_distribution', {})
            operation_time = None
            if time_distribution:
                operation_time = list(time_distribution.keys())[0]  # 使用最新的时间
            
            if not operation_time:
                operation_time = datetime.now().strftime("%Y-%m-%d")
            
            # 计算所有相关行为的得分
            total_score = 0.0
            for behavior in related_behaviors:
                # 创建行为字典
                behavior_dict = {
                    'behavior_strength': trait_behaviors.get('behavior_strength', '中'),
                    'impact_level': trait_behaviors.get('impact_level', '中')
                }
                score = self.calculate_behavior_score(behavior_dict, operation_time)
                total_score += score
            
            # 计算平均得分
            avg_score = total_score / len(related_behaviors)
            
            # 更新当前分数，使用加权平均
            new_score = current_score * 0.7 + avg_score * 0.3
            return min(self.max_score, max(self.min_score, new_score))
            
        except Exception as e:
            logger.error(f"计算性格特征得分时出错: {str(e)}")
            return current_score

    def save_scores_to_db(self, user_id: int, new_scores: dict, db_path="user.db"):
        logger.warning("save_scores_to_db called!")
        logger.info(f"Using database at: {os.path.abspath(db_path)}")
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            set_clause = ", ".join([f"{trait} = ?" for trait in new_scores.keys()])
            sql = f"UPDATE personality SET {set_clause} WHERE id = ?"
            values = list(new_scores.values()) + [user_id]
            cursor.execute(sql, values)
            conn.commit()
            logger.info(f"用户{user_id}的新分数已写入数据库")
        except Exception as e:
            logger.error(f"写入数据库时出错: {e}")
        finally:
            conn.close()

    def update_personality_scores(self, 
                                behavior_summary: Dict, 
                                current_scores: Dict[str, float],
                                user_id: int,
                                db_path: str = "user.db") -> Dict[str, float]:
        """更新所有性格特征分数，并在每次trait分数更新后写回数据库"""
        updated_scores = {}
        for trait, current_score in current_scores.items():
            updated_scores[trait] = self.calculate_trait_score(
                trait, behavior_summary, current_score
            )
            # 每次trait分数更新后立即写回数据库
            self.save_scores_to_db(user_id, updated_scores, db_path)
        return updated_scores

    def get_score_change_reasons(self, 
                                behavior_summary: Dict, 
                                trait: str) -> List[str]:
        """获取分数变化的原因"""
        reasons = []
        trait_behaviors = behavior_summary.get('trait_analysis', {}).get(trait, {})
        
        if not trait_behaviors:
            return reasons
        
        related_behaviors = trait_behaviors.get('related_behaviors', [])
        behavior_strength = trait_behaviors.get('behavior_strength', '中')
        impact_level = trait_behaviors.get('impact_level', '中')
        
        for behavior in related_behaviors:
            reason = f"用户{behavior}，行为强度：{behavior_strength}，影响程度：{impact_level}"
            reasons.append(reason)
        
        return reasons 