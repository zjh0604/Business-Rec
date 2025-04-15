import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any

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
        """计算时间权重"""
        try:
            op_time = datetime.strptime(operation_time, "%Y-%m-%d %H:%M:%S")
            time_diff = (datetime.now() - op_time).days
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
            behavior_strength = behavior.get('behavior_strength', 1.0)
            # 获取影响程度
            impact_level = behavior.get('impact_level', '中')
            impact_weights = {'高': 1.5, '中': 1.0, '低': 0.5}
            impact_weight = impact_weights.get(impact_level, 1.0)
            
            # 计算时间权重
            time_weight = self.calculate_time_weight(operation_time)
            
            # 计算基础分数
            base_score = behavior_strength * 10.0
            
            # 综合计算得分
            score = base_score * impact_weight * time_weight
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
            # 获取该性格特征相关的所有行为
            trait_behaviors = behavior_summary.get('trait_analysis', {}).get(trait, [])
            
            if not trait_behaviors:
                return current_score
            
            # 计算所有相关行为的得分
            total_score = 0.0
            for behavior in trait_behaviors:
                score = self.calculate_behavior_score(
                    behavior,
                    behavior.get('time', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                )
                total_score += score
            
            # 计算平均得分
            avg_score = total_score / len(trait_behaviors)
            
            # 更新当前分数
            new_score = current_score + (avg_score - 50) * 0.1  # 调整幅度
            return min(self.max_score, max(self.min_score, new_score))
            
        except Exception as e:
            logger.error(f"计算性格特征得分时出错: {str(e)}")
            return current_score

    def update_personality_scores(self, 
                                behavior_summary: Dict, 
                                current_scores: Dict[str, float]) -> Dict[str, float]:
        """更新所有性格特征分数"""
        updated_scores = {}
        
        for trait, current_score in current_scores.items():
            updated_scores[trait] = self.calculate_trait_score(
                trait, behavior_summary, current_score
            )
        
        return updated_scores

    def get_score_change_reasons(self, 
                                behavior_summary: Dict, 
                                trait: str) -> List[str]:
        """获取分数变化的原因"""
        reasons = []
        trait_behaviors = behavior_summary.get('trait_analysis', {}).get(trait, [])
        
        for behavior in trait_behaviors:
            reason = f"用户执行了{behavior['action']}操作"
            if 'content_title' in behavior:
                reason += f"（内容：{behavior['content_title']}）"
            if 'impact_level' in behavior:
                reason += f"，影响程度：{behavior['impact_level']}"
            reasons.append(reason)
        
        return reasons 