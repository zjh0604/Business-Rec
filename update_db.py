import sqlite3
import os

# 备份原始数据库
if os.path.exists('user.db'):
    conn = sqlite3.connect('user.db')
    cursor = conn.cursor()
    
    # 获取现有数据
    cursor.execute("SELECT * FROM personality")
    existing_data = cursor.fetchall()
    
    # 获取现有列名
    cursor.execute("PRAGMA table_info(personality)")
    existing_columns = [column[1] for column in cursor.fetchall()]
    print("现有列：", existing_columns)
    
    # 创建新的性格特征列表
    new_traits = [
        "Kindness", "Self_centeredness", "Competitive_mentality", "Cooperation_willingness", 
        "Social_skills", "Calmness", "Nervousness", "Adventurous_tendency", "Romance", 
        "Sense_of_responsibility", "Impulsiveness", "Frugality", "Luxury", "Pursuit_of_security", 
        "Adaptability", "Political_participation_willingness", "Moral_sensitivity", 
        "Decision_making_ability", "Honesty_and_trustworthiness", "Problem_solving_ability", 
        "Emotional_management_ability", "Prudence", "Time_management_ability", 
        "Love_of_novelty_and_dislike_of_the_old", "Toadyism", "Snobbishness", 
        "Resistance_to_being_managed", "Being_lenient_with_oneself_and_strict_with_others", 
        "Patience_and_understanding", "Sharing_and_caring", "Caring_about_others_needs", 
        "Listening_and_communication", "Praise_and_encouragement", "Providing_support", 
        "Emotional_regulation_ability", "Hedonism", "Health_awareness", "Open_mindedness", 
        "Respect_for_differences", "Sense_of_justice", "Sustainability_awareness", 
        "Risk_awareness", "Integrity", "Dedication", "Learning_attitude", "Self_discipline", 
        "Rationality", "Communication_skills", "Innovation_ability", "Optimism", 
        "Meticulousness_and_thoughtfulness", "Emotional_expression_ability", 
        "Stress_coping_ability", "Social_responsibility", 
        "Attitude_towards_handling_success_and_failure", "Confidence", "Resilience", 
        "Cooperation_awareness", "Decisiveness", "Conflict_management", "Love_for_food", 
        "Family_oriented", "Entertainment_lover", "Enthusiastic_about_charity", 
        "Tech_enthusiast", "Competitor", "Environmentalist", "Learner", "Humility", 
        "Spirit_of_exploration", "Outgoing", "Friendly", "Greed", "Selfishness", "Anxiety", 
        "Discontent", "Dependence", "Short_sightedness", "Aggressiveness", "Controlling_desire", 
        "Lack_of_empathy", "Indifference", "Irresponsibility", "Emotional_instability", 
        "Low_self_control", "Infringement_of_others_privacy", "Obsessive_compulsive_cleanliness", 
        "Fastidious_about_neatness", "Avoidance_of_public_goods", "Impatience", 
        "Meeting_fatigue", "Boredom_with_repetitive_work", "Avoidance_of_sports", 
        "Giving_up_long_term_goals", "Avoidance_of_team_responsibilities", 
        "Avoidance_of_daily_chores", "Frustration_and_discouragement", 
        "Avoidance_of_complex_problems", "Piety_and_loyalty", "Humility_and_awe", 
        "Fear_and_alertness", "Seeking_help", "Fear_of_taboos", "Resistance_to_change", 
        "Self_exploration", "Tolerance_and_acceptance", "Sensitivity", "Self_expression", 
        "Challenging_authority", "Critical_thinking", "Inspiration_and_intuition", "Empathy", 
        "Fairness_and_justice", "Kind_acts", "Respect_for_the_environment", 
        "Conflict_resolution", "Trust", "Reliability", "Self_management", "Patience", 
        "Adaptability", "Leadership", "Highly_goal_oriented", "Adventurous_spirit", 
        "Self_acceptance", "Humor", "Gratitude", "Caring_and_thoughtful", 
        "Inclusiveness_and_support", "Sacrifice_and_dedication", "Missing_someone", "Candor", 
        "Adherence_to_principles", "Self_awareness", "Loyalty", "Perseverance", 
        "Pursuit_of_excellence", "Deep_thinking", "Prudence_and_fastidiousness", 
        "Emotional_richness", "Concentration", "Sense_of_righteousness", 
        "Honesty_and_transparency", "Good_at_communication", "Understanding_and_tolerance", 
        "Self_consciousness", "Liberalism", "Human_rights_awareness", "Good_at_listening"
    ]
    
    # 过滤掉已存在的特征
    traits_to_add = [trait for trait in new_traits if trait not in existing_columns]
    print(f"需要添加 {len(traits_to_add)} 个新列")
    
    # 为每个新特征添加列
    for trait in traits_to_add:
        try:
            cursor.execute(f"ALTER TABLE personality ADD COLUMN {trait} INTEGER")
        except sqlite3.OperationalError as e:
            print(f"添加列 {trait} 时出错：{e}")
            continue
    
    # 为 userid=1 更新新的性格特征值
    new_values = {
        'Kindness': 80, 'Self_centeredness': 30, 'Competitive_mentality': 75,
        'Cooperation_willingness': 85, 'Social_skills': 92, 'Calmness': 88,
        'Nervousness': 45, 'Adventurous_tendency': 68, 'Romance': 72,
        'Sense_of_responsibility': 95, 'Impulsiveness': 50, 'Frugality': 60,
        'Luxury': 35, 'Pursuit_of_security': 70, 'Adaptability': 82,
        'Political_participation_willingness': 55, 'Moral_sensitivity': 89,
        'Decision_making_ability': 78, 'Honesty_and_trustworthiness': 99,
        'Problem_solving_ability': 87, 'Emotional_management_ability': 94,
        'Prudence': 73, 'Time_management_ability': 86,
        'Love_of_novelty_and_dislike_of_the_old': 40, 'Toadyism': 25,
        'Snobbishness': 20, 'Resistance_to_being_managed': 35,
        'Being_lenient_with_oneself_and_strict_with_others': 30,
        'Patience_and_understanding': 91, 'Sharing_and_caring': 97,
        'Caring_about_others_needs': 98, 'Listening_and_communication': 95,
        'Praise_and_encouragement': 94, 'Providing_support': 93,
        'Emotional_regulation_ability': 92, 'Hedonism': 65,
        'Health_awareness': 85, 'Open_mindedness': 88,
        'Respect_for_differences': 90, 'Sense_of_justice': 91,
        'Sustainability_awareness': 87, 'Risk_awareness': 75,
        'Integrity': 99, 'Dedication': 98, 'Learning_attitude': 97,
        'Self_discipline': 96, 'Rationality': 95, 'Communication_skills': 94,
        'Innovation_ability': 93, 'Optimism': 92,
        'Meticulousness_and_thoughtfulness': 91,
        'Emotional_expression_ability': 90, 'Stress_coping_ability': 89,
        'Social_responsibility': 88,
        'Attitude_towards_handling_success_and_failure': 87,
        'Confidence': 86, 'Resilience': 85, 'Cooperation_awareness': 84,
        'Decisiveness': 83, 'Conflict_management': 82, 'Love_for_food': 75,
        'Family_oriented': 70, 'Entertainment_lover': 65,
        'Enthusiastic_about_charity': 95, 'Tech_enthusiast': 80,
        'Competitor': 75, 'Environmentalist': 90, 'Learner': 95,
        'Humility': 92, 'Spirit_of_exploration': 93, 'Outgoing': 88,
        'Friendly': 97, 'Greed': 25, 'Selfishness': 30, 'Anxiety': 45,
        'Discontent': 50, 'Dependence': 40, 'Short_sightedness': 35,
        'Aggressiveness': 20, 'Controlling_desire': 30,
        'Lack_of_empathy': 25, 'Indifference': 20, 'Irresponsibility': 15,
        'Emotional_instability': 35, 'Low_self_control': 25,
        'Infringement_of_others_privacy': 10,
        'Obsessive_compulsive_cleanliness': 40,
        'Fastidious_about_neatness': 55, 'Avoidance_of_public_goods': 30,
        'Impatience': 35, 'Meeting_fatigue': 40,
        'Boredom_with_repetitive_work': 45, 'Avoidance_of_sports': 50,
        'Giving_up_long_term_goals': 25,
        'Avoidance_of_team_responsibilities': 20,
        'Avoidance_of_daily_chores': 30,
        'Frustration_and_discouragement': 35,
        'Avoidance_of_complex_problems': 40, 'Piety_and_loyalty': 90,
        'Humility_and_awe': 85, 'Fear_and_alertness': 45,
        'Seeking_help': 60, 'Fear_of_taboos': 55,
        'Resistance_to_change': 40, 'Self_exploration': 80,
        'Tolerance_and_acceptance': 95, 'Sensitivity': 85,
        'Self_expression': 90, 'Challenging_authority': 75,
        'Critical_thinking': 85, 'Inspiration_and_intuition': 80,
        'Empathy': 95, 'Fairness_and_justice': 98, 'Kind_acts': 99,
        'Respect_for_the_environment': 97, 'Conflict_resolution': 96,
        'Trust': 95, 'Reliability': 94, 'Self_management': 93,
        'Patience': 92, 'Adaptability': 91, 'Leadership': 90,
        'Highly_goal_oriented': 89, 'Adventurous_spirit': 88,
        'Self_acceptance': 87, 'Humor': 86, 'Gratitude': 85,
        'Caring_and_thoughtful': 84, 'Inclusiveness_and_support': 83,
        'Sacrifice_and_dedication': 82, 'Missing_someone': 81,
        'Candor': 79, 'Adherence_to_principles': 78,
        'Self_awareness': 77, 'Loyalty': 76, 'Perseverance': 75,
        'Pursuit_of_excellence': 74, 'Deep_thinking': 73,
        'Prudence_and_fastidiousness': 72, 'Emotional_richness': 71,
        'Concentration': 70, 'Sense_of_righteousness': 69,
        'Honesty_and_transparency': 67, 'Good_at_communication': 65,
        'Understanding_and_tolerance': 64, 'Self_consciousness': 63,
        'Liberalism': 62, 'Human_rights_awareness': 61,
        'Good_at_listening': 60
    }
    
    # 只更新新添加的列的值
    update_cols = ', '.join([f"{k} = {v}" for k, v in new_values.items() if k in traits_to_add])
    if update_cols:
        cursor.execute(f"UPDATE personality SET {update_cols} WHERE id = 1")
    
    # 提交更改
    conn.commit()
    conn.close()
    
    print("数据库更新完成") 