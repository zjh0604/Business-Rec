import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.font_manager as fm

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial Unicode MS', 'Microsoft YaHei', 'SimHei', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

def ensure_output_dir():
    """确保输出目录存在"""
    if not os.path.exists('output'):
        os.makedirs('output')

def create_custom_colormap():
    """创建自定义颜色映射"""
    colors = [
        '#0000FF',  # 深蓝
        '#0066FF',  # 中蓝
        '#00CCFF',  # 浅蓝
        '#00FF00',  # 绿色
        '#FFFF00',  # 黄色
        '#FF9900',  # 橙色
        '#FF3300',  # 橙红
        '#FF0000'   # 红色
    ]
    return LinearSegmentedColormap.from_list('custom', colors)

def create_heatmap(data, title, save_path=None, circular=False):
    """创建热力图
    
    Args:
        data: 包含特征和分数的字典
        title: 图表标题
        save_path: 保存路径
        circular: 是否使用圆形热力图
    """
    ensure_output_dir()
    
    try:
        # 准备数据并确保转换为浮点数
        traits = list(data.keys())
        scores = []
        for value in data.values():
            try:
                scores.append(float(value))
            except (ValueError, TypeError):
                scores.append(0.0)
        
        if circular:
            # 创建圆形热力图
            fig = plt.figure(figsize=(15, 15))
            ax = fig.add_subplot(111, projection='polar')
            
            # 计算角度
            angles = np.linspace(0, 2*np.pi, len(traits), endpoint=False)
            
            # 闭合图形
            scores = np.concatenate((scores, [scores[0]]))
            angles = np.concatenate((angles, [angles[0]]))
            traits = np.concatenate((traits, [traits[0]]))
            
            # 绘制雷达图
            ax.plot(angles, scores, 'o-', linewidth=2)
            ax.fill(angles, scores, alpha=0.25)
            
            # 设置刻度标签
            ax.set_xticks(angles[:-1])
            ax.set_xticklabels(traits[:-1], fontsize=12)
            
            # 设置标题
            plt.title(title, pad=20, fontsize=14)
            
            # 设置刻度范围
            ax.set_ylim(0, 100)
            
            # 添加网格
            ax.grid(True)
            
        else:
            # 创建普通热力图
            plt.figure(figsize=(25, 6))  # 增加图表宽度
            
            # 准备数据矩阵
            data_matrix = np.array(scores, dtype=float).reshape(1, -1)
            
            # 创建热力图
            sns.heatmap(data_matrix, 
                       annot=True, 
                       fmt='.0f',  # 使用整数格式
                       cmap=create_custom_colormap(),  # 使用自定义颜色
                       xticklabels=traits,
                       yticklabels=['分数'],
                       cbar_kws={'label': '分数'})
            
            # 设置标题和字体
            plt.title(title, fontsize=14, pad=20)
            plt.xticks(rotation=45, ha='right', fontsize=12)
            plt.yticks(fontsize=12)
            
            # 增加左右边距
            plt.subplots_adjust(left=0.05, right=0.95)
            
            # 调整布局，确保所有元素可见
            plt.tight_layout()
        
        # 保存图表
        if save_path:
            plt.savefig(os.path.join('output', save_path), 
                       bbox_inches='tight',  # 确保所有内容都在图中
                       dpi=300,
                       pad_inches=0.5)  # 增加边距
        plt.close()
        
    except Exception as e:
        print(f"Error creating heatmap: {str(e)}")
        raise

def create_comparison_heatmap(old_data, new_data, title, save_path=None):
    """创建对比热力图，显示新旧值的变化
    
    Args:
        old_data: 旧的特征和分数字典
        new_data: 新的特征和分数字典
        title: 图表标题
        save_path: 保存路径
    """
    ensure_output_dir()
    
    try:
        # 确保两个数据字典包含相同的特征
        all_traits = sorted(set(old_data.keys()) | set(new_data.keys()))
        
        # 准备数据并确保转换为浮点数
        old_scores = []
        new_scores = []
        for trait in all_traits:
            try:
                old_scores.append(float(old_data.get(trait, 0)))
            except (ValueError, TypeError):
                old_scores.append(0.0)
            try:
                new_scores.append(float(new_data.get(trait, 0)))
            except (ValueError, TypeError):
                new_scores.append(0.0)
        
        # 创建图表
        plt.figure(figsize=(25, 10))  # 增加图表大小
        
        # 创建两个子图
        plt.subplot(2, 1, 1)
        data_matrix_old = np.array(old_scores, dtype=float).reshape(1, -1)
        sns.heatmap(data_matrix_old, 
                   annot=True, 
                   fmt='.0f',  # 使用整数格式
                   cmap=create_custom_colormap(),  # 使用自定义颜色
                   xticklabels=all_traits,
                   yticklabels=['初始分数'],
                   cbar_kws={'label': '分数'})
        plt.title('Initial Personality Scores', fontsize=14, pad=20)
        plt.xticks(rotation=45, ha='right', fontsize=12)
        plt.yticks(fontsize=12)
        
        plt.subplot(2, 1, 2)
        data_matrix_new = np.array(new_scores, dtype=float).reshape(1, -1)
        sns.heatmap(data_matrix_new, 
                   annot=True, 
                   fmt='.0f',  # 使用整数格式
                   cmap=create_custom_colormap(),  # 使用自定义颜色
                   xticklabels=all_traits,
                   yticklabels=['更新分数'],
                   cbar_kws={'label': '分数'})
        plt.title('Updated Personality Scores', fontsize=14, pad=20)
        plt.xticks(rotation=45, ha='right', fontsize=12)
        plt.yticks(fontsize=12)
        
        # 设置总标题
        plt.suptitle(title, fontsize=16, y=1.02)
        
        # 增加左右边距
        plt.subplots_adjust(left=0.05, right=0.95)
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图表
        if save_path:
            plt.savefig(os.path.join('output', save_path), 
                       bbox_inches='tight',  # 确保所有内容都在图中
                       dpi=300,
                       pad_inches=0.5)  # 增加边距
        plt.close()
        
    except Exception as e:
        print(f"Error creating comparison heatmap: {str(e)}")
        raise 

def save_plot(fig, save_path):
    """保存图表"""
    try:
        # 确保输出目录存在
        os.makedirs('output', exist_ok=True)
        # 保存图表
        fig.savefig(os.path.join('output', save_path), 
                   bbox_inches='tight', 
                   dpi=300,
                   facecolor='white')
        plt.close(fig)
        return True
    except Exception as e:
        print(f"保存图表时出错: {str(e)}")
        return False 