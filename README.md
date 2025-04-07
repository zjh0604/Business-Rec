 # 个性化推荐系统

这是一个基于 LLM 的个性化推荐系统，能够根据用户的行为和性格特征提供个性化的推荐。

## 功能特点

- 用户行为分析
- 性格特征分析
- 个性化推荐
- 用户管理界面
- 数据可视化

## 技术栈

- Python
- Flask
- SQLite
- Bootstrap
- Chart.js

## 安装说明

1. 克隆仓库：
```bash
git clone [你的仓库URL]
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 运行应用：
```bash
python web_app.py
```

## 使用说明

1. 访问主页：http://localhost:8080
2. 在用户管理页面可以查看所有用户
3. 点击"查看性格"可以查看用户的性格特征分布
4. 点击"查看波动"可以查看用户最近的性格变化
5. 点击"CURD"可以进行用户操作

## 项目结构

```
├── app.py              # 核心应用逻辑
├── web_app.py          # Web应用入口
├── templates/          # HTML模板
├── static/            # 静态文件
├── requirements.txt   # 项目依赖
└── README.md         # 项目文档
```


## 许可证

MIT License