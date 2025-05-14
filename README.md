 # 个性化推荐系统

这是一个基于 LLM 的商单推荐系统

## 功能特点

- 个性化推荐
- 用户界面


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
3.下载嵌入模型
```bash
git lfs install

git clone https://www.modelscope.cn/thomas/text2vec-base-chinese.git (可换为其他嵌入模型)
```

3. 运行应用：
```bash
python business_web_app.py
```

## 使用说明

1. 访问主页：http://localhost:5000


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
