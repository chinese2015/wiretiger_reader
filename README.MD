# MongoDB WiredTiger 文件读取工具

这个工具用于直接读取MongoDB 5.0.25版本的WiredTiger存储引擎文件。

## 环境要求

- Python 3.7+
- MongoDB 5.0.25
- WiredTiger 3.3.0

## 安装

1. 安装依赖：
```bash
pip install -r requirements.txt
```

## 使用方法

1. 列出所有集合：
```bash
python wt_reader.py <数据目录路径>
```

2. 读取特定集合的数据：
```bash
python wt_reader.py <数据目录路径> <集合名称> [限制数量]
```

## 注意事项

1. 确保数据目录包含完整的WiredTiger文件（包括WiredTiger.wt, WiredTiger.turtle等）
2. 程序以只读模式打开数据库，不会修改任何数据
3. 如果遇到权限问题，请确保有足够的文件读取权限

## 示例

```bash
# 列出所有集合
python wt_reader.py /path/to/mongodb/data

# 读取users集合的前10条数据
python wt_reader.py /path/to/mongodb/data users 10
``` 
