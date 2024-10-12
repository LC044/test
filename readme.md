# 在线创建索引测试

## 安装依赖

```shell
pip install -r requirements.txt
```

## 修改openGauss配置

改成本地openGauss对应配置

./database/opengauss.py

```python
connection_params = {
    'host': '127.0.0.1',
    'port': '33000',
    'dbname': 'postgres',
    'user': 'shuaikangzhou',
    'password': 'zhou@123'
}
```

## 运行main.py

```shell
python main.py
```