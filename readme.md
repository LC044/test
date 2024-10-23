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

```sql
SELECT
    c.oid AS tuple_id,               -- 数据行的OID
    c.reltuples AS tuple_count,      -- 数据行的计数
    t.xmin AS xmin,                  -- 记录插入的事务ID
    t.xmax AS xmax                   -- 记录删除的事务ID
FROM
    pg_class c                        -- 表的系统目录
JOIN
    pg_index i ON c.oid = i.indrelid -- 索引与表关联
JOIN
    pg_attribute a ON a.attrelid = c.oid
JOIN
    pg_statistic s ON s.starelid = c.oid
JOIN
    (SELECT
        t.oid,
        xmin,
        xmax
     FROM
        users t
    ) AS t ON t.oid = c.oid
WHERE
    i.indexrelid = 'idx_users_name_email'::regclass;

SELECT
    t.oid AS tuple_id,               -- 数据行的OID
    t.xmin AS xmin,                  -- 记录插入的事务ID
    t.xmax AS xmax                   -- 记录删除的事务ID
FROM
    users t                 -- 直接从目标表中查询
JOIN
    pg_index i ON i.indrelid = 'users'::regclass -- 替换为表名
JOIN
    pg_class c ON c.oid = i.indexrelid
JOIN
    pg_attribute a ON a.attrelid = c.oid
JOIN
    pg_statistic s ON s.starelid = c.oid
WHERE
    i.indexrelid = 'idx_users_name_email'::regclass; -- 替换为索引名


```