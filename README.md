<p align="center"><h1 style="font-size: 50px" align="center">py-seal</h1></p>
<p align="center">
    <em>基于 fastapi 的一个快速开发框架，更简约的注解、链式构建 sql 语句</em>
</p>
<p align="center">
<a href="https://pypi.org/project/py-seal" target="_blank">
    <img src="https://img.shields.io/pypi/v/py-seal?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/py-seal" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/fastapi.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>

---
## 集成
- [x] fastapi
- [x] websocket
- [x] loguru
- [x] sqlite
- [x] mysql
- [x] cache
- [x] schedule


## 安装
```shell
pip install py-seal
```

## 例子
### 配置文件示例
```yaml
seal:
  authorization:
    jwt_key: py-seal
    expire: 3600
    excludes:
      - /test/**
  mysql:
    host: 127.0.0.1
    port: 3306
    user: root
    password: root
    database: test
```

### 接口
```python
@post("/login/submit")
async def projects(param: UserLoginParam):
    user = Query(User).eq("username", param.username).first()
    if user is None or user.password != param.password:
        raise BusinessException(message="用户名或密码错误！")
    return generate_token(uid=user.id, username=user.username)
```
### 定义实体
支持自定义和动态读取 model
```python
@entity(table='user')
class User(BaseEntity):
    id: int
    username: str
    
@entity(table='role', dynamic=True)
class Role(BaseEntity):
    ...
```
### sql 查询
mybatis plus 风格的链式构建 sql 语句
```python
Query(User).eq("id", 1).first()
Query(table='user').eq("id", 1).first()
Query(table='user').list()
Update(User).set(status=1).eq("id", 1).update()
Update(User).eq("id", 1).delete()
```


## License
GNU GENERAL PUBLIC LICENSE

## About
https://www.tiangua.info/blog/2e16a83a-24eb-4472-bfa4-187430f84be1