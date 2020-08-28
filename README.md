# xbus

## 启动姿势

- 启动 mysql 和 etcd
    - 使用 sql 目录下的 `create_tables.sql` 建表
    - 冷启动数据去 qa 摸，或者开 ops-center 从头来点
- 编译 `go build`
- 运行 `./xbus gen-root` 生成 `rootcert.pem` 和 `rootkey.pem`
- 配置 `config.yaml`
    - 进入 qa 集群的 xbus pod，查看 `config.yaml`
    - 复制 `config.yaml` 配置到本地
    - 修改 mysql、etcd 配置
    - 复制 `xbus-api2cert.pem` 和 `xbus-api2key.pem`，修改 api 配置
- 运行 `./xbus run`

## 大致逻辑说明

xbus 本身主要做的事情是对 etcd 做二次封装，然后提供 http 接口

### 根目录

各种命令行配置，启动服务关键入口在 `cmd_run.go` 中

### api 目录

路由模块，各种流程主要入口在这里找。

- `server.go` 主要注册路由
- `api_xx.go` 为各种路由函数，主要做获取参数、检查参数、调用功能模块、返回
- `request.go` 获取参数的工具
- `response.go` 返回 json 用到的工具

### apps

xbus 关于 app 的相关逻辑所在目录

### configs

xbus 关于配置项的相关逻辑所在目录

### services

xbus 关于 rpc 服务的相关逻辑所在目录

