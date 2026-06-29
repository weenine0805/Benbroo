# Benbroo 开发者文档

## 1. 项目简介

Benbroo 是一个用 Go 语言开发的微服务注册中心与配置管理平台，提供以下核心能力：

- **服务注册与发现** — 微服务实例注册、注销、查询、心跳维护，支持 4 种负载均衡策略（轮询/随机/加权/一致性哈希）
- **配置管理** — 配置的发布、查询、删除、版本历史、长轮询监听变更
- **健康检查** — 主动探测（TCP/HTTP）+ 被动探测（消费端失败上报）+ 心跳超时检测
- **变更订阅** — 长轮询监听配置变更
- **命名空间隔离** — 多环境/多租户隔离
- **集群管理** — 多节点部署、Leader 选举、健康检查分片、集群间数据同步
- **DNS 服务** — 基于 DNS 的服务发现（A 记录/SRV 记录），支持加权路由
- **多协议通信** — HTTP REST、gRPC、Raw TCP Socket、DNS 四种通信协议
- **Client SDK** — Go 客户端 SDK，支持所有协议与自动心跳
- **Web 控制台** — 可视化管理服务、配置、命名空间

---

## 2. 快速开始

### 2.1 环境要求

| 依赖 | 版本要求 |
|------|---------|
| Go | 1.22+ |
| MySQL | 5.7+ / 8.0+ |

### 2.2 编译

```bash
cd d:\myproject\Benbroo
go mod tidy
go build -o build/benbroo.exe ./cmd/server/
```

### 2.3 配置

编辑 `configs/server.yaml`：

```yaml
server:
  port: 8848           # HTTP REST 端口（默认 8848）
  grpcPort: 9848       # gRPC 端口（默认 HTTP+1000）
  dnsPort: 8553        # DNS 服务端口（默认 8553）
  tcpPort: 6848        # Raw TCP Socket 端口（默认 HTTP-2000）
  host: "0.0.0.0"

mysql:
  host: "127.0.0.1"       # MySQL 服务器地址（默认 127.0.0.1）
  port: 3306             # MySQL 端口（默认 3306）
  username: "root"        # MySQL 用户名
  password: "your_password" # MySQL 密码
  database: "benbroo"     # 数据库名（不存在时自动创建）
  charset: "utf8mb4"     # 字符集（默认 utf8mb4）
  maxIdleConns: 10        # 最大空闲连接数
  maxOpenConns: 100       # 最大连接数
  # 如设置 dsn 则覆盖以上所有字段
  # dsn: "root:password@tcp(127.0.0.1:3306)/benbroo?charset=utf8mb4&parseTime=True&loc=Local"

cluster:
  selfAddr: "127.0.0.1:8848"     # 当前节点地址
  members:                        # 集群所有节点
    - "127.0.0.1:8848"

health:
  checkInterval: 5     # 主动探测间隔（秒）
  failThreshold: 3     # 主动探测连续失败次数后标记不健康
  recoveryThreshold: 3 # 主动探测连续成功后恢复健康
  activeTimeout: 3     # 主动探测超时（秒）
  removeTimeout: 30    # 心跳超时后移除（秒）
  passiveWindow: 60    # 被动探测滑动窗口（秒）
  passiveThreshold: 5  # 被动探测窗口内失败次数阈值
```

> 数据库会自动创建，无需手动建库建表。

### 2.4 启动

```bash
.\build\benbroo.exe
```

启动日志示例：
```
{"msg":"starting Benbroo server","port":8848}
{"msg":"MySQL connected and migrated","host":"127.0.0.1","port":3306,"database":"benbroo","username":"root"}
{"msg":"health checker started","interval":5}
{"msg":"cluster manager started","self":"127.0.0.1:8848"}
{"msg":"cluster syncer started"}
{"msg":"DNS server listening (UDP)","port":8553}
{"msg":"DNS server listening (TCP)","port":8553}
{"msg":"TCP server listening","addr":"0.0.0.0:6848"}
{"msg":"gRPC server listening","addr":"0.0.0.0:9848"}
{"msg":"HTTP server listening","addr":"0.0.0.0:8848"}
```

### 2.5 Web 控制台

浏览器打开 `http://localhost:8848/`，可管理：
- Dashboard：集群状态、服务/配置/实例数量
- 服务列表：搜索、查看实例、健康状态
- 配置管理：搜索、查看/编辑/删除配置
- 命名空间：创建/删除命名空间

### 2.6 通信协议总览

| 协议 | 端口 | 传输层 | 用途 |
|------|------|--------|------|
| HTTP/REST | 8848 | TCP | RESTful API、Web 控制台 |
| gRPC | 9848 | TCP | 高性能 RPC 通信（Protobuf） |
| Raw TCP Socket | 6848 | TCP | 轻量级文本协议，持久连接 |
| DNS | 8553 | UDP + TCP | 基于 DNS 的服务发现 |
| Cluster Sync | 8848 | HTTP | 集群节点间数据同步通知 |

---

## 3. API 接口文档

所有接口返回统一格式：
```json
{ "code": 0, "message": "ok", "data": ... }
```
`code=0` 表示成功，非 0 为失败。

---

### 3.1 服务发现 API（/v1/ns）

#### 注册实例

```
POST /v1/ns/instance
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| ip | 是 | - | 实例 IP |
| port | 是 | - | 实例端口 |
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | DEFAULT_GROUP | 分组名 |
| clusterName | 否 | DEFAULT | 集群名 |
| weight | 否 | 1.0 | 权重（0~100） |
| ephemeral | 否 | true | 是否临时实例 |
| metadata | 否 | {} | 元数据（JSON 字符串） |

**示例：**
```bash
curl -X POST "http://localhost:8848/v1/ns/instance" \
  -d "serviceName=user-service&ip=192.168.1.10&port=8080&weight=1.0"
```

**返回：**
```json
{ "code": 0, "message": "ok" }
```

---

#### 注销实例

```
DELETE /v1/ns/instance
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| ip | 是 | - | 实例 IP |
| port | 是 | - | 实例端口 |
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | DEFAULT_GROUP | 分组名 |
| clusterName | 否 | DEFAULT | 集群名 |

**示例：**
```bash
curl -X DELETE "http://localhost:8848/v1/ns/instance?serviceName=user-service&ip=192.168.1.10&port=8080"
```

---

#### 更新实例

```
PUT /v1/ns/instance
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| ip | 是 | - | 实例 IP |
| port | 是 | - | 实例端口 |
| weight | 否 | 1.0 | 新权重 |
| enabled | 否 | true | 是否启用 |
| metadata | 否 | {} | 新元数据 |

**示例：**
```bash
curl -X PUT "http://localhost:8848/v1/ns/instance" \
  -d "serviceName=user-service&ip=192.168.1.10&port=8080&weight=2.0"
```

---

#### 查询实例列表

```
GET /v1/ns/instance/list
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | DEFAULT_GROUP | 分组名 |
| clusters | 否 | "" | 集群名（逗号分隔） |
| healthyOnly | 否 | false | 只返回健康实例 |

**示例：**
```bash
curl "http://localhost:8848/v1/ns/instance/list?serviceName=user-service&healthyOnly=true"
```

**返回：**
```json
{
  "code": 0,
  "count": 1,
  "hosts": [
    {
      "ip": "192.168.1.10",
      "port": 8080,
      "weight": 1.0,
      "healthy": true,
      "enabled": true,
      "clusterName": "DEFAULT",
      "metadata": "{}"
    }
  ]
}
```

---

#### 查询单个实例

```
GET /v1/ns/instance
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| ip | 是 | - | 实例 IP |
| port | 是 | - | 实例端口 |

---

#### 发送心跳

```
PUT /v1/ns/instance/beat
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| ip | 是 | - | 实例 IP |
| port | 是 | - | 实例端口 |

**示例：**
```bash
curl -X PUT "http://localhost:8848/v1/ns/instance/beat" \
  -d "serviceName=user-service&ip=192.168.1.10&port=8080"
```

---

#### 创建服务

```
POST /v1/ns/service
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | DEFAULT_GROUP | 分组名 |
| protectThreshold | 否 | 0 | 保护阈值（0~1） |
| metadata | 否 | {} | 元数据 |

---

#### 查询服务详情

```
GET /v1/ns/service
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |

**返回：** 包含服务信息和所有实例列表。

---

#### 服务列表（分页）

```
GET /v1/ns/service/list
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | "" | 分组名（空=全部） |
| pageNo | 否 | 1 | 页码 |
| pageSize | 否 | 20 | 每页数量 |

---

#### 集群节点列表

```
GET /v1/ns/serverlist
```

返回所有集群节点及其状态。

---

### 3.2 健康检查 API（/v1/ns/health）

Benbroo 提供两种健康探测机制：

| 类型 | 说明 |
|------|------|
| **主动探测（Active Check）** | 服务端周期性对实例发起 TCP 连接或 HTTP GET 探测 |
| **被动探测（Passive Check）** | 消费端上报调用失败/成功，服务端在滑动窗口内统计失败次数 |

每个服务可通过 `healthCheckType` 配置探测模式：

| 值 | 说明 |
|----|------|
| `NONE` | 不启用健康探测（仅依赖心跳） |
| `ACTIVE` | 仅主动探测 |
| `PASSIVE` | 仅被动探测 |
| `BOTH` | 同时启用主动 + 被动探测 |

---

#### 更新健康检查配置（Provider 侧）

```
PUT /v1/ns/health/config
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | DEFAULT_GROUP | 分组名 |
| healthCheckType | 否 | NONE | 探测类型：NONE / ACTIVE / PASSIVE / BOTH |
| healthCheckProto | 否 | TCP | 主动探测协议：TCP / HTTP |
| healthCheckPath | 否 | / | HTTP 探测路径（如 /actuator/health） |
| healthCheckPort | 否 | 0 | 自定义探测端口（0 = 使用实例端口） |
| activeInterval | 否 | 0 | 主动探测间隔（秒，0 = 使用全局配置） |
| passiveWindow | 否 | 0 | 被动探测窗口（秒，0 = 使用全局配置） |
| passiveThreshold | 否 | 0 | 被动探测阈值（0 = 使用全局配置） |

**示例：**
```bash
curl -X PUT "http://localhost:8848/v1/ns/health/config" \
  -d "serviceName=order-svc&healthCheckType=BOTH&healthCheckProto=TCP&passiveWindow=30&passiveThreshold=3"
```

**返回：**
```json
{ "code": 0, "message": "ok" }
```

---

#### 查询健康状态

```
GET /v1/ns/health/status
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | DEFAULT_GROUP | 分组名 |

**示例：**
```bash
curl "http://localhost:8848/v1/ns/health/status?serviceName=order-svc"
```

**返回：**
```json
{
  "code": 0,
  "data": {
    "healthConfig": {
      "healthCheckType": "BOTH",
      "healthCheckProto": "TCP",
      "healthCheckPath": "/",
      "healthCheckPort": 0,
      "activeInterval": 0,
      "passiveWindow": 30,
      "passiveThreshold": 3
    },
    "instances": [
      {
        "ip": "192.168.1.10",
        "port": 8080,
        "healthy": true,
        "passiveFailures": 0
      }
    ]
  }
}
```

---

#### 消费端上报失败（Consumer 侧被动探测）

```
POST /v1/ns/health/instance/fail
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | DEFAULT_GROUP | 分组名 |
| instanceId | 否* | - | 实例 ID |
| ip | 否* | - | 实例 IP（与 instanceId 二选一） |
| port | 否* | - | 实例端口（与 ip 配合使用） |

> *必须提供 `instanceId` 或 `ip+port` 之一。

**示例：**
```bash
curl -X POST "http://localhost:8848/v1/ns/health/instance/fail" \
  -d "serviceName=order-svc&ip=192.168.1.10&port=8080"
```

**说明：** 每次上报在滑动窗口内记录一次失败。当窗口内失败次数达到 `passiveThreshold` 时，实例标记为不健康。

---

#### 消费端上报成功（Consumer 侧被动探测）

```
POST /v1/ns/health/instance/succeed
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| serviceName | 是 | - | 服务名 |
| namespaceId | 否 | public | 命名空间 |
| groupName | 否 | DEFAULT_GROUP | 分组名 |
| instanceId | 否* | - | 实例 ID |
| ip | 否* | - | 实例 IP（与 instanceId 二选一） |
| port | 否* | - | 实例端口（与 ip 配合使用） |

**示例：**
```bash
curl -X POST "http://localhost:8848/v1/ns/health/instance/succeed" \
  -d "serviceName=order-svc&ip=192.168.1.10&port=8080"
```

**说明：** 上报成功会清除该实例窗口内的失败计数。当实例因被动探测被标记不健康后，需要连续成功达到 `recoveryThreshold` 次才恢复健康。

---

### 3.3 配置管理 API（/v1/cs）

#### 发布配置

```
POST /v1/cs/configs
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| dataId | 是 | - | 配置 ID（如 application.yaml） |
| content | 是 | - | 配置内容 |
| tenant | 否 | public | 命名空间 |
| group | 否 | DEFAULT_GROUP | 分组名 |
| type | 否 | text | 配置类型（text/yaml/json/xml/properties） |

**示例：**
```bash
curl -X POST "http://localhost:8848/v1/cs/configs" \
  -d "dataId=application.yaml&group=DEFAULT_GROUP&content=server.port:8080&type=yaml"
```

---

#### 获取配置

```
GET /v1/cs/configs
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| dataId | 是 | - | 配置 ID |
| tenant | 否 | public | 命名空间 |
| group | 否 | DEFAULT_GROUP | 分组名 |

**返回：**
```json
{
  "code": 0,
  "data": {
    "dataId": "application.yaml",
    "groupName": "DEFAULT_GROUP",
    "content": "server.port:8080",
    "md5": "9208de65e3b27d31045733e3c7fdcf3e",
    "type": "yaml"
  }
}
```

---

#### 删除配置

```
DELETE /v1/cs/configs
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| dataId | 是 | - | 配置 ID |
| tenant | 否 | public | 命名空间 |
| group | 否 | DEFAULT_GROUP | 分组名 |

---

#### 配置列表（分页）

```
GET /v1/cs/configs/list
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| tenant | 否 | public | 命名空间 |
| group | 否 | "" | 分组名 |
| dataId | 否 | "" | 模糊搜索 dataId |
| pageNo | 否 | 1 | 页码 |
| pageSize | 否 | 20 | 每页数量 |

---

#### 配置历史

```
GET /v1/cs/configs/history
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| dataId | 是 | - | 配置 ID |
| tenant | 否 | public | 命名空间 |
| group | 否 | DEFAULT_GROUP | 分组名 |

---

#### 配置变更监听（长轮询）

```
POST /v1/cs/configs/listener
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 说明 |
|------|------|------|
| Listening-Configs | 是 | 监听格式：`dataId\x02group\x02md5\x02tenant\x01` |
| tenant | 否 | 命名空间 |

> 服务端最长等待 30 秒。如有变更立即返回，否则超时返回空。

---

### 3.4 命名空间 API（/v1/console）

#### 列出所有命名空间

```
GET /v1/console/namespaces
```

---

#### 创建命名空间

```
POST /v1/console/namespaces
Content-Type: application/x-www-form-urlencoded
```

| 参数 | 必填 | 说明 |
|------|------|------|
| customNamespaceId | 是 | 命名空间 ID |
| namespaceName | 是 | 显示名称 |
| namespaceDesc | 否 | 描述 |

---

#### 更新命名空间

```
PUT /v1/console/namespaces
```

| 参数 | 必填 | 说明 |
|------|------|------|
| namespace | 是 | 命名空间 ID |
| namespaceShowName | 否 | 新名称 |
| namespaceDesc | 否 | 新描述 |

---

#### 删除命名空间

```
DELETE /v1/console/namespaces?namespaceId=xxx
```

> `public` 命名空间不可删除。

---

### 3.5 Dashboard

```
GET /v1/console/dashboard
```

**返回：**
```json
{
  "code": 0,
  "data": {
    "serviceCount": 1,
    "instanceCount": 2,
    "configCount": 3,
    "clusterNodes": [...],
    "isLeader": true,
    "selfAddr": "127.0.0.1:8848"
  }
}
```

---

### 3.6 gRPC 协议（端口 9848）

Benbroo 支持通过 gRPC 进行高性能 RPC 通信，使用 Protobuf 序列化。

**Proto 定义文件：** `proto/benbroo.proto`

**三大 gRPC 服务：**

| 服务 | 方法 | 说明 |
|------|------|------|
| NamingService | RegisterInstance | 注册实例 |
| | DeregisterInstance | 注销实例 |
| | Heartbeat | 发送心跳 |
| | GetInstances | 查询实例列表 |
| ConfigService | PublishConfig | 发布配置 |
| | GetConfig | 获取配置 |
| | DeleteConfig | 删除配置 |
| HealthService | ReportSuccess | 上报调用成功 |
| | ReportFailure | 上报调用失败 |

**客户端使用：**
```go
c := client.New("http://localhost:8848")
c.ConnectGRPC("localhost:9848")
c.GRPCRegisterInstance(client.RegisterOptions{...})
c.GRPCGetInstances("public", "DEFAULT_GROUP", "my-service", false)
```

---

### 3.7 Raw TCP Socket 协议（端口 6848）

Benbroo 支持通过原生 TCP Socket 进行轻量级通信，采用文本协议，适合低开销场景。

**协议格式：**
```
请求: COMMAND json_payload\n
响应: OK json_payload\n  或  ERR message\n
```

**支持的命令：**

| 命令 | 说明 | 请求示例 |
|------|------|---------|
| PING | 连通性检查 | `PING {}` |
| REGISTER | 注册实例 | `REGISTER {"serviceName":"svc","ip":"10.0.0.1","port":8080}` |
| DEREGISTER | 注销实例 | `DEREGISTER {"serviceName":"svc","ip":"10.0.0.1","port":8080}` |
| HEARTBEAT | 发送心跳 | `HEARTBEAT {"serviceName":"svc","ip":"10.0.0.1","port":8080}` |
| DISCOVER | 查询实例 | `DISCOVER {"serviceName":"svc","healthyOnly":true}` |
| CONFIG_GET | 获取配置 | `CONFIG_GET {"dataId":"app.yaml"}` |
| CONFIG_PUB | 发布配置 | `CONFIG_PUB {"dataId":"app.yaml","content":"...","type":"yaml"}` |
| CONFIG_DEL | 删除配置 | `CONFIG_DEL {"dataId":"app.yaml"}` |
| HEALTH_OK | 上报成功 | `HEALTH_OK {"serviceName":"svc","ip":"10.0.0.1","port":8080}` |
| HEALTH_FAIL | 上报失败 | `HEALTH_FAIL {"serviceName":"svc","ip":"10.0.0.1","port":8080}` |

**客户端使用：**
```go
c := client.New("http://localhost:8848")
c.ConnectTCP("localhost:6848")
c.TCPRegisterInstance(client.RegisterOptions{...})
instances, _ := c.TCPGetInstances("public", "DEFAULT_GROUP", "svc", true)
```

**手动测试（telnet）：**
```
telnet localhost 6848
PING {}
OK {"msg":"pong","ts":1782741881542}
```

---

### 3.8 DNS 服务（端口 8553）

Benbroo 内置 DNS 服务器，支持通过 DNS 查询实现服务发现。

**DNS 名称格式：**
```
<service>.<group>.<namespace>.benbroo.
```

**示例：**
```
order-service.DEFAULT_GROUP.public.benbroo.
```

**支持的记录类型：**

| 类型 | 说明 |
|------|------|
| A | 返回健康实例的 IP 地址列表 |
| SRV | 返回 IP:Port 对，包含权重信息 |

**加权路由：** DNS 响答中的实例顺序按权重排序，并通过轮询旋转，实现加权负载分配。

**查询示例：**
```bash
nslookup -type=A order-service.DEFAULT_GROUP.public.benbroo. 127.0.0.1:8553
```

**返回示例：**
```
Name:    order-service.DEFAULT_GROUP.public.benbroo
Address:  192.168.1.12
Address:  192.168.1.11
Address:  192.168.1.10
```

---

## 4. 项目架构

```
Benbroo/
├── cmd/
│   ├── server/main.go          # 服务器启动入口（HTTP+gRPC+TCP+DNS）
│   └── client/main.go          # 客户端测试程序（10 项功能测试）
├── configs/server.yaml         # 运行配置
├── proto/benbroo.proto         # gRPC Protobuf 定义
├── pkg/
│   ├── model/                  # 领域模型（5 个实体）
│   ├── storage/                # MySQL 数据访问层（GORM）
│   ├── naming/naming.go        # 服务注册与发现
│   ├── config/config.go        # 配置管理 + 长轮询
│   ├── health/                 # 健康检查（主动探测 + 被动探测）
│   │   ├── health.go           # 协调器
│   │   ├── config.go           # 健康检查配置
│   │   ├── active.go           # 主动探测（TCP/HTTP）
│   │   └── passive.go          # 被动探测（滑动窗口计数器）
│   ├── subscribe/eventbus.go   # 内存事件总线（发布/订阅）
│   ├── namespace/namespace.go  # 命名空间管理
│   ├── cluster/
│   │   ├── cluster.go          # 集群管理 + Leader 选举
│   │   └── sync.go             # 集群间数据同步（事件广播）
│   ├── api/server.go           # REST API 路由与处理器
│   ├── grpcserver/             # gRPC 服务端
│   │   ├── server.go           # gRPC 服务实现
│   │   ├── benbroo.pb.go       # Protobuf 生成代码
│   │   └── benbroo_grpc.pb.go  # gRPC 存根代码
│   ├── tcpserver/server.go     # Raw TCP Socket 服务端
│   ├── dns/server.go           # DNS 服务端（A/SRV 记录）
│   └── client/                # 客户端 SDK
│       ├── client.go           # HTTP 客户端 + 负载均衡 + 配置监听
│       ├── grpc.go             # gRPC 客户端方法
│       └── tcp.go              # TCP Socket 客户端方法
└── web/
    ├── console.go              # 静态资源嵌入
    └── static/index.html       # Web 控制台前端
```

### 核心流程

**服务注册流程：**
```
客户端 POST /v1/ns/instance
  → API Handler
    → naming.Service.RegisterInstance()
      → 自动创建 Service（如不存在）
      → 插入/更新 Instance
      → EventBus.PublishServiceChange() 通知订阅者
```

**配置监听流程：**
```
客户端 POST /v1/cs/configs/listener（携带当前 MD5）
  → API Handler
    → config.Service.LongPoll()
      → 每 500ms 检查一次 MD5 变化
      → 有变化立即返回 / 30 秒超时返回空
```

**主动探测流程（Active Check）：**
```
后台 Goroutine（按配置间隔）
  → 加载所有服务的健康配置
  → 筛选 checkType = ACTIVE 或 BOTH 的服务
  → 对每个实例发起 TCP 连接 或 HTTP GET 探测
  → 连续失败 failThreshold 次 → 标记不健康
  → 连续成功 recoveryThreshold 次 → 恢复健康
  → 通过 EventBus 通知订阅者
```

**被动探测流程（Passive Check）：**
```
消费端调用 POST /v1/ns/health/instance/fail 或 /succeed
  → API Handler
    → passiveTracker 记录事件（滑动窗口内）
    → 窗口内失败次数 ≥ passiveThreshold → 标记不健康
    → 上报成功 → 清除失败计数
    → 连续成功 recoveryThreshold 次 → 恢复健康
    → 通过 EventBus 通知订阅者
```

**心跳超时检测：**
```
后台 Goroutine（每 5 秒）
  → 查询所有临时实例
  → 检查心跳超时（> removeTimeout 秒 → 标记不健康/移除）
```

**集群数据同步流程：**
```
节点 A 发生服务/配置变更
  → Syncer.NotifyServiceChange() / NotifyConfigChange()
    → 向所有 UP 状态的 Peer 节点发送 POST /v1/cluster/sync
      → Peer 节点接收事件 → EventBus.Publish*() 通知本地订阅者
```

---

## 5. 客户端 SDK

Benbroo 提供 Go 客户端 SDK（`pkg/client`），支持所有通信协议和核心功能。

### 5.1 创建客户端

```go
import "github.com/benbroo/benbroo/pkg/client"

c := client.New("http://localhost:8848")
```

### 5.2 服务注册与发现

```go
// 注册实例
c.RegisterInstance(client.RegisterOptions{
    ServiceName: "my-service",
    IP:          "192.168.1.100",
    Port:        8080,
    Weight:      1.0,
    Ephemeral:   true,
    Metadata:    `{"version":"v1.0"}`,
})

// 自动心跳（每 5 秒）
c.StartAutoHeartbeat("public", "DEFAULT_GROUP", "my-service", "DEFAULT", "192.168.1.100", 8080)
defer c.StopAutoHeartbeat("public", "DEFAULT_GROUP", "my-service", "DEFAULT", "192.168.1.100", 8080)

// 服务发现 + 负载均衡
inst, _ := c.SelectInstance("public", "DEFAULT_GROUP", "my-service", client.LBRoundRobin)
// inst.IP, inst.Port

// 负载均衡策略
//   client.LBRoundRobin         — 轮询
//   client.LBRandom             — 随机
//   client.LBWeightedRoundRobin — 加权
//   client.LBConsistentHash     — 一致性哈希（需用 SelectInstanceWithKey 传入 hashKey）
```

### 5.3 配置管理

```go
// 发布配置
c.PublishConfig("public", "DEFAULT_GROUP", "app.yaml", "server:\n  port: 8080", "yaml")

// 拉取配置
cfg, _ := c.GetConfig("public", "DEFAULT_GROUP", "app.yaml")
// cfg.Content, cfg.MD5, cfg.Type

// 监听配置变更（长轮询）
cancel := c.WatchConfig("public", "DEFAULT_GROUP", "app.yaml", func(newContent string) {
    fmt.Println("Config changed:", newContent)
})
defer cancel()
```

### 5.4 健康上报

```go
// 消费端上报成功
c.ReportSuccess("public", "DEFAULT_GROUP", "my-service", "192.168.1.100", 8080)

// 消费端上报失败
c.ReportFailure("public", "DEFAULT_GROUP", "my-service", "192.168.1.100", 8080)
```

### 5.5 多协议支持

```go
// gRPC
err := c.ConnectGRPC("localhost:9848")
c.GRPCRegisterInstance(client.RegisterOptions{...})
c.GRPCGetInstances("public", "DEFAULT_GROUP", "svc", false)

// Raw TCP Socket
err := c.ConnectTCP("localhost:6848")
c.TCPRegisterInstance(client.RegisterOptions{...})
c.TCPGetInstances("public", "DEFAULT_GROUP", "svc", false)
c.TCPPing() // -> {"msg":"pong","ts":...}
```

### 5.6 运行测试程序

```bash
go build -o build/benbroo-client.exe ./cmd/client/
./build/benbroo-client.exe
```

测试程序包含 10 个测试项：服务注册、服务发现与负载均衡、心跳机制、配置管理、配置监听、健康上报、gRPC 协议、DNS 服务、TCP Socket 协议、清理。

---

## 6. 集群部署

多节点部署时，修改每个节点的 `configs/server.yaml`：

```yaml
# 节点 1（192.168.1.1）
cluster:
  selfAddr: "192.168.1.1:8848"
  members:
    - "192.168.1.1:8848"
    - "192.168.1.2:8848"
    - "192.168.1.3:8848"

# 节点 2（192.168.1.2）
cluster:
  selfAddr: "192.168.1.2:8848"
  members:
    - "192.168.1.1:8848"
    - "192.168.1.2:8848"
    - "192.168.1.3:8848"
```

集群特性：
- **Leader 选举**：地址最小的存活节点自动成为 Leader
- **健康检查分片**：实例按哈希分配到不同节点检查
- **节点心跳**：每 5 秒上报心跳，15 秒无心跳标记 DOWN
- **数据同步**：服务/配置变更时，通过 HTTP 向 Peer 节点广播事件，Peer 节点接收后通过 EventBus 通知本地订阅者
