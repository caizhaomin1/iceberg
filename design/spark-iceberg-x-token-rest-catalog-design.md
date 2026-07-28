# Spark + Iceberg 定制 REST Catalog 设计

## 1. 目标

在 Apache Iceberg REST Catalog 客户端中提供两项可独立启用的兼容能力：

1. 根据 Spark Catalog 配置的用户名和密码调用登录接口，并在所有 Iceberg REST
   Catalog 请求中增加 `X-Token` Header。
2. 将 namespace 路径从 `{namespace}` 适配为
   `{catalog-name}0x1F{namespace}`。其中 `0x1F` 是六个 ASCII 字面字符，不是
   `U+001F`，也不是 URL 编码后的 `%1F`。

两个特性默认关闭。关闭后认证管理器、REST 路径以及网络请求行为与开源 Iceberg
保持一致。

## 2. 非目标

- 不改变 Iceberg REST Catalog OpenAPI 协议。
- 不改变表、视图、namespace 或 transaction 请求体。
- 不在 Spark 模块引入定制逻辑；Spark Catalog 配置会作为普通 Iceberg Catalog
  properties 传递给 core 模块。
- 不修改 OAuth2、Basic、SigV4 或 Google 等已有认证行为。
- 不对服务端提前撤销但尚未到期的 Token 自动进行 401 重放。客户端通过响应中的
  `expires` 提前刷新；401 保持 Iceberg 原生错误处理语义，避免对非幂等提交请求做
  未经协议保证的自动重放。

## 3. 配置

所有配置均位于 Spark Catalog 配置空间下。

| Iceberg property | Spark 配置后缀 | 默认值 | 说明 |
|---|---|---:|---|
| `rest.x-token-auth-enabled` | `rest.x-token-auth-enabled` | `false` | 启用 X-Token 认证 |
| `rest.x-token.username` | `rest.x-token.username` | 无 | 登录用户名 |
| `rest.x-token.password` | `rest.x-token.password` | 无 | 登录密码 |
| `rest.x-token.user-type` | `rest.x-token.user-type` | `human` | `human` 或 `machine` |
| `rest.x-token.uri` | `rest.x-token.uri` | 按用户类型推导 | 显式覆盖 Token URL |
| `rest.x-token.refresh-before-ms` | `rest.x-token.refresh-before-ms` | `60000` | 提前刷新时间 |
| `rest.catalog-namespace-prefix-enabled` | 同名 | `false` | 启用 URL namespace 前缀 |
| `rest.catalog-namespace-prefix` | 同名 | Spark Catalog 名 | 覆盖 URL 中的 Catalog 名 |

示例：

```bash
spark-submit \
  --conf spark.sql.catalog.prod=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.prod.type=rest \
  --conf spark.sql.catalog.prod.uri=https://catalog.example.com \
  --conf spark.sql.catalog.prod.rest.x-token-auth-enabled=true \
  --conf spark.sql.catalog.prod.rest.x-token.username=alice \
  --conf spark.sql.catalog.prod.rest.x-token.password=secret \
  --conf spark.sql.catalog.prod.rest.x-token.user-type=human \
  --conf spark.sql.catalog.prod.rest.catalog-namespace-prefix-enabled=true
```

对于 Spark 表标识：

```text
prod.sales.orders
```

Catalog 请求路径为：

```text
/v1/namespaces/prod0x1Fsales/tables/orders
```

如果 namespace 为多级 namespace `region.sales`，Iceberg 原有 namespace
分隔符仍独立生效：

```text
/v1/namespaces/prod0x1Fregion%1Fsales/tables/orders
```

这里：

- 第一个 `0x1F` 是定制服务要求的六个字面字符；
- 第二个 `%1F` 是 Iceberg 默认的多级 namespace 分隔符。

## 4. Token 协议

### 4.1 Human 用户

默认 URL：

```text
PUT {catalog-uri}/rest/usermgmt/v1/users/sessions
```

请求体：

```json
{
  "user_name": "alice",
  "value": "password"
}
```

响应：

```json
{
  "accessSession": "token-value",
  "expires": 300000
}
```

### 4.2 Machine 用户

默认 URL：

```text
PUT {catalog-uri}/rest/plat/smapp/v1/sessions
```

请求体：

```json
{
  "grantType": "password",
  "userName": "machine-user",
  "value": "password"
}
```

响应：

```json
{
  "session-id": "token-value",
  "expires": 300000
}
```

`expires` 按相对当前时间的毫秒数解释。缺失或非正数表示服务端没有提供可用的
过期时间，此时 Token 缓存到 Catalog 关闭。

如果 Token 服务与 Catalog 服务不在同一地址，可设置 `rest.x-token.uri` 为完整
绝对 URL。

## 5. 组件设计

### 5.1 AuthManagers

`AuthManagers.loadAuthManager` 只在 `rest.x-token-auth-enabled=true` 时创建
`XTokenAuthManager`。开关关闭时继续执行开源 Iceberg 原有分支。

为避免认证语义冲突，启用 X-Token 时不能同时配置 `rest.auth.type`。

### 5.2 XTokenAuthManager

`XTokenAuthManager` 使用 Iceberg 原生 `AuthManager` 生命周期：

- `initSession` 创建短生命周期 Session，用于认证 `/v1/config`。
- `catalogSession` 创建 Catalog 生命周期 Session，用于后续所有 REST 请求。
- 每个 Session 拥有独立 TokenProvider，不使用 JVM 全局单例，不会在多个 Spark
  Catalog、Server 或用户之间串用 Token。

### 5.3 XTokenAuthSession

`XTokenAuthSession.authenticate(HTTPRequest)` 在请求发送前：

1. 读取当前有效 Token。
2. 删除请求中可能存在的旧 `X-Token`。
3. 增加唯一的 `X-Token` Header。
4. 返回不可变的新 `HTTPRequest`。

Token 获取使用独立 Apache HTTP Client，不通过 Iceberg Catalog RESTClient，
因此登录接口不会递归要求 `X-Token`。

### 5.4 XTokenProvider

Provider 使用 `AtomicReference<Token>` 缓存 Token，并通过实例级同步实现
single-flight 刷新：

```text
请求线程
  ├─ Token 未过期：直接读取缓存
  └─ Token 缺失/即将过期
       └─ synchronized
            ├─ 二次检查缓存
            └─ 仅一个线程调用 Token 接口
```

Catalog 关闭时清除 Token 引用并关闭登录 HTTP Client。

### 5.5 ResourcePaths

URL 适配位于 `ResourcePaths.pathEncode`，而不是通用 HTTP 字符串拦截器。
这样只影响结构上确实包含 namespace 的路径：

- namespace；
- namespace properties；
- table、register、metrics、remote sign；
- view、register view；
- server-side scan planning。

以下路由保持原样：

- `/v1/config`；
- namespace 列表根路径；
- table/view rename；
- transaction commit；
- OAuth 路由。

Catalog 前缀先按一个路径元素进行 URL 编码，再连接六字符 `0x1F` 和 Iceberg
原有 encoded namespace。

开关值只从客户端初始 properties 读取，服务端 `/v1/config` 不能远程启用该
定制能力。启用后，服务端仍可通过 `/v1/config` 提供原生 Iceberg
`namespace-separator` 等配置。

## 6. 兼容性

### 6.1 默认关闭

```properties
rest.x-token-auth-enabled=false
rest.catalog-namespace-prefix-enabled=false
```

此时：

- `AuthManagers` 继续创建原生 Noop/OAuth2/Basic/SigV4/Google AuthManager；
- `ResourcePaths.forCatalogProperties(Map)` 的输出不变；
- 不创建 Token HTTP Client；
- 不访问登录接口；
- 不增加 `X-Token`；
- 不增加 Catalog namespace 前缀。

### 6.2 两项能力独立

可只启用认证、只启用 URL 适配，也可同时启用。用户名、密码等配置仅在认证开关
启用时校验。

## 7. 安全

- Token 与密码不写日志。
- 非 200 登录响应不输出响应体，避免服务端错误载荷泄露凭据。
- Token Header 使用不可变请求副本，刷新后替换旧值。
- 每个 Catalog 单独缓存 Token。
- 生产环境必须使用 HTTPS。
- Spark 应配置敏感参数脱敏，例如：

```properties
spark.redaction.regex=(?i)secret|password|token|rest\.x-token
```

更安全的部署方式是从 Kubernetes Secret、Hadoop Credential Provider 或其他
Secret Manager 注入密码，而不是在命令行直接传递明文。

## 8. 测试

核心测试位于：

```text
core/src/test/java/org/apache/iceberg/rest/auth/TestXTokenAuth.java
core/src/test/java/org/apache/iceberg/rest/TestResourcePaths.java
```

覆盖：

- 开关关闭时使用原生 NoopAuthManager；
- 开关启用时加载 XTokenAuthManager；
- 与其他 `rest.auth.type` 冲突检查；
- human 请求 URL、请求体和 `accessSession` 响应；
- machine 请求 URL、请求体和 `session-id` 响应；
- Token 缓存与提前刷新；
- 替换已有 `X-Token` Header；
- URL 开关关闭时路径不变；
- 字面量 `0x1F`；
- 多级 namespace；
- 自定义 Catalog 前缀；
- 真实 RESTCatalog 初始化、`/v1/config` Header 和 listTables 路径的端到端验证。

验证命令：

```bash
./gradlew :iceberg-core:test \
  --tests org.apache.iceberg.rest.auth.TestXTokenAuth \
  --tests org.apache.iceberg.rest.TestResourcePaths

./gradlew spotlessCheck
```

## 9. 代码迁移

原 `org.apache.iceberg.rest.auth.token` 目录中的临时代码已删除。主要原因：

- 使用 JVM 全局 TokenManager，多个 Catalog 之间可能串用 Token；
- 缓存键只有 username，未包含 Server 和用户类型；
- 使用 Jackson 注解，不符合当前仓库序列化规范；
- Token 过期时间存在毫秒/秒混用；
- 依赖底层 Apache HTTP ExecChain 拦截器，扩展面大；
- 缺少 `UserType` 定义，当前代码不能独立编译。

新实现使用 Iceberg 原生 AuthManager/AuthSession 生命周期，并将 URL 适配限制在
ResourcePaths。
