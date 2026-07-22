# 境外平台采集系统总体架构

## 目标

在当前服务器上部署一个统一的境外平台采集模块，覆盖 TikTok、YouTube、Facebook、X、Instagram。系统对外表现为一个服务，对内按平台拆分采集实现，保证账号池、任务调度、存储、日志、重试和数据输出都由核心层统一管理。

## 非目标

- 不在核心层硬编码任何平台的反自动化绕过逻辑。
- 不把 Cookie 文件当作唯一采集机制；Cookie 只能作为“已授权浏览器会话”的一种凭据类型。
- 不在首阶段实现全平台真实采集，先完成协议和边界。

## 架构分层

```text
API 层
  创建采集目标、查询任务、查看账号池健康、查看平台能力

调度层
  扫描到期目标 -> 创建 collection job -> 租用账号凭据 -> 调用平台 adapter

账号池层
  凭据存储、租约、速率预算、健康状态、冷却、审计

平台适配器层
  YouTube / TikTok / Facebook / X / Instagram
  实现 discover、collect_content、collect_comments、collect_profile、resolve_media

归一化层
  将平台原始结果转成 Account / Content / Comment / MediaAsset / RunLog

存储层
  结构化数据：MySQL/PostgreSQL
  文件对象：MinIO 或本地兼容目录
  运行日志：数据库 + 文件日志
```

## 核心数据流

```text
monitor_target
  -> scheduler 判断 next_run_at
  -> collection_job(PENDING)
  -> account_pool lease credential
  -> platform_adapter collect
  -> normalize records
  -> object storage upload
  -> persist structured records
  -> update cursor / account health / run stats
```

## 平台能力模型

每个平台声明自己支持的能力，而不是让调度器猜测：

- `SEARCH_KEYWORD`：关键词搜索。
- `ACCOUNT_TIMELINE`：账号主页/时间线采集。
- `CONTENT_DETAIL`：单内容详情。
- `COMMENTS`：评论采集。
- `PROFILE`：公开账号资料。
- `MEDIA_DOWNLOAD`：媒体下载。
- `LIVE_RESOLVE`：直播流或直播间解析。

调度器只会下发平台声明支持的任务。新增平台时，如果只支持 `PROFILE` 和 `ACCOUNT_TIMELINE`，也可以先上线。

## 账号池与稳定性

账号池不直接等于并发池。每次采集前必须通过账号池取得 lease：

- 同一账号同一时间只允许一个租约。
- 平台、账号、任务类型都有独立速率预算。
- 失败按错误类型分类：临时网络错误、授权失效、平台限流、凭据风险、代码错误。
- 账号进入 `COOLDOWN` 或 `DISABLED` 后不会被调度。
- 所有租约、失败、冷却和人工操作写审计日志。

## 低风险约束

核心服务只提供以下采集模式：

- `OFFICIAL_API`：平台官方 API 或正式授权 API。
- `AUTHORIZED_CONNECTOR`：项目方提供的授权连接器。
- `BROWSER_SESSION`：机构自有、明确授权的登录会话，例如人工导出的 Cookie 或隔离浏览器 Profile。
- `PUBLIC_FETCH`：平台允许访问的公开资源。

核心服务不提供验证码绕过、指纹伪装、账号批量注册、封禁规避或突破访问控制的能力。平台 adapter 遇到登录失效、访问受限、风控提示时，应返回明确错误并让账号池降级或停用。

## 与 YouTube 示例的关系

`/home/ubuntu/youtube_collection_system` 里的示例包含可复用经验：

- `monitor_account -> account_scan -> collection_job -> worker` 的任务链路。
- Cookie 文件和浏览器 Profile 的配置方式。
- MinIO/本地文件兼容上传。
- 结构化内容和评论的归一化表。

本仓库不会直接复制整个示例项目。建议迁移路径：

1. 抽取 YouTube 采集调用为 `PlatformAdapter` 实现。
2. 保留原有 Cookie/Profile 配置，但放入账号池凭据。
3. 将 YouTube 结果转成统一 `CollectedContent`、`CollectedComment`、`CollectedAccount`。
4. 由核心服务统一创建 job、更新 cursor、记录运行统计。
