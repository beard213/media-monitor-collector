# 平台适配器开发规范

## 适配器职责

平台 adapter 只负责平台差异：

- 解析目标。
- 调用官方 API、授权连接器或已授权浏览器会话。
- 返回统一领域模型。
- 将平台错误映射为统一错误码。

adapter 不负责：

- 创建任务。
- 选择账号。
- 存储数据。
- 上传文件。
- 更新任务状态。
- 输出下游数据库。

## 必须实现的接口

```python
class PlatformAdapter(Protocol):
    platform: Platform
    capabilities: set[Capability]

    def healthcheck(self) -> AdapterHealth: ...
    def discover(self, request: CollectionRequest, credential: CredentialLease | None) -> CollectionBatch: ...
    def collect_comments(self, request: CollectionRequest, credential: CredentialLease | None) -> CollectionBatch: ...
    def collect_profile(self, request: CollectionRequest, credential: CredentialLease | None) -> CollectionBatch: ...
    def resolve_media(self, request: CollectionRequest, credential: CredentialLease | None) -> CollectionBatch: ...
```

## 新增平台步骤

1. 在 `app/domain/enums.py` 添加平台枚举值。
2. 新增 `app/adapters/<platform>.py`。
3. 在 `app/adapters/registry.py` 注册 adapter。
4. 补充平台能力声明。
5. 添加 adapter 契约测试。
6. 接入账号池所需凭据类型。
7. 写部署配置和运维检查项。

## 输出要求

每条内容必须至少包含：

- `platform`
- `external_id`
- `url`
- `content_type`
- `author_external_id`
- `collected_at`
- `raw`

评论必须至少包含：

- `platform`
- `external_id`
- `content_external_id`
- `author_external_id`
- `text`
- `collected_at`
- `raw`

不要丢弃平台原始 payload，先放入 `raw`，再逐步补充强类型字段。

## 错误处理

adapter 不应吞掉异常。平台相关异常要转成 `CollectorError`，并带上：

- `error_code`
- `retryable`
- `credential_action`
- `message`

`credential_action` 可选值：

- `NONE`
- `COOLDOWN`
- `REAUTH`
- `DISABLE`

## YouTube 迁移建议

现有 `/home/ubuntu/youtube_collection_system` 的 YouTube 代码应拆成三层：

- downloader/browser collector：保留底层执行脚本。
- YouTube adapter：包装脚本调用并返回统一模型。
- core job runner：由本仓库统一调度账号、任务和存储。
