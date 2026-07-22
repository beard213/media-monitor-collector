# 账号池治理方案

## 设计目标

账号池负责“安全使用已授权凭据”，不是负责提高规避能力。它的价值是降低单账号故障影响、控制调用频率、记录审计链路，并为不同平台 adapter 提供统一租约。

## 凭据类型

- `API_TOKEN`：官方 API token、OAuth token。
- `SESSION_COOKIE`：人工授权导出的 Cookie 文件。
- `BROWSER_PROFILE`：隔离浏览器 Profile 路径。
- `CONNECTOR_TOKEN`：外部授权连接器 token。

所有凭据只保存引用，不在日志、任务 payload、API 响应里输出明文。

## 状态机

```text
ACTIVE
  -> COOLDOWN      临时失败、限流、平台短时不可用
  -> NEEDS_REAUTH  登录失效、token 过期
  -> DISABLED      人工停用、风险过高、连续失败超过阈值

COOLDOWN
  -> ACTIVE        冷却到期且健康检查通过
  -> DISABLED      冷却后仍持续失败

NEEDS_REAUTH
  -> ACTIVE        人工更新凭据并验证通过
  -> DISABLED      长期未处理或人工停用
```

## 租约规则

每个采集 job 运行前必须申请租约：

- 维度：`platform + capability + credential_id`。
- 租约有过期时间，worker 崩溃后可以自动回收。
- 同一凭据默认不并发。
- 支持平台级、账号级、能力级预算，例如 `youtube:comments` 与 `youtube:media_download` 分开控制。
- 租约释放时必须上报结果类型，用于更新健康分。

## 错误分类

adapter 必须把异常映射为统一错误码：

- `NETWORK_TEMPORARY`：网络、DNS、超时，可短重试。
- `RATE_LIMITED`：平台或连接器限流，账号进入冷却。
- `AUTH_EXPIRED`：登录失效或 token 过期，账号进入 `NEEDS_REAUTH`。
- `ACCESS_DENIED`：目标不可访问或权限不足，不应频繁重试。
- `PLATFORM_CHANGED`：页面/API 结构变化，需要开发修复。
- `COLLECTOR_BUG`：代码错误。

## 审计字段

每次租约和释放记录：

- `credential_id`
- `platform`
- `capability`
- `collection_job_id`
- `leased_at`
- `released_at`
- `worker_id`
- `result_status`
- `error_code`
- `cooldown_until`

## 账号池表建议

```text
collector_credentials
  id
  platform
  credential_type
  secret_ref
  display_name
  status
  allowed_capabilities
  max_concurrent
  cooldown_until
  last_success_at
  last_failure_at
  failure_count
  metadata_json

credential_leases
  id
  credential_id
  platform
  capability
  collection_job_id
  worker_id
  status
  lease_expires_at
  acquired_at
  released_at
  result_status
  error_code

rate_budgets
  id
  scope_type
  scope_key
  window_seconds
  max_events
  used_events
  reset_at
```
