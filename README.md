# 境外平台采集系统服务

本仓库是面向 TikTok、YouTube、Facebook、X、Instagram 的统一采集服务架构骨架。当前阶段先固定服务边界、平台适配器协议、账号池治理、调度模型和部署配置，后续各平台开发只需要在既定接口下实现采集能力。

设计原则：

- 稳定：任务调度、账号租约、错误隔离、重试和熔断由核心服务统一处理。
- 清晰：平台差异收敛在 adapter，不把 YouTube 的 Cookie 示例扩散到所有平台。
- 可持续：新增平台只实现标准接口，数据输出统一归一化。
- 低风险：优先官方 API、正式授权连接器和明确授权的账号会话；账号池只做凭据治理、速率预算和审计，不用于规避平台限制。

## 目录

```text
app/
  account_pool/       账号凭据、租约、健康状态和速率预算
  adapters/           平台适配器协议与注册表
  core/               配置
  domain/             统一领域模型
  scheduler/          任务调度契约
docs/
  architecture.md     总体架构
  account-pool.md     账号池治理方案
  platform-adapter.md 平台开发规范
  deployment.md       单机部署规划
```

## 快速检查

```bash
python3 -m compileall -q app
```

## 当前可用 API

```text
GET  /health
GET  /platforms
POST /collections/run
```

YouTube 已经接入真实 `yt-dlp` adapter，使用方式见 [docs/youtube-adapter.md](docs/youtube-adapter.md)。
账号会话长期维护方案见 [docs/session-cookie-lifecycle.md](docs/session-cookie-lifecycle.md)。

## 下一步开发顺序

1. 接入持久化数据库，优先 PostgreSQL 或 MySQL；本地开发可用 SQLite。
2. 实现账号池存储、租约释放、失败降级和平台级速率预算。
3. 把同步 `/collections/run` 扩展为异步 job + worker。
4. 为 TikTok、Facebook、X、Instagram 各自新增 adapter，先从官方 API 或授权连接器开始。
