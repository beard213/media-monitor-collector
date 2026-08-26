# 单机部署规划

## 推荐进程

```text
media-monitor-api       FastAPI 管理接口
media-monitor-worker    采集任务 worker
media-monitor-scheduler 到期目标扫描器
minio                   对象存储，可先用本地目录兼容
mysql/postgresql        结构化数据与任务状态
```

首阶段可以把 API、worker、scheduler 放在同一台服务器，用不同 systemd service 管理。后续任务量上来后再横向扩展 worker。

## 目录规划

```text
/opt/media-monitor
  app/
  docs/
  data/
    storage/
      private/          Cookie、Profile、token 引用文件，禁止对外暴露
      media/            本地对象存储兼容目录
    logs/
```

## 环境变量

```env
APP_NAME=foreign-media-collector
ENVIRONMENT=development
DATABASE_URL=sqlite:///./data/media_monitor.db
STORAGE_ROOT=./data/storage
ADMIN_API_KEY=

ACCOUNT_LEASE_TTL_SECONDS=1800
DEFAULT_COLLECTION_INTERVAL_SECONDS=3600
WORKER_ID=local-worker-1

YOUTUBE_ENABLED=true
TIKTOK_ENABLED=false
FACEBOOK_ENABLED=false
X_ENABLED=false
INSTAGRAM_ENABLED=false
```

## 运维检查

- API `/health` 返回服务状态。
- API `/platforms` 返回平台能力。
- 账号池只暴露脱敏状态，不暴露 secret。
- worker 日志包含 `job_id`、`platform`、`credential_id`、`error_code`。
- 所有凭据文件位于 `data/storage/private`，目录权限收紧。
