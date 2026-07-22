# Session and Cookie Lifecycle

反复手工导出 cookies.txt 不适合持续采集。生产方案应使用“持久浏览器 Profile + 账号池租约”：

```text
一次人工登录
  -> 独立 browser profile 保存会话
  -> 账号池记录 profile 路径
  -> 采集任务申请账号 lease
  -> adapter 从 profile 自动读取最新 Cookie
  -> 成功则释放 lease
  -> 限流则 COOLDOWN 并切换账号
  -> 登录失效则 NEEDS_REAUTH 并切换账号
```

## YouTube 当前支持

YouTube adapter 已支持两种凭据：

- `SESSION_COOKIE`：Netscape cookies.txt 文件。
- `BROWSER_PROFILE`：Chrome/Chromium/Edge/Firefox profile 目录，由 `yt-dlp` 在采集时读取 Cookie。

优先级：

```text
账号池传入的 SESSION_COOKIE
-> .env 的 YOUTUBE_COOKIE_FILE
-> cookie 目录中的第一个 cookies.txt
-> 账号池传入的 BROWSER_PROFILE
-> .env 的 YOUTUBE_BROWSER_PROFILE
-> public metadata only
```

## 推荐操作方式

每个账号一个独立 profile：

```text
data/storage/private/browser_profiles/youtube/youtube_01
data/storage/private/browser_profiles/youtube/youtube_02
```

人工用该 profile 登录一次 YouTube。之后不要导出 Cookie，系统采集时直接读取 profile。

单账号测试可在 `.env` 配：

```env
YOUTUBE_BROWSER=chrome
YOUTUBE_BROWSER_PROFILE=/home/ubuntu/media-monitor/data/storage/private/browser_profiles/youtube/youtube_01
```

然后重启服务，`GET /platforms` 应显示：

```text
browser_profile=chrome:/home/ubuntu/media-monitor/data/storage/private/browser_profiles/youtube/youtube_01
```

## 失效处理

系统可以自动做：

- 检测 Cookie/Profile 是否能采集。
- 当前账号失败后切换下一个账号。
- `RATE_LIMITED` 进入冷却。
- `AUTH_EXPIRED` 标记 `NEEDS_REAUTH`。
- 记录每个账号的成功率、失败原因和最后使用时间。

系统不应做：

- 自动绕过验证码。
- 自动绕过 2FA。
- 批量注册账号。
- 规避平台封禁或访问控制。

当账号进入 `NEEDS_REAUTH` 时，需要人工重新打开该 profile 完成登录。完成后系统下一轮健康检查会恢复使用。
