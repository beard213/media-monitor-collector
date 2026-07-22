# YouTube Adapter

当前 YouTube 已经从 stub 迁移为真实 adapter，运行模式为 `yt-dlp`。

## 支持能力

- `CONTENT_DETAIL`：单视频元数据。
- `COMMENTS`：评论采集。
- `MEDIA_DOWNLOAD`：媒体下载，默认限制为 360p，可通过 `metadata.max_height` 覆盖。
- `SEARCH_KEYWORD`：关键词搜索，内部转为 `ytsearchdateN:<keyword>`。
- `ACCOUNT_TIMELINE`：频道、handle 或频道 URL 的公开视频列表。
- `PROFILE`：从视频/频道结果中归一化公开账号信息。

## API 示例

只采元数据：

```bash
curl -s -X POST http://127.0.0.1:18120/collections/run \
  -H 'Content-Type: application/json' \
  -d '{
    "platform": "YOUTUBE",
    "capability": "CONTENT_DETAIL",
    "target_type": "CONTENT",
    "target_value": "dQw4w9WgXcQ",
    "source_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "limit": 1,
    "collect_comments": false,
    "download_media": false
  }'
```

采少量评论：

```bash
curl -s -X POST http://127.0.0.1:18120/collections/run \
  -H 'Content-Type: application/json' \
  -d '{
    "platform": "YOUTUBE",
    "capability": "CONTENT_DETAIL",
    "target_type": "CONTENT",
    "target_value": "dQw4w9WgXcQ",
    "source_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "limit": 1,
    "collect_comments": true,
    "download_media": false,
    "metadata": {
      "max_comments": 5
    }
  }'
```

下载低清媒体：

```bash
curl -s -X POST http://127.0.0.1:18120/collections/run \
  -H 'Content-Type: application/json' \
  -d '{
    "platform": "YOUTUBE",
    "capability": "MEDIA_DOWNLOAD",
    "target_type": "CONTENT",
    "target_value": "dQw4w9WgXcQ",
    "source_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "limit": 1,
    "collect_comments": false,
    "download_media": true,
    "metadata": {
      "max_height": 144
    }
  }'
```

## Cookie 配置

公开视频元数据通常可以不配置 cookie。需要授权会话时，把 Netscape cookie 文件放在：

```text
data/storage/private/youtube_cookies/*.txt
```

或在 `.env` 指定：

```env
YOUTUBE_COOKIE_FILE=/absolute/path/to/youtube-cookies.txt
```

adapter 不会在 API 响应或日志中输出 cookie 内容。

也可以不导出 cookies.txt，直接配置已登录浏览器 Profile：

```env
YOUTUBE_BROWSER=chrome
YOUTUBE_BROWSER_PROFILE=/home/ubuntu/media-monitor/data/storage/private/browser_profiles/youtube/youtube_01
```

这种方式适合长期采集：人工登录一次，系统每次采集自动从 profile 读取最新 Cookie。

## 当前边界

当前闭环是同步 API 调用，已经跑通：

```text
API request -> YouTubeAdapter -> yt-dlp -> normalized CollectionBatch -> API response
```

下一步需要接入持久化任务表、账号池租约表、对象存储上传和 worker 异步执行。
