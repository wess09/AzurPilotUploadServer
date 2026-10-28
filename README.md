# AzurPilot Upload Server — A 节点

Go 1.26 + SQLite 上传入口。独立于统计服务器 C 和 Python 识别节点 B，无需 Redis。

## 启动

复制 `.env.example` 为 `.env`，填写 123 盘开放平台凭据、目录 ID 与 `WORKER_TOKEN`，默认使用 B 主动连接的 `WORKER_MODE=reverse`，运行：

```sh
docker compose pull
docker compose up -d
curl http://localhost:8090/healthz
```

原生运行：先将配置导入进程环境，再执行 `go run .`；程序不自动读取 `.env`。同一 `DATA_DIR` 只运行一个进程。Compose 端口仅绑定本机，通过 HTTPS 反向代理或 Cloudflare Tunnel 公开上传入口。Cloudflare 免费套餐单次请求上限为 100 MB；走 Tunnel 时使用下述按文件上传协议，每个请求最多 10 MiB，整组仍支持 500 MiB。保留的整组 multipart 接口需要代理允许至少 502 MiB 请求体并关闭请求体缓存。

镜像地址为 `ghcr.io/wess09/azurpilotuploadserver:latest`，提供 Linux amd64/arm64。`main` 是主分支标签，`sha-<完整提交号>` 可用于固定版本；推送 `v*` 标签发布版本镜像。主分支和版本标签经测试后自动推送 GHCR，PR 只构建。需要本地构建时使用 `docker compose up -d --build`，可通过 `UPLOAD_IMAGE` 指定镜像版本。

Linux 服务器也可在镜像已拉取或导入后执行 `sh deploy/start.sh`；脚本先检查必需配置，避免在服务器上编译或再次访问镜像仓库。`.env` 设为仅管理员可读（`chmod 600 .env`），未填 123 盘和工作节点令牌时不会启动接收。

## 资源与存储

默认 30 路接收、2 路向云盘上传、1 GiB 图片暂存配额、每文件 10 MiB、每组 500 MiB，64 KiB 拷贝缓冲。`.env.example` 可调这些限制；B/C 的协议校验上限需同步调整。`BATCH_INTERVAL=30m` 按每小时的 :00/:30 派发；`BATCH_SIZE=30` 仅限制单条传输消息容量，不是触发门槛。

接收前逐文件预留空间；已写入部分也被该预留覆盖。空间不足返回 `503`，并发耗尽返回 `429`，均带 `Retry-After`。上传读取空闲超过 30 秒会中断。客户端保持原 `upload_id`、元数据及文件顺序重传整组，已收到文件会验证哈希后复用。完整文件立即进入上传处理，不等整组完成。

123 盘明确返回 `completed=true` 与文件 ID 后才删除本地原图。上传失败保留磁盘文件并重试。SQLite WAL 和少量元数据不计入 1 GiB **图片配额**，须额外预留磁盘；默认还保留 256 MiB 磁盘余量。重启清理无登记的临时图片，恢复上传和未确认通知。

## 运行日志

在 A 的部署目录执行 `docker compose logs -f --tail 100 uploader`。默认 INFO 日志覆盖关键状态：

- `upload initialized`、`file received`、`upload group received`：登记、单文件落盘、整组接收完成；`duplicate` 表示是否复用已有图片组。
- `upload retry` → `cloud upload completed` → `local file cleared`：失败保留文件、10 秒后重试；云盘成功记录 `remote_id`，本地清理后记录剩余暂存用量。
- `batch ready` → `batch sent` → `batch accepted`：半小时批次生成、发给 B、B 持久化确认；确认接收不代表识别已经结束。
- `uploader status`：启动时及每分钟输出待上传/上传中文件数、待确认批次、暂存已用/预留、B 连接状态和下次调度时间。重复组等待清理的文件不算上传积压。

按 `upload_id`、`file` 或 `batch_id` 关联记录。日志不输出密钥、提交人、原图内容或每个传输缓冲；健康探测不逐次记录。默认镜像的日志时间及 `next_dispatch_at` 使用 UTC，北京时间需加 8 小时。Compose 对日志轮转，最多保留 3 × 10 MiB。

### 可视化监控大屏

打开 A 的 `/dashboard/`（根路径也会跳转），可查看新增接收、重复传输、A 转存和 v2 直传完成的实时卡片，以及图片/秒、MiB/秒两组折线图。支持 15 分钟 / 1 小时时间范围、图例筛选、悬停数值、暂停刷新、全屏和手机访问。页面内嵌于 Go 镜像，不依赖外部 CDN 或前端服务。

暂存面板分别显示已落盘与预留空间；队列显示待转存、转存中、直传待确认、重试文件及待回执批次。v1 接收数和 v2 控制请求数独立显示，界面不暴露提交人、原图、文件 ID 或凭据。

`GET /api/v1/monitor` 提供只读数据，每 5 秒刷新；`?after=<Unix秒>` 增量获取历史。趋势使用固定 720 个 5 秒时间桶，保留本次进程最近 1 小时，重启清零。所有访问者共用 15 秒队列摘要缓存，监控不占上传名额，也不保存原图或逐次写入 SQLite。

### 图片上传 / 下载速率

`GET /healthz` 的 `transfers` 返回最近 10 秒、60 秒的成功图片吞吐及本次进程启动以来的累计数：

- `download`：客户端 → A，完整接收并登记的单张图片；已存在文件的重传通过哈希核验后也计一次实际接收。
- `upload`：A → 123 盘，云盘确认完成且状态保存成功的单张图片；失败重试不增加成功数。
- `received` / `retransmit`：分别为完整新文件接收与已登记文件重传；两者相加为原有 `download`，用于区分新增量和重传带宽。
- `images_per_second` / `images_per_second_60s`：10 秒 / 60 秒平均张数每秒。
- `bytes_per_second` / `bytes_per_second_60s`：对应成功图片大小每秒，不含协议开销或失败传输的带宽。
- `total.images / total.bytes`：从 `since` 开始累计，重启清零；空闲超过窗口后速率自动回零。

按文件计数，多图上报不合并为一张；重复接收的网络吞吐与组哈希去重后的正式统计数量是不同指标。B 从云盘下载原图不经过 A，不包含在 A 的下载计数中。速率使用固定 60 个秒级桶，图表另用固定 720 个 5 秒桶；图片仍按 64 KiB 流式落盘，不增加原图缓存或数据库写入。

每 10 秒自动输出 `transfer rates` 日志，包含两个方向的张/秒、60 秒均速、MiB/秒和累计图片数。例：

```text
INFO transfer rates window_seconds=10 download_images_per_second=3.2 upload_images_per_second=1.7 download_images_per_second_60s=2.5 upload_images_per_second_60s=1.6 download_mib_per_second=0.8 upload_mib_per_second=0.4 download_images_total=192 upload_images_total=102
```

## 上传协议

### v2 客户端直传（新 AP 默认）

图片正文由客户端直接 PUT 到 123 盘的 HTTPS 预签名地址，A 只接收控制 JSON，不占用图片暂存配额。现有 v1 与 `/api/upload` 保留原样。

1. `POST /api/v2/uploads/init`：请求 `{"schema_version":2,"metadata":{...},"files":[{"md5":"32位小写哈希","sha256":"64位小写哈希"}]}`。`metadata` 使用下方现有元数据格式，`files` 按 `file_sizes` 的顺序排列。单文件 10 MiB、整组 500 MiB 的校验仍然有效；JSON 上限 256 KiB。
2. `POST /api/v2/uploads/{id}/files/{index}/prepare`，空请求体：已验证文件返回 `completed:true`；否则返回 `parts`，每项包含 `number / offset / size / url`。客户端严格按该范围流式 PUT 到 `url`，不携带 A 或网盘账户的认证头，不跟随上传重定向。地址过期后重新 prepare；预上传任务持久化，可跨 A 重启恢复。
3. `POST /api/v2/uploads/{id}/files/{index}/complete`，空请求体：A 自行向 123 盘请求完成/轮询，取得云端文件 ID，登记云盘上传完成。A 不再额外请求文件详情校验，也不接受客户端传入文件 ID。`completed:false` 时至少间隔一秒重试。
4. 全部文件云端完成后 `POST /api/v2/uploads/{id}/complete`，返回组哈希与 `duplicate`，按原来的半小时边界派发 B。B 下载时逐块计算 **SHA-256 并核对客户端声明的文件大小**，只有完全一致才识别并将结果提交 C。不一致时清理临时文件、记录错误重试，不计入统计。采集时间不变。

本系统的 v2 路由使用 123 盘官方 **V1 预签名上传 API**；这两个版本号互不关联。Client Secret、访问令牌和云端预上传 ID 留在 A，只把限定分片的预签名 URL 发给客户端。MD5 用于云盘创建上传，SHA-256 用于组标识、不可变文件名和 B 的实际字节校验。v2 元数据请求不占 v1 的 30 路接收名额，也不另设应用并发上限；仍有单任务互斥、请求大小限制和云端请求超时。

SQLite 自动为旧 `uploads` 添加 `protocol=1`，新 v2 任务使用 `direct_files` 保存上传进度；无需清空数据库。新 AP 的磁盘队列记录 `upload_version=2`，旧队列缺少该字段时继续使用 v1，不把已有任务切换到另一协议。

`/healthz` 的 `transfers.direct_upload` 与 `direct upload rates` 日志显示**云盘已确认完成**的张/秒及累计数，包含云盘内容复用；`direct_upload_stage=cloud_completed` 明确该阶段。这不是 B 校验或正式统计的完成数。`direct_control_active` 显示正在处理的 v2 请求数，原有 `download / upload` 仍表示图片经过 A 的传输。

### 按文件上传（Cloudflare Tunnel 推荐）

1. `POST /api/v1/uploads/init`：JSON 请求体为下方完整元数据，登记整组文件大小。返回 `202`；只登记，不预占整组磁盘。
2. `PUT /api/v1/uploads/{upload_id}/files/{index}`：请求体为单张 PNG 原始字节，`index` 从 0 开始，大小须与 `file_sizes` 对应项相同。完整落盘立即进入云盘上传队列。
3. `POST /api/v1/uploads/{upload_id}/complete`：所有文件接收完成后确认整组，返回 `202` 和组哈希。缺文件返回 `409`，不会提前加入待派发样本。

```sh
curl --fail-with-body -H 'Content-Type: application/json' --data-binary @metadata.json https://upload.example.com/api/v1/uploads/init
curl --fail-with-body -X PUT --data-binary @image.png https://upload.example.com/api/v1/uploads/303d1f40-3280-4053-84dd-574a83f59d20/files/0
curl --fail-with-body -X POST https://upload.example.com/api/v1/uploads/303d1f40-3280-4053-84dd-574a83f59d20/complete
```

同组文件顺序发送；不同组可并发。断线或重启后保留原元数据、上传 ID 和索引重试，已接收文件即使已清理本地副本也会按哈希核验并复用。重复确认不会重复派发；相同 ID 的元数据或文件变化返回 `409`。与整组接口共用限流、配额、云盘任务和组哈希去重。

### 整组上传（兼容接口）

`POST /api/v1/uploads`，`multipart/form-data`，第一项必须叫 `metadata`，内容为以下 JSON；后续每项为 PNG 文件，顺序与 `file_sizes` 一致：

```json
{"upload_id":"303d1f40-3280-4053-84dd-574a83f59d20","device_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","instance_id":"bbbbbbbbbbbbbbbb","project":"research","scene":"research_complete","server":"cn","collected_at":"2026-10-26T12:00:00+08:00","file_sizes":[12345]}
```

`project` 为 `research` 或 `drop`；科研刷新用 `research_refresh`。成功返回 `202` 和 `sha256`、`duplicate`。大小超限 `413`，无效或中断请求 `400`，同一上传 ID 内容冲突 `409`。

图片组 SHA-256 = 按上传顺序连接各文件 SHA-256 的 **32 字节二进制摘要**后再计算 SHA-256。相同组哈希只统计第一次登记的提交信息。重复组不增加可派发计数；部分重复文件可能已传到云盘，但不会重复统计。

## 连接方向与半小时调度

B 主动连接 A 的 `/internal/v1/worker/connect`，携带 `Authorization: Bearer <WORKER_TOKEN>`，A 通过同一 WebSocket 推送批次。每个 A 同时只接受一个 B；无效令牌返回 401，重复连接返回 409。公网必须使用 WSS；也可通过 SSH 本地转发连接 `ws://127.0.0.1:18092/internal/v1/worker/connect`，避免在 HTTP 公网发送令牌。原有 A 连接 B 的方式可显式设置 `WORKER_MODE=forward` 和 `WORKER_WS_URL`。

每半小时将已完整上传到云盘的唯一组登记为固定待发队列；1 份也发送，空时段不发送。超过 30 份拆为多条消息并依次处理，末条允许不足 30 份，同一时段不会遗漏余数。新增样本等待下一边界，不会因数量满 30 提前触发。

队列、下次调度时间和通知持久化到 SQLite。B 落库后回 `batch_accepted`；确认丢失或重启时，原批次 ID 补发。`window_start/window_end` 是派发时段，统计必须依据每组原始 `collected_at`，不能用派发、识别或重试时间代替。模板重识别不受此调度约束。

## 验证与参考

```sh
go test -count=1 ./...
go test -race -count=1 ./... # Windows 需要 C 编译器，或在 Linux Go 容器中运行
go vet ./...
```

资源测试同时流入 30 × 10 MiB、检查两路上传及堆增量；另覆盖配额、断流、重启、确认丢失、反向鉴权、重复组、半小时边界和 61 份样本拆批恢复。

可选真实 A→B 契约测试：设置 `WORKER_TEST_WS_URL` 与 `WORKER_TEST_TOKEN`，运行 `go test -run TestRealWorkerWebSocketContract -v .`。目标必须是使用 `create_app(config, start_engine=False)` 启动的专用 B 测试实例；该测试发送 30 份模拟云盘记录，确认真实 WebSocket 与 B 的持久化回执，不用于生产。

123 盘使用官方 [上传域名接口](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/agn8lolktbqie7p9) 与 [单步上传接口](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/xhiht1uh3yp92pzc)。长期原图留在 123 盘，A 不主动删除云盘原图。
## 历史客户端接口

`POST /api/upload` 和 `/api/upload/` 接收单个 multipart `file`，支持旧 AP 的 `device_id/genre/combat_count`，以及 `User-Agent: Alas (<id>)` 的单文件上传。成功 HTTP 200，兼容 Lsky/Imgurl 回执。原 PNG 文件名中的 13 位毫秒时间戳作为采集时间；显式 `collected_at` 优先。无场景信息的截图由 B 分类，不确定时进入复核。

`STATISTICS_PUBLIC_URL` 设置 C 公网地址（例如 `https://stats.azurpilot.com`），历史 JSON 和查询接口以 307 转交 C。图片仍在 A 流式落盘，每请求只预留单文件上限，上传 123 盘完成后删除本地副本。调度仍按 :00/:30，`BATCH_SIZE` 不是触发份数。
