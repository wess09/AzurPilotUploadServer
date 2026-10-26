# AzurPilot Upload Server — A 节点

Go 1.26 + SQLite 上传入口。独立于统计服务器 C 和 Python 识别节点 B，无需 Redis。

## 启动

复制 `.env.example` 为 `.env`，填写 123 盘开放平台凭据、目录 ID、B 的 WSS 地址与 `WORKER_TOKEN`，运行：

```sh
docker compose pull
docker compose up -d
curl http://localhost:8090/healthz
```

原生运行：先将配置导入进程环境，再执行 `go run .`；程序不自动读取 `.env`。同一 `DATA_DIR` 只运行一个进程。Compose 端口仅绑定本机，通过 HTTPS 反向代理或 Cloudflare Tunnel 公开上传入口。Cloudflare 免费套餐单次请求上限为 100 MB；走 Tunnel 时使用下述按文件上传协议，每个请求最多 10 MiB，整组仍支持 500 MiB。保留的整组 multipart 接口需要代理允许至少 502 MiB 请求体并关闭请求体缓存。

镜像地址为 `ghcr.io/wess09/azurpilotuploadserver:latest`，提供 Linux amd64/arm64。`main` 是主分支标签，`sha-<完整提交号>` 可用于固定版本；推送 `v*` 标签发布版本镜像。主分支和版本标签经测试后自动推送 GHCR，PR 只构建。需要本地构建时使用 `docker compose up -d --build`，可通过 `UPLOAD_IMAGE` 指定镜像版本。

Linux 服务器也可在镜像已拉取或导入后执行 `sh deploy/start.sh`；脚本会先检查必需配置，并避免在服务器上编译或再次访问镜像仓库。`.env` 建议设为仅管理员可读（`chmod 600 .env`），未填 123 盘和 B 的连接配置时不会启动接收。

## 资源与存储

默认 30 路接收、2 路向云盘上传、1 GiB 图片暂存配额、每文件 10 MiB、每组 500 MiB，64 KiB 拷贝缓冲。`.env.example` 可调这些限制；B/C 的协议校验上限需同步调整。`BATCH_SIZE` 应为 1–30，生产保持 30。

接收前逐文件预留空间；已写入部分也被该预留覆盖。空间不足返回 `503`，并发耗尽返回 `429`，均带 `Retry-After`。上传读取空闲超过 30 秒会中断。客户端保持原 `upload_id`、元数据及文件顺序重传整组，已收到文件会验证哈希后复用。完整文件立即进入上传处理，不等整组完成。

123 盘明确返回 `completed=true` 与文件 ID 后才删除本地原图。上传失败保留磁盘文件并重试。SQLite WAL 和少量元数据不计入 1 GiB **图片配额**，须额外预留磁盘；默认还保留 256 MiB 磁盘余量。重启清理无登记的临时图片，恢复上传和未确认通知。

## 上传协议

### 按文件上传（Cloudflare Tunnel 推荐）

1. `POST /api/v1/uploads/init`：JSON 请求体为下方完整元数据，登记整组文件大小。返回 `202`；只登记，不预占整组磁盘。
2. `PUT /api/v1/uploads/{upload_id}/files/{index}`：请求体为单张 PNG 原始字节，`index` 从 0 开始，大小须与 `file_sizes` 对应项相同。完整落盘立即进入云盘上传队列。
3. `POST /api/v1/uploads/{upload_id}/complete`：所有文件接收完成后确认整组，返回 `202` 和组哈希。缺文件返回 `409`，不会提前加入 30 份批次。

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

恰好 30 份唯一组全部上传完成后，A 持久化 `batch_ready` 并通过 WebSocket 推送 B。B 落库后回 `batch_accepted`；丢失确认时沿用同一批次 ID 补发。不满 30 份继续等待，没有定时强制派发。

## 验证与参考

```sh
go test -count=1 ./...
go test -race -count=1 ./... # Windows 需要 C 编译器，或在 Linux Go 容器中运行
go vet ./...
```

资源测试同时流入 30 × 10 MiB、检查两路上传及堆增量；另覆盖配额、断流、重启、确认丢失、重复组和 29/30 边界。

可选真实 A→B 契约测试：设置 `WORKER_TEST_WS_URL` 与 `WORKER_TEST_TOKEN`，运行 `go test -run TestRealWorkerWebSocketContract -v .`。目标必须是使用 `create_app(config, start_engine=False)` 启动的专用 B 测试实例；该测试发送 30 份模拟云盘记录，确认真实 WebSocket 与 B 的持久化回执，不用于生产。

123 盘使用官方 [上传域名接口](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/agn8lolktbqie7p9) 与 [单步上传接口](https://123yunpan.yuque.com/org-wiki-123yunpan-muaork/cr6ced/xhiht1uh3yp92pzc)。长期原图留在 123 盘，A 不主动删除云盘原图。
