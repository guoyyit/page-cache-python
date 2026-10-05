# page-cache-python

本地页缓存与延迟回写服务。位于上层读写调用与底层块存储之间，以固定大小的页为单位缓存数据，写操作先更新缓存、后异步落盘，并通过 HTTP 接口暴露按页读写、刷盘与统计查询能力。全部能力在本地进程内完成，不依赖外部服务。

## 架构

- `pagecache/cache.py` — 核心缓存：读写、脏页水位与节流、LRU 淘汰、后台回写线程、统计。
- `pagecache/storage.py` — 底层块存储。每页一个带 CRC32 校验的文件，写入采用临时文件 + `fsync` + 原子 `rename`，不会产生撕裂页。
- `pagecache/journal.py` — 回写预写日志。回写前记录意图（含完整页数据），落盘成功后记录提交标记；崩溃后重放未提交的意图，前滚到最后已确认状态。
- `pagecache/service.py` — FastAPI 接口层，页数据以 base64 编码传输。
- `main.py` — 服务装配入口，数据默认落在 `./data/`。

## 读写语义

- 读未命中时从底层存储载入并缓存；读命中返回缓存内容，命中/未命中计数准确。
- 写操作更新缓存后立即可见，后续读取必然返回最后一次已确认写入的内容。
- 同一页的多次写入按序落盘；写与回写并发交错时，落盘结果永远是某一次完整写入，不会撕裂。
- 从未写过的页读出为全零。

## 持久化保证

- `flush` / `flush_page` 返回成功即代表数据已持久化（`fsync` 完成），崩溃恢复后不会回退到更早内容。
- 回写失败会被记录并向调用方报告：`flush_page`/`flush_all` 抛出 `WritebackError`，统计中的 `writeback_failures` 递增。
- 启动时重放日志中未完成的回写；校验失败的损坏页抛出 `StorageCorruptionError`，不会当作有效数据返回。

## 配置项（`CacheConfig`）

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `page_size` | 4096 | 页大小（字节） |
| `max_pages` | 64 | 缓存页数上限，超出触发淘汰 |
| `dirty_low_watermark` | 8 | 脏页数超过该值启动后台回写 |
| `dirty_high_watermark` | 16 | 脏页数达到该值对写入方限流 |
| `throttle_timeout` | 5.0 | 限流等待超时（秒），超时抛 `ThrottleTimeoutError` |
| `writeback_interval` | 0.05 | 后台回写线程唤醒间隔（秒） |

淘汰策略：优先直接丢弃干净页；无干净页时选择脏页先回写成功再释放；正在被读写的页不会被淘汰。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/pages/{page_id}` | 读页，返回 `{"page_id", "data"(base64)}` |
| `PUT` | `/pages/{page_id}` | 写页，请求体 `{"data"(base64)}` |
| `POST` | `/flush` | 整体刷盘 |
| `POST` | `/flush/{page_id}` | 指定页刷盘 |
| `GET` | `/stats` | 统计：`hits`/`misses`/`dirty_pages`/`cached_pages`/`max_pages`/`writeback_failures` |

## 运行与测试

```bash
uv sync
uv run uvicorn main:app        # 启动服务
uv run pytest                  # 运行全部测试
```
