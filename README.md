# 本地页缓存与回写服务（page-cache-python）

一个运行在单进程内的固定大小页缓存，位于上层读写调用与底层块存储之间。
写操作先更新内存缓存并写入 WAL（崩溃恢复依据），数据随后由后台工作线程
**延迟回写**到下层页文件。服务以 Python API 与 HTTP API 两种方式暴露
按页读写、强制刷盘与统计查询能力，不依赖任何外部服务。

## 目录结构

```
src/pagecache/
  config.py     配置项与校验
  errors.py     异常类型
  page.py       页状态与每页锁（rw 锁 + 回写 io 锁）
  storage.py    底层块存储（每页一个文件，原子写 + CRC）
  journal.py    WAL 日志（WRITE/COMMIT/CHECKPOINT，CRC）
  writeback.py  后台回写线程池、去重队列、失败退避
  cache.py      核心缓存：读写、LRU 淘汰、水位节流、统计、恢复
  service.py    服务门面与生命周期
  api.py        FastAPI HTTP 接口
tests/          自动化测试
main.py          uvicorn 入口
```

## 快速开始

```bash
uv sync --group dev
PAGE_CACHE_DATA_DIR=./cache-data uv run uvicorn main:app --port 8000
```

直接在进程内使用：

```python
from pagecache.config import CacheConfig
from pagecache.service import PageCacheService

with PageCacheService(CacheConfig(data_dir="./cache-data")) as svc:
    svc.write_page(7, b"hello")      # 更新缓存并确认（WAL 已落盘）
    svc.read_page(7)                 # 立即读到 b"hello"
    svc.flush_page(7)                # 强制该页持久化
    svc.flush_all()                  # 强制全部脏页持久化
    svc.stats()                      # 命中/脏页/容量等统计
```

## HTTP API

页负载以 base64 字符串在 JSON 中传输。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET`  | `/health` | 健康检查 |
| `GET`  | `/pages/{id}` | 读页（未命中从底层载入） |
| `PUT`  | `/pages/{id}` | 写页，请求体 `{"data": "<base64>"}` |
| `POST` | `/pages/{id}/flush` | 强制落盘指定页 |
| `POST` | `/flush` | 强制落盘全部脏页 |
| `GET`  | `/stats` | 命中、脏页数、容量、淘汰、回写统计 |
| `GET`  | `/invalid-pages` | 启动时识别出的损坏/无法确认的页 |

错误状态码：页不存在 `404`，损坏页 `409`，写节流超时 `503`，刷盘超时 `504`。

## 读写语义

- **读命中**：直接返回缓存内容，保证等于该页最后一次已确认写入。
- **读未命中**：从底层块存储载入到缓存帧后返回；底层不存在时报 `PageNotFoundError`。
- **写可见性**：写操作在更新内存并追加 WAL 的 WRITE+COMMIT（fsync）后返回，
  之后同一进程的任何读取立即看到新内容。
- 每次写入分配单调递增的全局序号（`sequence`），重启后从已恢复的最大序号继续，
  不会出现“新版本序号更小”。

## 延迟回写与顺序保证

- 脏页数量达到 `dirty_low_watermark` 时，后台工作线程开始回写。
- 每页有独立的 `io` 锁：同一页任意时刻只有一个回写在执行，因此同一页的多次
  写入严格按序号落盘，回写落盘的是某一次写入的**完整快照**，并发写与回写交错时
  不会产生半新半旧的撕裂页。
- 回写采用“写临时文件 → fsync → 原子 rename → fsync 目录”，崩溃后页文件要么是
  旧版本完整内容，要么是新版本完整内容。
- 回写失败：页保持脏态并记录错误，`flush_page` / `flush_all` 会抛出
  `WritebackFailedError`；后台线程按 `writeback_retry_interval` 退避重试，
  显式 `flush_page` 会立即再尝试一次。

## 水位与写节流

- 脏页数 `>= dirty_low_watermark`：唤醒回写。
- 脏页数 `>= dirty_high_watermark`：写入方阻塞等待，直到脏页降到高水位以下；
  等待超过 `write_throttle_timeout` 抛 `ThrottleTimeoutError`。

## 容量控制与淘汰

- 缓存帧数由 `capacity` 限定，采用 LRU 淘汰。
- 干净页可直接丢弃；脏页必须回写成功后才能释放；正在读写（`pin_count > 0`）
  或正在回写（`io_inflight`）的页不会被淘汰。
- 缓存满且全是脏页/占用页时，写操作驱动回写并等待，超时抛 `CacheCapacityError`。

## 强制落盘与确认语义

- `flush_page(id)` / `flush_all()` 成功返回即表示对应内容已经 `fsync` 到
  底层存储并写入 CHECKPOINT 记录，崩溃恢复后不会回退到更早内容。
- 刷盘在 `flush_timeout` 内无法完成（例如底层持续故障）时抛
  `FlushTimeoutError` 或最后一次 `WritebackFailedError`。

## 并发与统计

- 全局缓存锁保护页表与统计，页内 `rw` 锁保护单页状态，`io` 锁串行化落盘，
  回写完成通过条件变量通知等待者，避免锁顺序反转。
- 命中数、未命中数、脏页数、缓存页数、淘汰数、回写成功/失败数在并发下
  都在同一把锁内更新，保持一致。

## 崩溃恢复

启动时依次处理：

1. 扫描 WAL（每条记录带 CRC）。损坏或半截的记录尾巴被识别并截断，不会误读。
2. 只有同时存在 WRITE 与对应 COMMIT 的版本才算“已确认”；未 COMMIT 的写入
  不会被当作有效数据。
3. 比较 WAL 已确认版本与页文件版本（页文件同样带 CRC）：
   - 页文件缺失或损坏但 WAL 有已确认版本：用 WAL 内容重建并标记为脏，随后回写；
   - 页文件版本与 WAL 一致：标记为干净并补 CHECKPOINT；
   - 页文件比 WAL 更新（上次回写已落盘但未来得及记 CHECKPOINT）：以页文件为准。
4. 页文件损坏且没有可确认的 WAL 版本时，该页被加入隔离列表
   （`GET /invalid-pages`），读取时报 `CorruptPageError`，绝不会返回无法确认的数据。
5. 正常关闭（`close()`）会先 `flush_all`，确认全部持久化后清空 WAL。

说明：测试中的“崩溃”用 `PageCacheService.crash_close()` 模拟——立即停止工作线程、
不刷盘、不清 WAL，只留下操作系统已经落盘的数据与日志。

## 配置项（`CacheConfig`）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `page_size` | `4096` | 单页最大字节数，写入超过即报错 |
| `capacity` | `1024` | 缓存帧上限（页数） |
| `dirty_low_watermark` | `128` | 达到该脏页数启动回写 |
| `dirty_high_watermark` | `256` | 达到该脏页数开始限流（须 ≤ capacity） |
| `write_throttle_timeout` | `30.0` | 写节流/取帧最长等待秒数 |
| `flush_timeout` | `60.0` | 强制刷盘最长等待秒数 |
| `worker_count` | `2` | 后台回写线程数 |
| `writeback_retry_interval` | `0.2` | 回写失败后的退避秒数 |
| `data_dir` | `./cache-data` | 页文件与 WAL 的目录 |
| `wal_fsync` | `True` | WRITE/COMMIT 是否 fsync（关闭会丢失崩溃确认保证） |

约束：`0 < low ≤ high ≤ capacity`，所有超时与容量必须为正数。

## 测试

```bash
uv run pytest
```

覆盖：命中/未命中、最新写入可见、回写顺序与无撕裂页、回写失败与重试、
低水位回写、高水位节流与超时、干净页淘汰、脏页/占用页保护、同页与跨页并发、
统计一致性、崩溃恢复到最后已确认版本、损坏页隔离，以及完整 HTTP 接口。
