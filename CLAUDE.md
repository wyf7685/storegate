# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

一个基于 `AbstractStorage` 抽象的多存储后端文件服务，同时暴露 FTP 和 WebDAV 两种协议访问。

## 开发命令

```bash
# 静态检查
uv run ruff check --fix    # lint 检查 + 自动修复
uv run ruff format         # 格式化检查（不修改文件）
uv run ty check            # 静态类型检查
```

本项目使用 Python 3.14，包管理器为 `uv`。

## 架构

### 分层结构

```
app/
├── const.py               # ROOT, DEFAULT_CHUNK_SIZE
├── log.py                 # loguru 日志配置 + logging→loguru 桥接
├── utils.py               # LoggerWrapper, with_semaphore, copy_signature,
│                            attach_async_context, SecretStrEncoder, abatched, coalesce_chunks
├── storage/               # 存储抽象层
│   ├── abstract.py        # AbstractStorage ABC + FileInfo dataclass
│   ├── factory.py         # ObjectSpec: JSON → 运行时对象反序列化（resolve_storage）
│   ├── cos/               # 腾讯云 COS 实现
│   │   ├── cos_client/    # 自研 COS SDK (httpx-based, V5 签名)
│   │   │   ├── auth.py     # CosV5Signer: V5 签名算法
│   │   │   ├── client.py   # AsyncCosClient: httpx-based HTTP 客户端
│   │   │   ├── errors.py   # CosClientError, CosHttpStatusError, CosResponseParseError
│   │   │   └── models.py   # CosConfig, HeadObjectResponse, ListObjectsItem/Dir
│   │   ├── storage.py     # CosStorage: 目录标记对象、分片上传、concurrent list_
│   │   └── utils.py       # MultipartUploadTask, 元数据序列化
│   ├── cached/            # 缓存装饰器层
│   │   ├── backend/       # CacheBackend 接口（MemoryCacheBackend / RedisCacheBackend）
│   │   └── storage.py     # CachedStorage: 交叉缓存后填、写入回填
│   ├── index/             # 分块索引层（大文件 → block 拆分 + ref-counted chunk）
│   │   └── storage.py     # IndexStorage: index + chunks 双存储，乐观锁 file-lock
│   ├── local/             # LocalStorage: 本地文件系统
│   └── memory/            # MemoryStorage: 内存实现，测试/临时用途
├── protocol/
│   ├── dav/               # WebDAV 服务（wsgidav + uvicorn）
│   │   ├── server.py      # DAVServer: 创建 wsgidav app, 包装 ASGI lifespan
│   │   ├── provider.py    # StorageProvider: 桥接 AbstractStorage → wsgidav DAVProvider
│   │   ├── collection.py  # StorageCollection: 目录操作（mkdir/rmtree/copytree/movetree）
│   │   ├── resource.py    # StorageResource + ResourceReader/Writer: 文件读写
│   │   └── utils.py       # run_async (anyio.from_thread.run), call_with_catch, ContextVar
│   └── ftp/               # FTP 服务（纯 anyio, 零依赖 FTP 实现）
│       ├── server.py      # FTPServer: TCP listener, 每客户端 FTPSession + FTPHandler
│       ├── handler.py     # FTPHandler: 生产者-消费者命令分发, _execute_with_monitor
│       ├── data.py        # DataConnection: PASV/PORT 数据通道管理
│       ├── session.py     # FTPSession dataclass: cwd/authenticated/rename_from
│       ├── response.py    # FTP 响应码枚举 (R)
│       └── listing.py     # Unix ls -l 格式输出
```

### 核心抽象与设计模式

**`AbstractStorage`** 是唯一存储接口，所有后端之间互相对称：
- **简单后端**: `LocalStorage`, `MemoryStorage` — 直接实现存储
- **中间件模式**: `CachedStorage` 和 `IndexStorage` 都包装其他 `AbstractStorage` 实例，形成装饰器链。典型堆叠: `IndexStorage(CachedStorage(CosStorage(...), CosStorage(...)))`
- **文件操作**: `upload/download_stream`、`unlink/rmdir/delete/delete_many`、`move/copy`
- **目录操作**: `mkdir/rmtree`、`copytree/movetree`（`movetree` 默认实现为 copy + rmtree，后端可覆盖为更高效的实现）

**`ObjectSpec`** (`factory.py`) 是依赖注入机制，允许通过 JSON 配置文件动态构建对象图：
```json
{"factory": "app.storage.cos:CosStorage", "args": {"config": "data/config.json"}}
{"factory": "app.storage.cached:CachedStorage", "args": {"storage": {...}, "ttl": 60}}
```

### 关键实现细节

- **COS 目录模拟**: 以 `<key>/` 标记对象表示目录，其内容为序列化的 `FileInfo`。`cos_client/` 是手写的 COS API 封装（基于 httpx，V5 签名）。大文件分片上传（`MultipartUploadTask`，支持重试和并发），大文件服务端拷贝自动切换为 multipart copy（>4MiB），`delete`/`delete_many` 内联分支减少请求，`rmtree` 批量删除（每次 100 个），`copytree` 并发复制。
- **IndexStorage**: 大文件按 `BLOCK_SIZE (64MB)` 拆分分块，SHA-256 去重，引用计数管理。使用基于文件锁的乐观并发控制（`_acquire_storage_file_lock`）。分块存储在 `hash[:2]/hash[2:6]/hash[6:].bin` 路径下。`upload_stream` 使用 worker pool 并发上传分块，失败时回滚已写入分块的引用计数。`copytree`/`movetree` 批量操作 chunk refs（`incref`/`transref`），`list_()`/`walk()` 并发获取文件元数据。
- **CachedStorage**: 6 个命名空间缓存 (`exists/is_file/is_dir/stat/iterdir/download`)。正结果交叉后填（如 `is_file=True` → 同时缓存 `exists=True, is_dir=False`）。写入操作回填已知状态。`CacheBackend` 抽象接口支持批量操作（`mget`/`mset`/`mdelete`），内置 `MemoryCacheBackend` 和 `RedisCacheBackend`。
- **FTP**: 完全自研的异步实现，非 pyftpdlib。命令分发使用生产者-消费者模式（`cmd_send/cmd_receive` memory stream）。慢操作通过 `_execute_with_monitor` 在 task group 中运行，支持 ABOR 取消。数据通道支持 PASV/PORT。
- **WebDAV**: 基于 wsgidav，通过 `run_async()` 桥接同步 wsgidav 到异步 `AbstractStorage`。`ResourceWriter` 使用独立线程 + memory object stream 处理异步上传。
- **日志**: 使用 loguru，`LoggerWrapper` 为每个类提供带色彩标签的实例日志器。`LoguruHandler` 将标准库 logging 桥接到 loguru。

### 配置与数据目录

- 日志输出到 `logs/` 目录，按日轮转
