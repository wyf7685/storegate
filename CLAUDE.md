# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

一个基于 `AbstractStorage` 抽象的多存储后端文件服务，同时暴露 FTP 和 WebDAV 两种协议访问。

## 开发命令

```bash
# 测试
uv run pytest                    # 全部测试（含 COS，需要 credentials）
uv run pytest -m "not cos"       # 跳过 COS（CI / 本地无 credentials）
uv run pytest -k "memory"        # 仅 MemoryStorage 参数化
uv run pytest -k "index"         # 仅 IndexStorage 参数化

# 静态检查
uv run ruff check --fix          # lint 检查 + 自动修复
uv run ruff format               # 格式化（修改文件）
uv run ty check                  # 静态类型检查
```

本项目使用 Python 3.14，包管理器为 `uv`。pytest 配置在 `pyproject.toml` 的 `[tool.pytest.ini_options]`，含 `asyncio_mode = "auto"`。已配置 `prek` pre-commit hooks（ruff 检查 + 格式化）。

## 代码风格

**Ruff 配置** (`ruff.toml`)：
- 行长 120 字符，缩进 4 空格，目标 Python 3.14
- 格式化：双引号、LF 换行、空格缩进
- 启用规则集：Pyflakes、pycodestyle、pyupgrade、Ruff、eradicate、flynt、refurb、isort、PEP8-naming、pandas-vet、Perflint、pygrep-hooks、tryceratops、flake8-async、flake8-annotations、flake8-bandit、flake8-builtins、flake8-bugbear、flake8-comprehensions、flake8-datetimez、flake8-debugger、flake8-errmsg、flake8-future-annotations、flake8-implicit-str-concat、flake8-import-conventions、flake8-pie、flake8-print、flake8-pyi、flake8-pytest-style、flake8-quotes、flake8-raise、flake8-return、flake8-self、flake8-simplify、flake8-slots、flake8-tidy-imports、flake8-unused-arguments、flake8-use-pathlib、flake8-type-checking、flake8-2020
- 测试文件 (`tests/*.py`) 特殊豁免：允许 `print`、访问私有成员、硬编码密码（mock 用途）、阻塞方法
- 已忽略规则：模块导入不在顶部、模糊 Unicode 字符、any-type、vanilla raise、try 内 raise、f-string 异常、尾随逗号、注释代码等

### 测试架构

```
tests/
├── conftest.py              # 核心 fixture：uid() + 参数化 storage（5 后端）
├── test_storage_general.py  # 通用接口测试（对所有后端执行）
├── test_cos_internals.py    # COS 特有测试（标记 @pytest.mark.cos）
├── test_cos_rollback.py     # COS 回滚路径测试（mocked client）
├── test_cached_storage.py   # CachedStorage 缓存状态验证（16 测试）
├── test_index_storage.py    # IndexStorage 分块/引用计数验证（15 测试）
├── test_ftp_handler.py      # FTP 命令分发单元测试
└── test_storage_factory.py  # ObjectSpec 序列化/反序列化测试
```

**参数化 fixture**：`storage` fixture 将每个测试对 5 个后端各执行一次：`memory` / `local` / `cos` / `cached` / `index`。COS 在 credentials 缺失时 `pytest.skip()`。`cached` 和 `index` 均以 `MemoryStorage` 为内部后端，保证可重现。使用 `pytest-xdist` 并行执行测试（`-n auto`）。

**覆盖率**：`pytest-cov` 已配置，运行 `uv run pytest` 自动输出覆盖报告。

## 架构

### 分层结构

```
app/
├── const.py               # ROOT, DEFAULT_CHUNK_SIZE
├── log.py                 # loguru 日志配置 + logging→loguru 桥接
├── utils.py               # LoggerWrapper (+ LoguruOpts), ExceptionTranslator, coalesce_chunks, flatten_exception_group
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
│   │   ├── backend/       # CacheBackend 抽象
│   │   │   ├── base.py    # CacheBackend ABC + snapshot() 内省方法（测试用）
│   │   │   ├── memory.py  # MemoryCacheBackend: 基于 expiringdictx 的内存实现
│   │   │   └── redis.py   # RedisCacheBackend: 基于 redis.asyncio 的 Redis 实现
│   │   └── storage.py     # CachedStorage: 交叉缓存后填、写入回填、dump_cache()
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
tests/
├── conftest.py            # uid(), 参数化 storage fixture（5 后端）
├── test_storage_general.py
├── test_cos_internals.py
├── test_cached_storage.py
└── test_index_storage.py
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

### 各后端行为差异

`AbstractStorage` 的文档（docstring）是各方法的权威契约。以下为已知的跨后端差异：

| 方法 / 场景 | MemoryStorage | LocalStorage | CosStorage | IndexStorage |
|---|---|---|---|---|
| `rmdir` nonexistent | `FileNotFoundError` | `FileNotFoundError` | 静默成功 | `FileNotFoundError` |
| `rmtree` 文件路径 | `NotADirectoryError` | `NotADirectoryError` | `NotADirectoryError` | 委托内部后端 |
| `move` 目标为文件 | `FileExistsError` | 覆盖（POSIX rename） | 覆盖（copy+unlink） | `FileExistsError` |
| `copy` 目标存在 | `FileExistsError` | 覆盖（shutil） | 覆盖（put_object_copy） | `FileExistsError` |
| `delete_many` | fail-fast（基类默认） | fail-fast（基类默认） | fail-fast（COS 批量优化） | fail-fast（基类默认） |
| `upload_stream` 重复路径 | stat → IsADirectoryError/FileExistsError | ← 同 | ← 同 | ← 同 |

所有后端的 `upload_stream` 入口统一通过 `stat()` 网关检查：目录 → `IsADirectoryError`，文件 + overwrite=False → `FileExistsError`。

### 关键实现细节

- **COS 目录模拟**: 以 `<key>/` 标记对象表示目录，其内容为序列化的 `FileInfo`。`cos_client/` 是手写的 COS API 封装（基于 httpx，V5 签名）。大文件分片上传（`MultipartUploadTask`，支持重试和并发），大文件服务端拷贝自动切换为 multipart copy（>4MiB），`delete`/`delete_many` 内联分支减少请求（`delete_many` 复用已获取的 `dir_key` 而非通过 `is_dir()` 再次 `head_object`），`rmtree` 批量删除（每次 100 个），`copytree` 并发复制。**回滚支持**: `move`、`copytree`、`_copy_multipart` 失败时自动回滚（删除已复制的目标或 abort multipart upload）。
- **IndexStorage**: 大文件按 `BLOCK_SIZE (64MB)` 拆分分块，SHA-256 去重，引用计数管理。使用基于文件锁的乐观并发控制（`_acquire_storage_file_lock`）。分块存储在 `hash[:2]/hash[2:6]/hash[6:].{bin,ref,lock}` 路径下。`upload_stream` 使用 worker pool 并发上传分块，失败时回滚已写入分块的引用计数。覆盖写入时自动 decref 旧文件不再引用的分块。**所有公开入口方法统一调用 `_to_abs_path()` 规范化路径为绝对路径**，确保 `FileMeta` 和 chunk ref 文件中存储的路径一致。`copytree`/`movetree` 批量操作 chunk refs（`incref`/`transref`），`list_()`/`walk()` 并发获取文件元数据。`move` 和 `copy` 使用 `PurePosixPath` 而非 `Path`（跨平台一致性）。
- **CachedStorage**: 6 个命名空间缓存 (`exists/is_file/is_dir/stat/iterdir/download`)。正结果交叉后填（如 `is_file=True` → 同时缓存 `exists=True, is_dir=False`）。写入操作回填已知状态。`CacheBackend` 抽象接口支持批量操作（`mget`/`mset`/`mdelete`），内置 `MemoryCacheBackend` 和 `RedisCacheBackend`。**`snapshot()` / `dump_cache()`** 内省 API 暴露缓存内部状态供测试验证命中/未命中/回填/失效行为。`list_()` 永远绕过 `iterdir` 缓存直接查询底层存储但回填逐条目元数据缓存。
- **FTP**: 完全自研的异步实现，非 pyftpdlib。命令分发使用生产者-消费者模式（`cmd_send/cmd_receive` memory stream）。慢操作通过 `_execute_with_monitor` 在 task group 中运行，支持 ABOR 取消。数据通道支持 PASV/PORT。
- **WebDAV**: 基于 wsgidav，通过 `run_async()` 桥接同步 wsgidav 到异步 `AbstractStorage`。`ResourceWriter` 使用独立线程 + memory object stream 处理异步上传。
- **日志**: 使用 loguru，`LoggerWrapper` 为每个类提供带色彩标签的实例日志器。支持 `LoguruOpts` 灵活配置日志选项（exception/record/lazy/colors/raw/capture/depth/ansi）。`LoguruHandler` 将标准库 logging 桥接到 loguru。
- **异常处理**: `ExceptionTranslator` 提供统一的异常翻译装饰器，支持 `bypass`、`catch`、`default` 异常分类和自定义异常映射。同时支持 `wrap`（普通异步函数）和 `wrap_agen`（异步生成器）。

### 配置与数据目录

- COS 测试使用 `data/cos/mock.json`（专门的测试桶，不进 git）
- 日志输出到 `logs/` 目录，按日轮转
- 根目录的 `test*.py` 和 `run*.py` 模式已加入 `.gitignore`（真实测试在 `tests/` 目录）
