# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## 项目概述

一个基于 `AbstractStorage` 抽象的多存储后端文件服务，同时暴露 FTP 和 WebDAV 两种协议访问。

## 开发命令

```bash
# 测试
uv run pytest                                # 全部测试（含 S3，需要 credentials）
uv run pytest -m "not s3"                    # 跳过外部 S3 测试（CI / 本地无 credentials）
uv run pytest -m integration                 # 本地 DAV / FTP 协议集成测试
uv run pytest tests/contract/storage -k memory  # 仅 MemoryStorage 公共契约

# 静态检查
uv run ruff check --fix          # lint 检查 + 自动修复
uv run ruff format               # 格式化（修改文件）
uv run ty check                  # 静态类型检查
```

本项目使用 Python 3.14，包管理器为 `uv`。pytest 配置在 `pyproject.toml` 的 `[tool.pytest.ini_options]`，含 `asyncio_mode = "auto"`。已配置 `prek` pre-commit hooks（ruff 检查 + 格式化 + ty 检查）。

## 开发要求

### 类型检查（ty）

项目使用 **ty**（而非 pyright）进行静态类型检查。所有代码必须：

- **完整类型注解**：所有公开函数、方法、变量必须有类型注解
- **通过 `uv run ty check`**：提交前必须确保 ty 检查零错误
- **`# ty: ignore` 仅限测试**：仅允许在单元测试中 mock 场景下使用 `# ty: ignore` 注释，业务代码不允许

### Lint（ruff）

项目使用 **ruff** 执行 lint 和格式化。所有代码必须：

- **完全遵循 `ruff.toml`**：代码风格必须与 `ruff.toml` 中指定的所有规则一致
- **通过 `uv run ruff check --fix`**：lint 检查必须零错误
- **通过 `uv run ruff format`**：格式化检查必须零变更

Agent 编写代码时须遵守以下纪律：

- **禁止擅自修改 `ruff.toml`**：不得未经用户许可更改任何 lint 配置
- **禁止文件级 `noqa`**：不得添加作用于整个文件的 `# noqa` 或 `per-file-ignores` 等豁免
- **允许针对性 `noqa`**：仅在必要且合理的少数位置，针对具体行添加 `# noqa: <rule>` 注释
- **测试文件豁免**：对于单元测试代码（`tests/*.py`），必要时可在获得用户许可后，向 `ruff.toml` 的 `lint.extend-per-file-ignores` 追加排除规则

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
├── conftest.py                    # 全局日志配置 + fixture plugin 注册
├── fixtures/
│   ├── storage.py                 # 7 后端参数化 storage fixture
│   └── protocol_servers.py        # session 级 DAV / FTP 本地协议服务
├── support/
│   ├── ids.py                     # uid() 隔离标识；测试禁止从 conftest.py 直接导入
│   └── factory_targets.py         # resolve_object 动态导入测试目标
├── contract/storage/              # AbstractStorage 公共契约
│   ├── test_metadata.py
│   ├── test_files.py
│   ├── test_directories.py
│   ├── test_trees.py
│   └── test_paths.py
├── storage/
│   ├── factory/                   # resolve_object / resolve_storage
│   ├── cached/                    # 缓存填充、失效、下载缓存、Redis backend
│   ├── index/                     # 分块、引用计数、目录操作、回滚
│   ├── dav/                       # 配置、认证、multistatus、错误映射
│   │   └── integration/           # 本地 wsgidav 上的 DavStorage 集成测试
│   ├── ftp/                       # FTPStorage 配置、连接池、流式与树操作
│   └── s3/                        # SigV4 与 mocked rollback
│       └── integration/           # 需要 credentials 的目录标记测试
└── server/ftp/                    # FTPServer 命令与协议行为集成测试
```

**参数化契约**：`tests/fixtures/storage.py` 的 `storage` fixture 将 `tests/contract/storage/` 对 7 个后端执行：`memory` / `local` / `s3` / `cached` / `index` / `ftp` / `dav`。S3 缺少配置时 `pytest.skip()`；`cached` 和 `index` 使用 `MemoryStorage` 保证可重现；DAV 和 FTP 通过 `tests/fixtures/protocol_servers.py` 启动独立线程、事件循环和随机端口的本地服务。

**测试分层**：目录路径表达被测组件，marker 只表达运行性质。`integration` 表示启动本地 DAV / FTP 协议服务，`s3` 表示需要外部 S3 credentials，`slow` 表示异常耗时测试。纯单元测试不得仅因属于某个后端而标记为 integration。

**测试辅助代码**：可导入辅助函数放在 `tests/support/`。`conftest.py` 只用于 fixture 和 pytest 配置，不作为普通 Python 模块导入。后端专属 fixture 放在对应目录的 `conftest.py`。

**覆盖率**：`pytest-cov` 已配置，运行 `uv run pytest` 自动输出覆盖报告。当前测试源码收集为 629 个参数化用例；无外部 S3 时执行 562 个用例。

## 架构

### 分层结构

```
app/
├── const.py               # ROOT, DEFAULT_CHUNK_SIZE
├── log.py                 # loguru 日志配置 + logging→loguru 桥接
├── utils.py               # LoggerWrapper (+ LoguruOpts), resolve_object, ExceptionTranslator, coalesce_chunks, flatten_exception_group
├── storage/               # 存储抽象层
│   ├── abstract.py        # AbstractStorage ABC + FileInfo dataclass
│   ├── factory.py         # resolve_storage / resolve_storage_from_file（基于 resolve_object）
│   ├── s3/                # S3 兼容实现（AWS SigV4，支持 AWS S3 / MinIO / 腾讯云 COS 等）
│   │   ├── client/        # 自研 S3 SDK (httpx-based, AWS SigV4 签名)
│   │   │   ├── auth.py     # AWSSigV4Signer: SigV4 签名算法
│   │   │   ├── client.py   # AsyncS3Client: httpx-based HTTP 客户端
│   │   │   ├── errors.py   # S3ClientError, S3HttpStatusError, S3ResponseParseError
│   │   │   └── models.py   # S3Config, HeadObjectOutput, ListObjectsContents/CommonPrefix
│   │   ├── storage.py     # S3Storage: 目录标记对象、分片上传、concurrent list_
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
│   ├── memory/            # MemoryStorage: 内存实现，测试/临时用途
│   ├── ftp/               # FTPStorage: plain FTP 客户端后端
│   │   ├── config.py      # FTPConfig: 认证、root_prefix、timeout、max_connections
│   │   ├── pool.py        # FTPClientPool / FTPClientLease: 独占租借、淘汰、关闭
│   │   └── storage.py     # FTPStorage: FTP → AbstractStorage 映射
│   └── dav/               # WebDAV 客户端实现（访问远程 WebDAV 服务器）
│       ├── client/        # 自研 WebDAV SDK (httpx-based)
│       │   ├── auth.py     # build_auth: Basic / Bearer / Anonymous
│       │   ├── client.py   # AsyncDavClient: httpx-based, PROPFIND/MKCOL/COPY/MOVE
│       │   ├── errors.py   # DavClientError, DavHttpStatusError, DavResponseParseError
│       │   └── models.py   # DavConfig, DavResource, AuthMode
│       ├── storage.py     # DavStorage: WebDAV → AbstractStorage 映射
│       └── utils.py       # multistatus XML 解析、DavResource → FileInfo
├── server/               # 协议服务层
│   ├── abstract.py       # AbstractServer ABC（storage + serve()）
│   ├── factory.py        # resolve_server / resolve_server_from_file（基于 resolve_object）
│   ├── dav/              # WebDAV 服务（wsgidav + uvicorn）
│   │   ├── server.py     # DAVServer: 创建 wsgidav app, 包装 ASGI lifespan
│   │   ├── provider.py   # StorageProvider: 桥接 AbstractStorage → wsgidav DAVProvider
│   │   ├── collection.py # StorageCollection: 目录操作（mkdir/rmtree/copytree/movetree）
│   │   ├── resource.py   # StorageResource + ResourceReader/Writer: 文件读写
│   │   └── utils.py      # run_async (anyio.from_thread.run), call_with_catch, ContextVar
│   └── ftp/              # FTP 服务（aioftp + AbstractStorage 适配层）
│       ├── server.py     # FTPServer: 配置 aioftp 服务与支持的命令
│       ├── pathio.py     # StoragePathIO: 适配 AbstractStorage 到 aioftp PathIO
│       └── handle.py     # 流式 ReadHandle / WriteHandle
tests/
├── conftest.py             # 全局 pytest 配置与 fixture plugin 注册
├── fixtures/               # 参数化 storage、DAV / FTP 本地服务
├── support/                # uid、动态 factory 测试目标
├── contract/storage/       # 7 后端共享 AbstractStorage 契约
├── storage/                # factory / cached / index / dav / ftp / s3 专属测试
│   ├── dav/integration/    # 本地 wsgidav 集成测试
│   └── s3/integration/     # 外部 S3 credentials 测试
└── server/ftp/             # FTPServer 协议集成测试
```

### 核心抽象与设计模式

**`AbstractStorage`** 是唯一存储接口，所有后端之间互相对称：
- **简单后端**: `LocalStorage`, `MemoryStorage` — 直接实现存储
- **网络后端**: `S3Storage`, `DavStorage`, `FTPStorage` — 使用异步网络客户端并映射为统一存储契约；S3/DAV 使用自研 httpx SDK，FTP 使用 aioftp + 独占 client pool
- **中间件模式**: `CachedStorage` 和 `IndexStorage` 都包装其他 `AbstractStorage` 实例，形成装饰器链。典型堆叠: `IndexStorage(CachedStorage(S3Storage(...), S3Storage(...)))`
- **文件操作**: `upload/download_stream`、`unlink/rmdir/delete/delete_many`、`move/copy`
- **目录操作**: `mkdir/rmtree`、`copytree/movetree`（`movetree` 默认实现为 copy + rmtree，后端可覆盖为更高效的实现）

**`resolve_object`** (`utils.py`) 是基于 dict 的依赖注入函数，使用 `$factory` 键约定动态构建对象图。`app/storage/factory.py` 和 `app/server/factory.py` 分别提供类型守卫的 `resolve_storage` / `resolve_server`：
```json
{"$factory": "~s3", "config": "data/config.json"}
{"$factory": "~cached", "storage": {"$factory": "~memory", "root": "cache-root"}, "ttl": 60}
```
- `~name` 简写 → `app.storage.name.Storage`（默认 cls），或 `~name:ClassName`
- `@name` 简写 → `app.server.name.Server`（默认 cls），或 `@name:ClassName`
- 嵌套 spec 通过 dict + `$factory` 键递归识别

### 各后端行为差异

`AbstractStorage` 的文档（docstring）是各方法的权威契约。以下为已知的跨后端差异：

| 方法 / 场景 | MemoryStorage | LocalStorage | S3Storage | IndexStorage | DavStorage | FTPStorage |
|---|---|---|---|---|---|---|
| `rmdir` nonexistent | `FileNotFoundError` | `FileNotFoundError` | 静默成功 | `FileNotFoundError` | 静默成功（对齐 S3） | `FileNotFoundError` |
| `rmtree` 文件路径 | `NotADirectoryError` | `NotADirectoryError` | `NotADirectoryError` | 委托内部后端 | `NotADirectoryError` | `NotADirectoryError` |
| `move` 目标为文件 | `FileExistsError` | 覆盖（POSIX rename） | 覆盖（copy+unlink） | `FileExistsError` | 覆盖（MOVE Overwrite:T，412 时先删目标再重试） | `FileExistsError` |
| `copy` 目标存在 | `FileExistsError` | 覆盖（shutil） | 覆盖（CopyObject） | `FileExistsError` | 覆盖（COPY Overwrite:T） | 覆盖（RETR→STOR 中继） |
| `delete_many` | fail-fast（基类默认） | fail-fast（基类默认） | fail-fast（S3 批量优化） | fail-fast（基类默认） | fail-fast（基类默认） | fail-fast（基类默认） |
| `upload_stream` 重复路径 | stat → IsADirectoryError/FileExistsError | ← 同 | ← 同 | ← 同 | ← 同 | ← 同 |

所有后端的 `upload_stream` 入口统一通过 `stat()` 网关检查：目录 → `IsADirectoryError`，文件 + overwrite=False → `FileExistsError`。

### 关键实现细节

- **S3 目录模拟**: 以 `<key>/` 标记对象表示目录，其内容为序列化的 `FileInfo`。`client/` 是手写的 S3 API 封装（基于 httpx，AWS SigV4 签名），支持 AWS S3 及所有 S3 兼容服务（MinIO、腾讯云 COS 等），通过 `S3Config.endpoint_url` + `path_style` 配置端点寻址（virtual-hosted / path-style）。大文件分片上传（`MultipartUploadTask`，支持重试和并发），大文件服务端拷贝自动切换为 multipart copy（>4MiB），`delete`/`delete_many` 内联分支减少请求（`delete_many` 复用已获取的 `dir_key` 而非通过 `is_dir()` 再次 `head_object`），`rmtree` 批量删除（每次 100 个），`copytree` 并发复制。**回滚支持**: `move`、`copytree`、`_copy_multipart` 失败时自动回滚（删除已复制的目标或 abort multipart upload）。
- **DavStorage**: WebDAV 客户端后端，与 `S3Storage` 对称（自研 `client/` SDK + `storage.py` 适配器，基于 httpx）。支持 Basic / Bearer / 匿名认证与 HTTPS/TLS（含自定义 CA `ca_cert_path`）。`root_prefix` 在 `base_url` 下隔离多实例（类比 S3 bucket）。方法映射：`PROPFIND`（Depth:0/1）→ stat/iterdir，`PUT`（httpx 流式 body）→ upload_stream，`GET`（Range）→ download_stream，`MKCOL` → mkdir，`DELETE` → unlink/rmdir/rmtree，`COPY`/`MOVE`（Overwrite 头）→ copy/move/copytree/movetree。`rmdir` 需 DELETE 前用 `iterdir` 预检空（WebDAV DELETE 天然递归）；`rmtree` 单次 DELETE 递归删集合；`walk` 用递归 Depth:1（避开被 Nextcloud/mod_dav 默认禁用的 infinity）；`copytree`/`movetree` 优先服务端 `COPY`/`MOVE` Depth:infinity，服务器返回 403/405/409/501 时回退 walk+逐文件 copy（回退失败 `rmtree(dst)` 回滚）。`move` 对 412（服务器拒覆盖）先删目标再重试以维持覆盖语义。`ExceptionTranslator` 映射 404→FileNotFoundError、403/423→PermissionError、412→FileExistsError；405 语义重载（MKCOL 已存在 / PUT 到集合）在各方法内联处理。multistatus XML 用 `xml.etree` + `{*}tag` 通配解析（与 S3 client 同法）。
- **IndexStorage**: 大文件按 `BLOCK_SIZE (64MB)` 拆分分块，SHA-256 去重，引用计数管理。使用基于文件锁的乐观并发控制（`_acquire_storage_file_lock`）。分块存储在 `hash[:2]/hash[2:6]/hash[6:].{bin,ref,lock}` 路径下。`upload_stream` 使用 worker pool 并发上传分块，失败时回滚已写入分块的引用计数。覆盖写入时自动 decref 旧文件不再引用的分块。**所有公开入口方法统一调用 `_to_abs_path()` 规范化路径为绝对路径**，确保 `FileMeta` 和 chunk ref 文件中存储的路径一致。`copytree`/`movetree` 批量操作 chunk refs（`incref`/`transref`），`list_()`/`walk()` 并发获取文件元数据。`move` 和 `copy` 使用 `PurePosixPath` 而非 `Path`（跨平台一致性）。
- **CachedStorage**: 6 个命名空间缓存 (`exists/is_file/is_dir/stat/iterdir/download`)。正结果交叉后填（如 `is_file=True` → 同时缓存 `exists=True, is_dir=False`）。写入操作回填已知状态。`CacheBackend` 抽象接口支持批量操作（`mget`/`mset`/`mdelete`），内置 `MemoryCacheBackend` 和 `RedisCacheBackend`。**`snapshot()` / `dump_cache()`** 内省 API 暴露缓存内部状态供测试验证命中/未命中/回填/失效行为。`list_()` 永远绕过 `iterdir` 缓存直接查询底层存储但回填逐条目元数据缓存。
- **FTPStorage**: plain FTP 客户端后端，`root_prefix` 将逻辑根隔离到远端绝对 POSIX 子目录；路径拒绝 NUL/`..` 越界。`FTPClientPool` 通过 `max_connections`（默认 1）按需创建 client，每个 lease 独占控制通道，transfer 在 EOF/`aclose()` 前持续持有 lease；取消、timeout 或控制通道失步会 invalidate 并淘汰连接。listing/walk 在 lease 内完整物化并排序，释放后才 yield。FTP 无服务端 COPY，`copy`/`copytree` 使用一个 pool source client + 一个操作级临时 destination client 流式中继；copytree 失败仅回滚本次创建内容。当前仅支持 plain FTP，不支持 FTPS/FTPES。
- **FTP 服务端**: 基于 `aioftp`，`StoragePathIO` 将协议文件操作映射到 `AbstractStorage`，`ReadHandle` / `WriteHandle` 保持下载和上传流式传输。仅支持 `rb` / `wb`，禁用 APPE；REST 仅用于 RETR 下载 offset，非零 REST + STOR 返回 504 且不修改目标文件。
- **WebDAV**: 基于 wsgidav，通过 `run_async()` 桥接同步 wsgidav 到异步 `AbstractStorage`。`ResourceWriter` 使用独立线程 + memory object stream 处理异步上传。
- **日志**: 使用 loguru，`LoggerWrapper` 为每个类提供带色彩标签的实例日志器。支持 `LoguruOpts` 灵活配置日志选项（exception/record/lazy/colors/raw/capture/depth/ansi）。`LoguruHandler` 将标准库 logging 桥接到 loguru。
- **异常处理**: `ExceptionTranslator` 提供统一的异常翻译装饰器，支持 `bypass`、`catch`、`default` 异常分类和自定义异常映射。同时支持 `wrap`（普通异步函数）和 `wrap_agen`（异步生成器）。

### 配置与数据目录

- S3 测试使用 `data/s3/mock.json`（复用腾讯云 COS 的 S3 兼容端点验证，不进 git）
- DavStorage 配置示例 `data/dav/config.json`（`base_url`/`auth_mode`/`username`/`password`/`token`/`root_prefix`/`verify_ssl`/`ca_cert_path`，不进 git）；`dav` 测试用本地 wsgidav，无需配置文件
- 日志输出到 `logs/` 目录，按日轮转
- 根目录的 `test*.py` 和 `run*.py` 模式已加入 `.gitignore`（真实测试在 `tests/` 目录）
