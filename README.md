# Storegate

Storegate 是一个异步、多后端文件存储服务。项目以 `AbstractStorage` 作为统一接口，可直接操作内存、本地文件系统、S3、WebDAV 和 FTP，也可以叠加缓存与分块索引层；同一套存储实现还能通过 WebDAV 或 FTP 协议对外提供服务。

> 当前版本：`0.1.0`。项目仍处于开发阶段，接口与配置格式可能调整。

## 主要能力

- 统一的异步文件接口：上传、下载、复制、移动、删除、目录遍历和元数据查询。
- 多种存储后端：Memory、Local、S3-compatible、WebDAV、FTP。
- 可组合中间层：
  - `CachedStorage`：缓存元数据、目录列表和小文件下载，支持内存与 Redis 缓存。
  - `IndexStorage`：将大文件切分为内容寻址分块，支持 SHA-256 去重和引用计数。
- 双协议服务端：将任意 `AbstractStorage` 暴露为 WebDAV 或 FTP 服务。
- JSON 对象工厂：通过嵌套配置组合存储和服务，无需在代码中手动组装对象图。
- 异步 I/O：基于 AnyIO、HTTPX、aioftp、Uvicorn 和 WsgiDAV。

## 架构概览

```mermaid
flowchart LR
    Client[应用代码] --> Storage[AbstractStorage]
    DAVUser[WebDAV 客户端] --> DAVServer[DAVServer]
    FTPUser[FTP 客户端] --> FTPServer[FTPServer]
    DAVServer --> Storage
    FTPServer --> Storage

    Storage --> Memory[MemoryStorage]
    Storage --> Local[LocalStorage]
    Storage --> S3[S3Storage]
    Storage --> DAV[DavStorage]
    Storage --> FTP[FTPStorage]
    Storage --> Cached[CachedStorage]
    Storage --> Index[IndexStorage]

    Cached --> Inner[任意 AbstractStorage]
    Index --> IndexMeta[索引存储]
    Index --> Chunks[分块存储]
```

项目分为两层：

1. **存储层 `app/storage/`**：定义统一文件系统语义，并实现各存储后端和组合层。
2. **协议层 `app/server/`**：将存储接口适配为 WebDAV 或 FTP 服务。

### 存储后端

| 后端 | 用途 | 关键特性 |
| --- | --- | --- |
| `MemoryStorage` | 测试、临时数据 | 无外部依赖，进程退出后数据丢失 |
| `LocalStorage` | 本地文件系统 | 限制所有路径位于指定根目录内 |
| `S3Storage` | AWS S3 与兼容服务 | SigV4、目录标记、分片上传、并发复制与失败回滚 |
| `DavStorage` | 远程 WebDAV | Basic/Bearer/匿名认证、流式传输、服务端 COPY/MOVE |
| `FTPStorage` | 远程普通 FTP | 连接池、流式传输、逻辑根目录隔离 |
| `CachedStorage` | 装饰其他后端 | 元数据交叉回填、写后回填、内存/Redis 缓存 |
| `IndexStorage` | 大文件分块与去重 | SHA-256 内容寻址、引用计数、并发上传、文件锁 |

`LocalStorage` canonicalizes the configured root once (so a symlinked root is
stable) and rejects symlink, junction, and Windows reparse-point components
in every logical path, including operation destinations and tree operations.
This policy addresses static path configuration and accidental link traversal;
it is not a general OS sandbox and does not claim to close check-to-use (TOCTOU)
races against a concurrent local attacker. Descriptor/handle-relative access is
outside this backend's threat model.

## 环境要求

- Python `>= 3.14`
- [uv](https://docs.astral.sh/uv/)（推荐的依赖与运行环境管理工具）

安装项目及开发依赖：

```bash
uv sync --group dev
```

## 快速开始

### 直接使用存储接口

```python
import anyio

from app.storage.memory import MemoryStorage


async def main() -> None:
    async with MemoryStorage() as storage:
        await storage.mkdir("/docs")
        await storage.upload_bytes(b"hello storegate", "/docs/hello.txt")

        info = await storage.stat("/docs/hello.txt")
        content = await storage.download_bytes(info.path)

        print(info)
        print(content.decode())


anyio.run(main)
```

本地文件系统只需替换后端：

```python
from app.storage.local import LocalStorage

storage = LocalStorage("./runtime/files")
```

所有后端都支持异步上下文管理器。网络后端应在 `async with` 中使用，以确保连接被正确建立和关闭。

### 启动 WebDAV 服务

```python
import anyio

from app.server.dav import DAVServer
from app.storage.local import LocalStorage


async def main() -> None:
    server = DAVServer(
        storage=LocalStorage("./runtime/dav"),
        host="127.0.0.1",
        port=8080,
    )
    await server.serve()


anyio.run(main)
```

启动后可使用支持 WebDAV 的文件管理器或客户端访问 `http://127.0.0.1:8080/`。

### 启动 FTP 服务

```python
import anyio

from app.server.ftp import FTPServer
from app.storage.local import LocalStorage


async def main() -> None:
    server = FTPServer(
        storage=LocalStorage("./runtime/ftp"),
        host="127.0.0.1",
        port=2121,
    )
    await server.serve()


anyio.run(main)
```

当前 FTP 服务端支持匿名访问，默认开发端口为 `2121`。

## JSON 配置与对象工厂

Storegate 使用 `$factory` 描述要创建的对象：

- `~name`：解析为 `app.storage.name.Storage`
- `~name:ClassName`：解析为指定存储类
- `@name`：解析为 `app.server.name.Server`
- `@name:ClassName`：解析为指定服务类
- 包含 `$factory` 的嵌套对象会被递归创建

例如，将本地存储包装为缓存存储，再通过 WebDAV 暴露：

```json
{
  "$factory": "@dav",
  "storage": {
    "$factory": "~cached",
    "storage": {
      "$factory": "~local",
      "root": "./runtime/files"
    },
    "ttl": 60,
    "download_cache_threshold": 65536
  },
  "host": "127.0.0.1",
  "port": 8080
}
```

加载配置：

```python
import anyio

from app.server import resolve_server_from_file

server = resolve_server_from_file("server.json")
anyio.run(server.serve)
```

也可使用 `resolve_storage()`、`resolve_storage_from_file()` 和 `resolve_server()` 直接解析字典或 JSON 文件。

## 后端配置

### S3-compatible

```json
{
  "access_key_id": "YOUR_ACCESS_KEY",
  "secret_access_key": "YOUR_SECRET_KEY",
  "region": "us-east-1",
  "bucket": "storegate",
  "endpoint_url": "localhost:9000",
  "path_style": true,
  "scheme": "http"
}
```

```python
from app.storage.s3 import S3Storage

storage = S3Storage("s3.json")
```

省略 `endpoint_url` 时使用 AWS S3；MinIO 等兼容服务通常需要设置 `endpoint_url` 和 `path_style`。

### WebDAV 客户端

```json
{
  "base_url": "https://dav.example.com/remote.php/dav/files/user",
  "auth_mode": "basic",
  "username": "user",
  "password": "password",
  "root_prefix": "/storegate",
  "verify_ssl": true
}
```

`auth_mode` 支持 `basic`、`bearer` 和 `anonymous`。使用自签名证书时可通过 `ca_cert_path` 指定 CA 文件；生产环境不建议关闭 `verify_ssl`。

### FTP 客户端

```json
{
  "host": "ftp.example.com",
  "port": 21,
  "username": "user",
  "password": "password",
  "root_prefix": "/storegate",
  "timeout": 30,
  "max_connections": 2
}
```

当前仅支持普通 FTP，不支持 FTPS/FTPES。

### 分块索引存储

`IndexStorage` 需要两个不同的存储实例：`index` 保存文件元数据，`chunks` 保存去重后的内容分块。

```json
{
  "$factory": "~index",
  "index": {
    "$factory": "~local",
    "root": "./runtime/index"
  },
  "chunks": {
    "$factory": "~local",
    "root": "./runtime/chunks"
  },
  "block_size": 67108864,
  "max_concurrent_uploads": 2
}
```

不要让 `index` 与 `chunks` 指向同一个存储实例。`skip_locking` 会关闭并发写保护，只应在能够确认不存在并发写入时使用。

## 公共存储接口

`AbstractStorage` 的主要方法：

- 生命周期：`connect()`、`close()`、`ping()`
- 上传：`upload_stream()`、`upload_bytes()`、`upload_file()`
- 下载：`download_stream()`、`download_bytes()`、`download_file()`
- 文件操作：`copy()`、`move()`、`unlink()`、`delete()`、`delete_many()`
- 目录操作：`mkdir()`、`rmdir()`、`rmtree()`、`copytree()`、`movetree()`
- 查询与遍历：`exists()`、`is_file()`、`is_dir()`、`stat()`、`iterdir()`、`list_()`、`walk()`

路径会被统一规范化为 POSIX 风格的绝对逻辑路径。具体异常语义以 `app/storage/abstract.py` 中的接口文档和契约测试为准；少数远程协议受服务端行为限制，可能存在后端差异。

## 开发与测试

```bash
# 全部测试；外部 S3 配置不可用时建议使用下一条命令
uv run pytest

# 排除需要外部 S3 凭据的测试
uv run pytest -m "not s3"

# 本地 WebDAV / FTP 协议集成测试
uv run pytest -m integration

# 仅运行 MemoryStorage 公共契约
uv run pytest tests/contract/storage -k memory

# Lint、格式化与类型检查
uv run ruff check --fix
uv run ruff format
uv run ty check

# 安装预提交钩子
uv run prek install
```

测试以 `tests/contract/storage/` 中的公共契约为核心，同一组行为会针对多个后端参数化执行。DAV 与 FTP 集成测试会在本地启动临时协议服务；外部 S3 测试需要单独提供凭据。

## 目录结构

```text
app/
├── storage/
│   ├── abstract.py       # AbstractStorage 与 FileInfo
│   ├── factory.py        # 存储配置解析
│   ├── memory/           # 内存后端
│   ├── local/            # 本地文件系统后端
│   ├── s3/               # S3-compatible 客户端与存储适配
│   ├── dav/              # WebDAV 客户端与存储适配
│   ├── ftp/              # FTP 客户端、连接池与存储适配
│   ├── cached/           # 缓存装饰层及缓存后端
│   └── index/            # 分块索引与引用计数
├── server/
│   ├── dav/              # WebDAV 服务端
│   └── ftp/              # FTP 服务端
├── log.py                # Loguru 日志配置
└── utils.py              # 对象工厂、异常翻译等通用工具

tests/
├── contract/storage/     # 多后端公共契约
├── storage/              # 各后端专项测试
├── server/               # 协议服务端测试
├── fixtures/             # 存储与本地协议服务 fixture
└── support/              # 测试辅助工具
```

## 安全说明与当前限制

- 当前 WebDAV 服务端允许匿名访问，FTP 服务端同样使用匿名用户；请仅绑定到可信网络或在前置代理/网络层增加认证与访问控制。
- FTP 后端目前不支持加密传输；跨不可信网络应优先使用 WebDAV over HTTPS 或在受保护网络中部署。
- 配置文件可能包含 S3、WebDAV、FTP 或 Redis 凭据。不要提交真实密钥；项目已忽略根目录下的 `data/`。
- S3、WebDAV 和 FTP 的复制、覆盖及目录删除语义可能受远端服务实现影响。接入新的服务端时应先运行对应契约和集成测试。
- `IndexStorage` 的索引与分块存储存在绑定关系；迁移或清理任一侧前应先确认引用数据的一致性。
