import contextlib
from pathlib import Path, PurePosixPath
from typing import override

import anyio

from storegate.storage.abstract import (
    AbstractStorage,
    PathLike,
    StorageCapabilities,
    make_namespace_identity,
)
from storegate.utils import ExceptionTranslator

from .client import AsyncS3Client, S3ClientError, S3Config, S3HttpStatusError

UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
# Files larger than this are copied via multipart upload to stay within
# the CopyObject 5 GiB limit and to allow parallel part copies.
COPY_MULTIPART_THRESHOLD = 4 * 1024 * 1024  # 4MB
# Upper bound on concurrent per-file copies during copytree. Each copy issues
# several sequential requests, so an unbounded fan-out would queue one task per
# file behind the client semaphore.
COPYTREE_MAX_WORKERS = 8


translator = ExceptionTranslator(
    bypass=OSError,
    catch=S3ClientError,
    default=OSError,
)


@translator.handles(S3HttpStatusError)
def _(exc: S3HttpStatusError, msg: str) -> OSError:
    return {404: FileNotFoundError, 403: PermissionError}.get(exc.status_code, OSError)(f"{msg}: {exc}")


_S3_CAPABILITIES = StorageCapabilities(compare_exchange=True)


class S3StorageBase(AbstractStorage):
    """State, lifecycle and key-mapping primitives shared by the S3Storage mixins.

    Splitting the operation mixins out of ``S3Storage`` keeps each transaction
    in its own module; they cooperate only through the members defined here, so
    this class is the whole contract between them.
    """

    _client: AsyncS3Client | None = None
    _config: S3Config

    def __init__(self, config: str | Path | S3Config) -> None:
        super().__init__()
        self._config = config if isinstance(config, S3Config) else S3Config.from_file(config)

    @property
    @override
    def display_id(self) -> str:
        return f"s3:{self._config.bucket}:{self._config.region}"

    @property
    @override
    def namespace_identity(self) -> str:
        config = self._config
        return make_namespace_identity(
            "s3",
            bucket=config.bucket,
            endpoint_url=config.endpoint_url,
            path_style=config.path_style,
            region=config.region,
            scheme=config.scheme,
        )

    @property
    @override
    def capabilities(self) -> StorageCapabilities:
        return _S3_CAPABILITIES

    @override
    async def connect(self) -> None:
        if self._client is not None:
            retained = self._client
            with anyio.CancelScope(shield=True):
                await retained.__aexit__(None, None, None)
                if self._client is retained:
                    self._client = None
        client = AsyncS3Client(self._config)
        self._client = client
        try:
            await client.__aenter__()
            if not await self.ping():
                raise RuntimeError("Failed to connect to S3 bucket. Please check your configuration.")
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await client.__aexit__(None, None, None)
                except BaseException as secondary:
                    cleanup_error = secondary
                else:
                    self._client = None
            if cleanup_error is not None:
                self._client = client
                raise BaseExceptionGroup("S3 connection rollback failed", [primary, cleanup_error]) from None
            raise
        self.log.info(f"Connected to bucket <c>{self._config.bucket}</c> in region <c>{self._config.region}</c>")

    @override
    async def close(self) -> None:
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            # Use list_objects instead of head_bucket to work with minimal
            # IAM policies (head_bucket requires GetBucket permission).
            async with contextlib.aclosing(self._client.list_objects(max_keys=1)) as agen:
                await anext(agen, None)
        except Exception:
            return False
        else:
            return True

    def _ensure_client(self) -> AsyncS3Client:
        if self._client is None:
            raise RuntimeError("Client is not connected.")
        return self._client

    def _remote_path_to_key(self, remote_path: PathLike) -> str:
        path = self.normalize_path(remote_path)
        relative = path.relative_to("/")
        return relative.as_posix() if relative != PurePosixPath(".") else ""

    def _dir_key(self, path: PathLike) -> str | None:
        """返回目录标记对象的 S3 键。

        目录标记对象使用尾随 ``/`` 的键存储。
        根目录（``""``）没有标记对象，返回 ``None``。
        """
        key = self._remote_path_to_key(path)
        if key == "":
            return None
        return key + "/"
