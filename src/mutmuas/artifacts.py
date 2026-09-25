"""Artifacts: data lives outside the message bus; messages carry only ArtifactRefs.

URI schemes understood in Phase 1:

  artifact://<project>/<key>     NATS JetStream object store (no extra infra; chunked;
                                 fine up to a few GB). Default for publish.
  file://<node>/<abs/path>       a path on a specific node. Fetchable only where that path
                                 is visible (same node or shared filesystem). Zero-copy for huge data.
  http(s)://...                  plain download.
  git://<repo>@<rev>             reference only — the receiver checks out itself.

Backends are chosen by URI scheme, so S3/MinIO/DVC can be added later as
another scheme without touching the protocol.
"""

from __future__ import annotations

import hashlib
import mimetypes
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from nats.js import api
from nats.js.errors import NotFoundError, ObjectNotFoundError

from .bus import Bus
from .protocol import ArtifactRef

DIR_MEDIA_TYPE = "application/x-mutmuas-dir+tar.gz"


class ArtifactError(RuntimeError):
    pass


class ArtifactUnavailable(ArtifactError):
    pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_key(key: str) -> str:
    parts = [p for p in key.replace("\\", "/").split("/") if p not in ("", ".", "..")]
    if not parts:
        raise ArtifactError(f"invalid artifact key {key!r}")
    return "/".join(parts)


class ArtifactStore:
    def __init__(self, bus: Bus | None, project: str, node: str, max_mb: float = 2048):
        self.bus = bus
        self.project = project
        self.node = node
        self.max_bytes = int(max_mb * 1024 * 1024)

    # ---- publish ------------------------------------------------------

    async def publish(self, path: str | Path, key: str, *, id: str = "", description: str = "",
                      backend: str = "object") -> ArtifactRef:
        """Publish a file or directory. backend='object' uploads; backend='file' only references."""
        src = Path(path).expanduser().resolve()
        if not src.exists():
            raise ArtifactError(f"no such file or directory: {src}")
        if backend == "file":
            size = src.stat().st_size if src.is_file() else None
            return ArtifactRef(uri=f"file://{self.node}{src}", id=id or src.name, size=size,
                               sha256=sha256_file(src) if src.is_file() else None,
                               media_type="inode/directory" if src.is_dir() else _guess(src),
                               description=description)
        if backend != "object":
            raise ArtifactError(f"unknown backend {backend!r}")
        if self.bus is None:
            raise ArtifactError("object backend needs a bus connection")

        tmp: Path | None = None
        media_type = _guess(src)
        if src.is_dir():
            fd, name = tempfile.mkstemp(suffix=".tar.gz")
            tmp = Path(name)
            with open(fd, "wb", closefd=True) as raw, tarfile.open(fileobj=raw, mode="w:gz") as tar:
                tar.add(src, arcname=src.name)
            upload, media_type = tmp, DIR_MEDIA_TYPE
        else:
            upload = src
        try:
            size = upload.stat().st_size
            if size > self.max_bytes:
                raise ArtifactError(f"{src} is {size / 2**20:.0f} MiB, above the object-store cap "
                                    f"({self.max_bytes / 2**20:.0f} MiB); publish with backend='file' instead")
            digest = sha256_file(upload)
            key = _safe_key(key)
            with open(upload, "rb") as f:
                # No description in the shared store (every node can list it): it travels in the ArtifactRef
                # inside the participants' messages instead. The bytes themselves are a step-2 risk.
                await self.bus.objects.put(key, f, meta=api.ObjectMeta(
                    name=key, description=None,
                    headers={"Mutmuas-Sha256": digest, "Mutmuas-Media-Type": media_type or ""}))
        finally:
            if tmp:
                tmp.unlink(missing_ok=True)
        return ArtifactRef(uri=f"artifact://{self.project}/{key}", id=id or src.name, size=size,
                           sha256=digest, media_type=media_type, description=description)

    # ---- fetch --------------------------------------------------------

    async def fetch(self, ref: ArtifactRef | str, dest_dir: str | Path) -> Path:
        """Materialise an artifact locally under ``dest_dir``; returns the file/dir path."""
        if isinstance(ref, str):
            ref = ArtifactRef(uri=ref)
        dest_dir = Path(dest_dir).expanduser().resolve()
        dest_dir.mkdir(parents=True, exist_ok=True)
        url = urlparse(ref.uri)
        if url.scheme == "artifact":
            path = await self._fetch_object(url.netloc, url.path.lstrip("/"), dest_dir)
        elif url.scheme == "file":
            path = self._fetch_file(url.netloc, url.path, dest_dir)
        elif url.scheme in ("http", "https"):
            path = dest_dir / (Path(url.path).name or "download")
            try:
                with urllib.request.urlopen(ref.uri, timeout=60) as r, open(path, "wb") as f:
                    shutil.copyfileobj(r, f)
            except OSError as e:
                raise ArtifactUnavailable(f"download failed for {ref.uri}: {e}") from e
        elif url.scheme == "git":
            raise ArtifactUnavailable(f"{ref.uri} is a git reference; fetch/checkout the revision yourself")
        else:
            raise ArtifactUnavailable(f"unsupported artifact scheme in {ref.uri!r}")

        if ref.sha256 and path.is_file() and sha256_file(path) != ref.sha256:
            path.unlink(missing_ok=True)
            raise ArtifactError(f"checksum mismatch for {ref.uri}")
        if ref.media_type == DIR_MEDIA_TYPE or path.name.endswith(".mmdir.tar.gz"):
            path = _extract(path, dest_dir)
        return path

    async def _fetch_object(self, project: str, key: str, dest_dir: Path) -> Path:
        if self.bus is None:
            raise ArtifactError("object backend needs a bus connection")
        if project != self.project:
            raise ArtifactUnavailable(f"artifact belongs to project {project!r}, this node is in {self.project!r}")
        try:
            info = await self.bus.objects.get_info(key)
        except (ObjectNotFoundError, NotFoundError) as e:
            raise ArtifactUnavailable(f"artifact://{project}/{key} not found (deleted or never published)") from e
        headers = (info.meta and info.meta.headers) or {}
        is_dir = headers.get("Mutmuas-Media-Type") == DIR_MEDIA_TYPE
        target = dest_dir / (Path(key).name + (".mmdir.tar.gz" if is_dir else ""))
        with open(target, "wb") as f:
            await self.bus.objects.get(key, writeinto=f)
        expected = headers.get("Mutmuas-Sha256")
        if expected and sha256_file(target) != expected:
            target.unlink(missing_ok=True)
            raise ArtifactError(f"checksum mismatch for artifact://{project}/{key}")
        return target

    def _fetch_file(self, node: str, path: str, dest_dir: Path) -> Path:
        src = Path(path)
        if not src.exists():
            where = "this node" if node in ("", self.node) else f"node {node}"
            raise ArtifactUnavailable(
                f"file://{node}{path} is not visible here (it lives on {where}); ask the owner to "
                "publish it with the object backend, or mount a shared filesystem")
        target = dest_dir / src.name
        if src.resolve() == target.resolve():
            return target
        if src.is_dir():
            shutil.copytree(src, target, dirs_exist_ok=True)
        else:
            shutil.copy2(src, target)
        return target

    async def list(self) -> list[dict]:
        if self.bus is None:
            return []
        try:
            infos = await self.bus.objects.list()
        except NotFoundError:
            return []
        return [{"uri": f"artifact://{self.project}/{i.name}", "size": i.size, "modified": str(i.mtime or ""),
                 "description": (i.description or "")} for i in infos if not i.deleted]


def _guess(path: Path) -> str | None:
    return mimetypes.guess_type(path.name)[0]


def _extract(archive: Path, dest_dir: Path) -> Path:
    with tarfile.open(archive, "r:gz") as tar:
        names = tar.getnames()
        root = names[0].split("/")[0] if names else archive.stem
        try:
            tar.extractall(dest_dir, filter="data")
        except TypeError:  # Python without tarfile extraction filters
            tar.extractall(dest_dir)
    archive.unlink(missing_ok=True)
    return dest_dir / root

