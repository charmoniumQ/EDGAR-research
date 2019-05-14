from __future__ import annotations
from typing import Iterable, Union, Dict, Any, IO, TYPE_CHECKING
from pathlib import PurePath, Path
from urllib.parse import urlparse
import io
# https://googleapis.github.io/google-cloud-python/latest/storage/client.html
from google.cloud.storage import Blob, Bucket, Client
try:
    from edgar_code.types import PathLike
except ImportError:
    if not TYPE_CHECKING:
        # this makes this module work ouside of edgar_code package.
        # I want to use this module in edgar_deploy
        PathLike = None


class GSPath:
    path: PurePath
    bucket: Bucket
    blob: Blob

    @classmethod
    def from_blob(cls, blob: Blob) -> GSPath:
        return cls(blob.bucket, blob.name)

    @classmethod
    def from_url(cls, url_str: str) -> GSPath:
        url = urlparse(url_str)
        if url.scheme != 'gs':
            raise ValueError('Wrong url scheme')
        return cls(url.netloc, url.path[1:])

    def __init__(
            self, bucket: Union[str, Bucket], path: Union[PurePath, str]
    ) -> None:
        self.path = PurePath(str(path))
        if isinstance(bucket, str):
            bucket = Client().bucket(bucket)
        self.bucket = bucket
        self.blob = self.bucket.blob(str(self.path))

    def __getstate__(self) -> Dict[str, Any]:
        return {'path': self.path, 'bucket': self.bucket.name}

    def __setstate__(self, data: Dict[str, Any]) -> None:
        self.path = data['path']
        self.bucket = Client().bucket(data['bucket'])
        self.blob = self.bucket.blob(str(self.path))

    # Otherwise pylint thinks GSPath is undefined
    # pylint: disable=undefined-variable
    def __truediv__(self, other: Union[str, PurePath]) -> GSPath:
        return GSPath(self.bucket, self.path / str(other))

    def __repr__(self) -> str:
        url = f'gs://{self.bucket.name}/{self.path}'
        return f'GSPath.from_url({url!r})'

    @property
    def parent(self) -> GSPath:
        return GSPath(self.bucket, self.path.parent)

    def mkdir(
            self, mode: int = 0, parents: bool = True, exist_ok: bool = True
    ) -> None:
        # no notion of 'directories' in GS
        pass

    def rmtree(self) -> None:
        for path in self.iterdir():
            path.unlink()

    def exists(self) -> bool:
        # print(f'{self.blob.name} exists? {self.blob.exists()}')
        return self.blob.exists()

    def unlink(self) -> None:
        self.blob.delete()

    def iterdir(self) -> Iterable[GSPath]:
        for blob in self.bucket.list_blobs(prefix=f'{self.path!s}/'):
            yield GSPath.from_blob(blob)

    def open(
            self, mode: str = 'r', encoding: str = 'UTF-8', newline: str = '\n'
    ) -> IO[Any]:
        if 'w' in mode:
            if 'b' in mode:
                return WGSFile(self, mode)
            else:
                return io.TextIOWrapper(
                    WGSFile(self, mode), encoding=encoding, newline=newline
                )
        elif 'r' in mode:
            if 'b' in mode:
                return io.BytesIO(self.blob.download_as_string())
            else:
                return io.StringIO(
                    self.blob.download_as_string()
                    .decode(encoding=encoding), newline
                )
        else:
            raise RuntimeError(f'Flag {mode} not supported')


class WGSFile(io.BytesIO):
    def __init__(self, gs_path: GSPath, _: str) -> None:
        super().__init__()
        self.gs_path = gs_path

    def close(self) -> None:
        self.gs_path.blob.upload_from_file(self, rewind=True)
        super().close()


path_attrs = ['__truediv__', 'open', 'parent']
def is_pathlike(obj: Any) -> bool:
    return all(hasattr(obj, attr) for attr in path_attrs)


def pathify(path: Union[str, PathLike]) -> PathLike:
    if isinstance(path, str):
        return Path(path)
    elif is_pathlike(path):
        return path
    else:
        raise TypeError()


def copy(in_path: PathLike, out_path: PathLike) -> None:
    with pathify(in_path).open('rb') as fin:
        with pathify(out_path).open('wb') as fout:
            fout.write(fin.read())
