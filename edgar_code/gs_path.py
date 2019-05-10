from typing import Iterable, Union, Dict, Any, IO, Optional
from pathlib import PurePath
from urllib.parse import urlparse
import io
from typing_extensions import Protocol
from google.cloud.storage import Blob, Bucket, Client
# https://googleapis.github.io/google-cloud-python/latest/storage/client.html


# TODO: config
# export GOOGLE_APPLICATION_CREDENTIALS=
# python3 -m edgar_code.executables.get_all_rfs


class PathLike(Protocol):
    # pylint: disable=no-self-use,unused-argument
    def __truediv__(self, other: Union[str, PathLike]) -> PathLike:
        ...
    def mkdir(self) -> None:
        ...
    def exists(self) -> bool:
        ...
    def unlink(self) -> None:
        ...
    def iterdir(self) -> Iterable[PathLike]:
        ...
    # pylint: disable=too-many-arguments
    def open(self, mode: str = 'r', buffering: int = 0, encoding: Optional[str] = None,
             errors: Optional[str] = None, newline: Optional[str] = None) -> IO[Any]:
        ...


class GSPath:
    path: PurePath
    bucket: Bucket
    blob: Blob

    @classmethod
    def from_blob(cls, blob: Blob):
        return cls(blob.bucket, blob.name)

    @classmethod
    def from_url(cls, url_str: str):
        url = urlparse(url_str)
        if url.scheme != 'gs':
            raise ValueError('Wrong url scheme')
        return cls(url.netloc, url.path[1:])

    def __init__(self, bucket: Union[str, Bucket], path: Union[PathLike, str]):
        self.path = PurePath(str(path))
        if isinstance(bucket, str):
            bucket = Client().bucket(bucket)
        self.bucket = bucket
        self.blob = self.bucket.blob(str(self.path))

    def __getstate__(self):
        return {'path': self.path, 'bucket': self.bucket.name}

    def __setstate__(self, data: Dict[str, Any]):
        self.path = data['path']
        self.bucket = Client().bucket(data['bucket'])
        self.blob = self.bucket.blob(str(self.path))

    # Otherwise pylint thinks GSPath is undefined
    # pylint: disable=undefined-variable
    def __truediv__(self, other: Union[str, PathLike]) -> GSPath:
        return GSPath(self.bucket, self.path / str(other))

    def __repr__(self):
        url = f'gs://{self.bucket.name}/{self.path}'
        return f'GSPath.from_url({url!r})'

    @property
    def parent(self) -> GSPath:
        return GSPath(self.bucket, self.path.parent)

    def mkdir(self, exist_ok=True, parents=True) -> None:
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

    # pylint: disable=too-many-arguments,unused-argument
    def open(self, mode: str = 'r', buffering: int = 0,
             encoding: Optional[str] = 'UTF-8',
             errors: Optional[str] = 'strict',
             newline: Optional[str] = '\n') -> IO[Any]:
        # cast away the Optional
        encoding = encoding if encoding is not None else 'UTF-8'
        if 'w' in mode:
            if 'b' in mode:
                return WGSFile(self, mode)
            else:
                return io.TextIOWrapper(
                    WGSFile(self, mode), encoding=encoding, errors=errors, newline=newline
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
    def __init__(self, gs_path: GSPath, _: str):
        super().__init__()
        self.gs_path = gs_path

    def close(self):
        self.gs_path.blob.upload_from_file(self, rewind=True)
        super().close()


path_attrs = ['__truediv__', 'open', 'parent']
def pathify(path: Union[str, PathLike]) -> PathLike:
    if isinstance(path, str):
        return PurePath(path)
    elif all(hasattr(path, attr) for attr in path_attrs):
        return path
    else:
        raise TypeError()


def copy(in_path: PathLike, out_path: PathLike):
    with pathify(in_path).open('rb') as fin:
        with pathify(out_path).open('wb') as fout:
            fout.write(fin.read())
