from typing import Iterable, IO


class Client:
    def bucket(self, bucket_name: str) -> Bucket:
        ...


class Bucket:
    def blob(self, key: str) -> Blob:
        ...
    def list_blobs(self, prefix: str) -> Iterable[Blob]:
        ...
    name: str


class Blob:
    def upload_from_file(self, file: IO[bytes], rewind: bool = ...) -> None:
        ...
    def download_as_string(self) -> bytes:
        ...
    def exists(self) -> bool:
        ...
    def delete(self) -> None:
        ...
    bucket: Bucket
    name: str
