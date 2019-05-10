from typing import Callable


def load(lang: str) -> Callable[[str], Doc]:
    ...


class Doc:
    ...
