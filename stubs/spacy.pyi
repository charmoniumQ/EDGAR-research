from typing import Callable, Iterator


def load(lang: str) -> Nlp:
    ...

class Nlp:
    def __init__(self, model: str) -> None:
        ...
    def __call__(self, input_doc: str) -> Doc:
        ...

class Doc:
    def __iter__(self) -> Iterator[Token]:
        ...
    ents: Iterator[Span]
    sents: Iterator[Span]

class Span:
    def __iter__(self) -> Iterator[Token]:
        ...

class Token:
    lemma_: str
    orth_: str
    tag_: str
    is_stop: bool
    is_alpha: bool
