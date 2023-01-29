from itertools import islice
from typing import TypeVar, Iterator, List

A = TypeVar("A")


def chunk(it: Iterator[A], n: int) -> Iterator[List[A]]:
    while c := list(islice(it, n)):
        yield c
