class Process:
    def __init__(self, pid: int) -> None:
        ...
    def memory_info(self) -> MemoryInfo:
        ...

class MemoryInfo:
    rss: int
    vsm: int
