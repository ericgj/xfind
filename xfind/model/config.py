from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Optional, List


@dataclass
class Config:
    root_dir: str = ""
    pattern: str = "*"
    omits: List[str] = field(default_factory=list)
    command: str = 'echo "{file_name}"'
    concurrency: int = 1
    stop_after: Optional[timedelta] = None
    limit: Optional[int] = None
    stdout: bool = False
    stderr: bool = False
    shell: bool = False

    @classmethod
    def from_args(cls, args) -> "Config":
        return cls(
            **{k: args[k] for k in args if not k == "config" and args[k] is not None}
        )

    def merge_args(self, args) -> "Config":
        return replace(
            self,
            **{k: args[k] for k in args if not k == "config" and args[k] is not None}
        )
