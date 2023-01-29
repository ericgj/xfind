from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Optional


@dataclass
class Config:
    root_dir: str = ""
    pattern: str = "*"
    command: str = 'echo "{file_name}"'
    concurrency: int = 1
    stop_after: Optional[timedelta] = None

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
