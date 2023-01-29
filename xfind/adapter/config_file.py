from datetime import timedelta
from typing import TypeVar, Tuple, Optional, cast

import durationpy  # type: ignore

try:
    import tomllib
except ImportError:
    import tomli as tomllib


from ..model.config import Config
from ..util.dict_ import assert_has_field

TOP = "xfind"
LOGGING = "logging"


def parse_file(file_name: str) -> Tuple[Config, Optional[dict]]:
    with open(file_name, "rb") as f:
        return parse(tomllib.load(f))


def parse(raw: dict) -> Tuple[Config, Optional[dict]]:
    assert_has_field(TOP, "config", raw)
    top = raw[TOP]
    args = {
        "pattern": parse_optional_string(top, "pattern"),
        "root_dir": parse_optional_string(top, "root_dir"),
        "command": parse_optional_string(top, "command"),
        "concurrency": parse_optional_int(top, "concurrency"),
        "stop_after": parse_optional_duration(top, "stop_after"),
    }
    return (
        Config(**{k: args[k] for k in args if not args[k] is None}),
        parse_logging(raw),
    )


def parse_logging(raw) -> Optional[dict]:
    return strict(dict, LOGGING, raw[LOGGING]) if LOGGING in raw else None


def parse_optional_string(raw, key: str) -> Optional[str]:
    v = raw.get(key, None)
    return None if v is None else strict(str, key, v)


def parse_optional_int(raw, key: str) -> Optional[int]:
    v = raw.get(key, None)
    return None if v is None else strict(int, key, v)


def parse_optional_duration(raw, key: str) -> Optional[timedelta]:
    v = raw.get(key, None)
    return None if v is None else parse_duration(str(v))


def parse_duration(s: str) -> timedelta:
    return cast(timedelta, durationpy.from_str(s))


# TODO: move

T = TypeVar("T")


def strict(t: T, label: str, value) -> T:
    if not isinstance(value, t):
        raise ValueError(f"Value of {label} is not a {t.__name__}")
    cast(T, value)
    return value
