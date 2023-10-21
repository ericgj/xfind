from fnmatch import fnmatch
from itertools import chain, product
import logging
import os.path
import re
from typing import List

from xfind.__main__ import main

FIXTURE_ROOT = os.path.join("test", "fixture", "test_command")


def test_single_thread_stop_after(caplog):
    caplog.set_level(logging.DEBUG)
    main_sleep(
        2,
        stop_after="2s",
        glob=os.path.join(FIXTURE_ROOT, "test_single_thread_stop_after", "*"),
    )

    # Note: assumes fixture dir has > 2 files in it

    assert len(caplog.records) > 0, "No log records emitted"
    assert all(
        rec.levelno < logging.WARNING for rec in caplog.records
    ), "Warnings/errors found in log"

    dur = caplog.records[-1].created - caplog.records[0].created
    assert dur <= 3, f"Execution time was {dur} seconds, expected <= 3"


def test_multi_thread_ends_before_stop_after(caplog):
    caplog.set_level(logging.DEBUG)

    fixture_path = os.path.join(
        FIXTURE_ROOT, "test_multi_thread_ends_before_stop_after"
    )
    expected_total = len(os.listdir(fixture_path))

    main_sleep(
        2,
        stop_after="10s",
        concurrency=10,
        glob=os.path.join(fixture_path, "*"),
    )

    assert len(caplog.records) > 0, "No log records emitted"
    assert all(
        rec.levelno < logging.WARNING for rec in caplog.records
    ), "Warnings/errors found in log"

    dur = caplog.records[-1].created - caplog.records[0].created
    assert dur <= 10, f"Execution time was {dur} seconds, expected <= 10"

    last_msg = caplog.records[-1].message
    assert (
        re.search(f"{expected_total} total files processed", last_msg) is not None
    ), f"Expected log to report {expected_total} total files, was {last_msg}"


def test_multi_thread_stopped_after(caplog):
    caplog.set_level(logging.DEBUG)

    fixture_path = os.path.join(FIXTURE_ROOT, "test_multi_thread_stopped_after")
    expected_total = 6  # note may be off by one due to timing issues!

    main_sleep(
        2,
        stop_after="3s",
        concurrency=2,
        glob=os.path.join(fixture_path, "*"),
    )

    assert len(caplog.records) > 0, "No log records emitted"
    assert all(
        rec.levelno < logging.WARNING for rec in caplog.records
    ), "Warnings/errors found in log"

    dur = caplog.records[-1].created - caplog.records[0].created
    assert dur <= 10, f"Execution time was {dur} seconds, expected <= 10"

    last_msg = caplog.records[-1].message
    assert (
        re.search(f"{expected_total} total files processed", last_msg) is not None
    ), f"Expected log to report {expected_total} total files, was {last_msg}"


def test_omits(caplog):
    caplog.set_level(logging.DEBUG)

    fixture_path = os.path.join(FIXTURE_ROOT, "test_omits")
    expected_total = 6
    omits = [
        os.path.join(fixture_path, "omit*"),
        os.path.join(fixture_path, "**", "*.omit"),
    ]

    main_sleep(
        1,
        stop_after="2s",
        glob=os.path.join(fixture_path, "**"),
        omits=omits,
    )

    assert len(caplog.records) > 0, "No log records emitted"
    assert all(
        rec.levelno < logging.WARNING for rec in caplog.records
    ), "Warnings/errors found in log"

    last_msg = caplog.records[-1].message
    assert (
        re.search(f"{expected_total} total files processed", last_msg) is not None
    ), f"Expected log to report {expected_total} total files, was {last_msg}"

    found = [
        m[1]
        for r in caplog.records
        if (m := re.search(r"Found: (.*)", r.message)) is not None
    ]
    assert (
        len(found) == expected_total
    ), f"Expected {expected_total} log records for found files, was {len(found)}"

    match_omits = [f for (f, o) in product(found, omits) if fnmatch(f, o)]
    assert (
        len(match_omits) == 0
    ), f"Expected no found files to match omit patterns, but found: {match_omits}"


def test_omits_no_dirs(caplog):
    caplog.set_level(logging.DEBUG)

    fixture_path = os.path.join(FIXTURE_ROOT, "test_omits")
    expected_total = 1
    omits = [
        os.path.join(fixture_path, "omit*"),
        os.path.join(fixture_path, "**", "*.omit"),
    ]

    main_sleep(
        1,
        stop_after="2s",
        glob=os.path.join(fixture_path, "**"),
        omits=omits,
        find_dirs=False,
    )

    assert len(caplog.records) > 0, "No log records emitted"
    assert all(
        rec.levelno < logging.WARNING for rec in caplog.records
    ), "Warnings/errors found in log"

    last_msg = caplog.records[-1].message
    assert (
        re.search(f"{expected_total} total files processed", last_msg) is not None
    ), f"Expected log to report {expected_total} total files, was {last_msg}"


def test_no_omits_no_dirs(caplog):
    caplog.set_level(logging.DEBUG)

    fixture_path = os.path.join(FIXTURE_ROOT, "test_omits")
    expected_total = 3
    omits: List[str] = []

    main_sleep(
        1,
        stop_after="2s",
        glob=os.path.join(fixture_path, "**"),
        omits=omits,
        find_dirs=False,
    )

    assert len(caplog.records) > 0, "No log records emitted"
    assert all(
        rec.levelno < logging.WARNING for rec in caplog.records
    ), "Warnings/errors found in log"

    last_msg = caplog.records[-1].message
    assert (
        re.search(f"{expected_total} total files processed", last_msg) is not None
    ), f"Expected log to report {expected_total} total files, was {last_msg}"


def test_omits_no_files(caplog):
    caplog.set_level(logging.DEBUG)

    fixture_path = os.path.join(FIXTURE_ROOT, "test_omits")
    expected_total = 5
    omits = [
        os.path.join(fixture_path, "omit*"),
        os.path.join(fixture_path, "**", "*.omit"),
    ]

    main_sleep(
        1,
        stop_after="2s",
        glob=os.path.join(fixture_path, "**"),
        omits=omits,
        find_files=False,
    )

    assert len(caplog.records) > 0, "No log records emitted"
    assert all(
        rec.levelno < logging.WARNING for rec in caplog.records
    ), "Warnings/errors found in log"

    last_msg = caplog.records[-1].message
    assert (
        re.search(f"{expected_total} total files processed", last_msg) is not None
    ), f"Expected log to report {expected_total} total files, was {last_msg}"


def test_no_omits_no_files(caplog):
    caplog.set_level(logging.DEBUG)

    fixture_path = os.path.join(FIXTURE_ROOT, "test_omits")
    expected_total = 8
    omits: List[str] = []

    main_sleep(
        1,
        stop_after="2s",
        glob=os.path.join(fixture_path, "**"),
        omits=omits,
        find_files=False,
    )

    assert len(caplog.records) > 0, "No log records emitted"
    assert all(
        rec.levelno < logging.WARNING for rec in caplog.records
    ), "Warnings/errors found in log"

    last_msg = caplog.records[-1].message
    assert (
        re.search(f"{expected_total} total files processed", last_msg) is not None
    ), f"Expected log to report {expected_total} total files, was {last_msg}"


def main_sleep(
    secs, *, stop_after, glob, omits=[], find_files=True, find_dirs=True, concurrency=1
):
    main(
        [
            "-x",
            f"ping -n {secs} 127.0.0.1",
            "-n",
            str(concurrency),
            "--stop-after",
            str(stop_after),
            "-p",
            glob,
        ]
        + list(chain.from_iterable([["-o", o] for o in omits]))
        + (["--no-files"] if not find_files else [])
        + (["--no-dirs"] if not find_dirs else [])
    )
