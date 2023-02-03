import logging
import os.path
import re

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


def main_sleep(secs, *, stop_after, glob, concurrency=1):
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
    )
