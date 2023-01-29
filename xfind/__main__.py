from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, Future, CancelledError
from glob import iglob
import logging.config
import logging
import os.path
import shlex
import subprocess
import sys
from time import time, sleep
from typing import Optional, List

from .adapter import config_file
from .model.config import Config
from .util.datetime import utc_from_posix, from_posix
from .util.itertools import chunk


def main(argv: List[str] = sys.argv[1:]):
    t = time()

    cli = ArgumentParser(
        prog="xfind", description="Execute commands concurrently on searched files"
    )
    cli.add_argument("-c", "--config", help="Config file")
    cli.add_argument("-x", "--command", help="Command pattern")
    cli.add_argument("-n", "--concurrency", type=int, help="Concurrency")
    cli.add_argument(
        "--stop-after",
        type=config_file.parse_duration,
        help="Stop cleanly after specified time, e.g. 2h30m",
    )
    cli.add_argument("--root-dir", help="Root directory of file search")
    cli.add_argument("glob", help="File pattern (glob)")

    args = cli.parse_args(argv)

    config: Config
    logging_config: Optional[dict]
    if args.config is None:
        config = Config.from_args(vars(args))
        logging_config = None
    else:
        config, logging_config = config_file.parse_file(args.config)
        config = config.merge_args(vars(args))

    if logging_config is None:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(levelname)-1s | %(asctime)s | %(name)s | %(module)s | %(threadName)s | %(message)s",
        )
    else:
        logging.config.dictConfig(logging_config)

    run(config, t)


def run(config: Config, timestamp: float):
    logger = logging.getLogger()
    executor = ThreadPoolExecutor(max_workers=config.concurrency)
    pattern = os.path.join(config.root_dir, config.glob)
    stop_time = (
        None
        if config.stop_after is None
        else timestamp + config.stop_after.total_seconds()
    )

    if config.stop_after is not None:
        logger.info(f"Note: stopping cleanly after {config.stop_after}")

    for source_files in chunk(iglob(pattern, recursive=True), config.concurrency):
        for f in source_files:
            logger.debug(f"Found: {f}")
        f_map = {
            executor.submit(
                run_in_subprocess, render_command(config.command, f, timestamp)
            ): f
            for f in source_files
        }
        for future in f_map:
            future.add_done_callback(ProcessCallback(f_map[future], logger=logger))

    if config.stop_after is None:
        # Wait for running futures and all pending futures to finish
        logger.info("Waiting for all tasks to complete")
        executor.shutdown(wait=True, cancel_futures=False)

    else:
        # Let the workers work until stop_after has elapsed
        # The problem is, what if all tasks are done before this?
        remain = stop_time - time()
        while remain > 0:
            sleep(1)
            remain = stop_time - time()

        # Wait for running futures, but cancel all pending futures
        logger.info("Waiting for currently running tasks to complete")
        executor.shutdown(wait=True, cancel_futures=True)

    logger.info("Done.")


def render_command(command: str, source_file: str, timestamp: float) -> List[str]:
    root, ext = os.path.splitext(source_file)
    data = {
        "file_name": source_file,
        "base_name": os.path.basename(source_file),
        "dir_name": os.path.dirname(source_file),
        "root_name": root,
        "ext_name": ext,
        "timestamp": from_posix(timestamp),
        "utc_timestamp": utc_from_posix(timestamp),
    }
    return shlex.split(command.format(**data))


def run_in_subprocess(command: List[str]) -> subprocess.CompletedProcess:
    logger = logging.getLogger()
    logger.debug(f"Running: `{shlex.join(command)}`")
    return subprocess.run(
        command, capture_output=True, encoding="utf-8", errors="ignore"
    )


class ProcessCallback:
    def __init__(self, source_file: str, logger: logging.Logger):
        self.source_file = source_file
        self.logger = logger

    def __call__(self, future: Future):
        logger = self.logger
        result: subprocess.CompletedResult
        try:
            result = future.result()

        except CancelledError:
            logger.info(f"Task cancelled for {self.source_file}")
            return

        except Exception as e:
            # Rare, but log if any other error
            logger.warning(f"Error running task for {self.source_file}: {e}")
            logger.exception(e)
            return

        if result.returncode > 0:
            # log subprocess error, including stdout and stderr
            logger.error(f"Failure ({result.returncode}): `{shlex.join(result.args)}`")
            if result.stdout is not None and len(result.stdout) > 0:
                logger.info(f"STDOUT:\n-------\n{result.stdout}")
            if result.stderr is not None and len(result.stderr) > 0:
                logger.info(f"STDERR:\n-------\n{result.stderr}")
        else:
            logger.info(f"Success ({result.returncode}): `{shlex.join(result.args)}`")


if __name__ == "__main__":
    main()