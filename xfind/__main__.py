from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, Future, CancelledError
from glob import iglob
from itertools import islice
import logging.config
import logging
import os.path
from queue import Queue, Empty
import shlex
import subprocess
import sys
from time import time, sleep
from typing import Optional, List

from .adapter import config_file
from .model.config import Config
from .util.datetime import utc_from_posix, from_posix
from .util.itertools import chunk

APP_NAME = "xfind"


def main(argv: List[str] = sys.argv[1:]):
    t = time()

    cli = ArgumentParser(
        prog=APP_NAME, description="Execute commands concurrently on searched files"
    )
    cli.add_argument("-c", "--config", help="Config file")
    cli.add_argument("-p", "--pattern", help="File pattern (glob)")
    cli.add_argument("-x", "--command", help="Command pattern")
    cli.add_argument("-n", "--concurrency", type=int, help="Concurrency")
    cli.add_argument(
        "--stop-after",
        type=config_file.parse_duration,
        help="Stop cleanly after specified time, e.g. 2h30m",
    )
    cli.add_argument(
        "--limit", type=int, help="Limit number of files (per concurrent task thread)"
    )
    cli.add_argument("--root-dir", help="Root directory of file search")
    cli.add_argument(
        "--stdout", default=False, action="store_true", help="Relay task out to stdout"
    )
    cli.add_argument(
        "--stderr", default=False, action="store_true", help="Relay task err to stderr"
    )
    cli.add_argument(
        "--shell", default=False, action="store_true", help="Run in DOS shell"
    )

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
    logger = logging.getLogger(APP_NAME)
    executor = ThreadPoolExecutor(max_workers=config.concurrency)
    queue = Queue()

    stop_time = (
        None
        if config.stop_after is None
        else timestamp + config.stop_after.total_seconds()
    )
    if config.stop_after is not None:
        logger.info(f"Note: stopping cleanly after {config.stop_after}")

    total_files = 0
    total_processed = 0
    pattern = os.path.join(config.root_dir, config.pattern)

    finder = chunk(iglob(pattern, recursive=True), config.concurrency)
    if config.limit is not None:
        finder = islice(finder, config.limit)

    for source_files in finder:
        for f in source_files:
            total_files += 1
            logger.debug(f"Found: {f}")
        f_map = {
            executor.submit(
                run_in_subprocess,
                render_command(config.command, f, timestamp),
                shell=config.shell,
                relay_stdout=config.stdout,
                relay_stderr=config.stderr,
            ): f
            for f in source_files
        }
        for future in f_map:
            future.add_done_callback(
                ProcessCallback(f_map[future], queue=queue, logger=logger)
            )

    if config.stop_after is None:
        # Wait for running futures and all pending futures to finish
        logger.info("Waiting for all tasks to complete")
        executor.shutdown(wait=True, cancel_futures=False)

    else:
        # Let the workers work until stop_after has elapsed
        # or until all found files have been processed
        remain = stop_time - time()
        while remain > 0 and total_processed < total_files:
            try:
                _ = queue.get_nowait()
                queue.task_done()
                total_processed += 1
            except Empty:
                sleep(0.1)
            remain = stop_time - time()

        # Wait for running futures, but cancel all pending futures
        logger.info("Waiting for currently running tasks to complete")
        executor.shutdown(wait=True, cancel_futures=True)

    # Count those completed in the shutdown
    while True:
        try:
            _ = queue.get_nowait()
            queue.task_done()
            total_processed += 1
        except Empty:
            break

    logger.info(f"Done: {total_processed} total files processed.")


def render_command(command: str, source_file: str, timestamp: float) -> List[str]:
    file_name = os.path.abspath(source_file)
    root, ext = os.path.splitext(file_name)
    data = {
        "file_name": file_name,
        "base_name": os.path.basename(file_name),
        "dir_name": os.path.dirname(file_name),
        "root_name": root,
        "ext_name": ext,
        "timestamp": from_posix(timestamp),
        "utc_timestamp": utc_from_posix(timestamp),
    }
    return shlex.split(command.format(**data))


def run_in_subprocess(
    command: List[str],
    *,
    shell: bool,
    relay_stdout: bool,
    relay_stderr: bool,
) -> subprocess.CompletedProcess:
    logger = logging.getLogger(APP_NAME)
    logger.debug(f"Running: `{shlex.join(command)}`")
    return subprocess.run(
        command,
        shell=shell,
        stdout=sys.stdout if relay_stdout else subprocess.PIPE,
        stderr=sys.stderr if relay_stderr else subprocess.PIPE,
        encoding="utf-8",
        errors="ignore",
    )


class ProcessCallback:
    def __init__(self, source_file: str, queue: Queue, logger: logging.Logger):
        self.source_file = source_file
        self.queue = queue
        self.logger = logger

    def __call__(self, future: Future):
        logger = self.logger
        result: subprocess.CompletedResult
        try:
            result = future.result()

        except CancelledError:
            logger.debug(f"Task cancelled for {self.source_file}")
            return

        except Exception as e:
            # Rare, but log if any other error
            logger.warning(f"Error running task for {self.source_file}: {e}")
            logger.exception(e)
            return

        self.queue.put(None)  # notify process done
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
