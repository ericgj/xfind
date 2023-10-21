import os.path
from xfind.__main__ import build_cli, args_to_config


FIXTURE_ROOT = os.path.join("test", "fixture", "test_config")


def test_flag_defaults():
    cli = build_cli()
    args = cli.parse_args(["-c", fixture_file("defaults.toml")])
    config, _ = args_to_config(args)

    assert config.find_files == True
    assert config.find_dirs == True


def test_flag_defaults_override():
    cli = build_cli()
    args = cli.parse_args(
        [
            "-c",
            fixture_file("defaults.toml"),
            "--no-files",
            "--no-dirs",
        ]
    )
    config, _ = args_to_config(args)

    assert config.find_files == False
    assert config.find_dirs == False


def test_flag_specified():
    cli = build_cli()
    args = cli.parse_args(["-c", fixture_file("no_files.toml")])
    config, _ = args_to_config(args)

    assert config.find_files == False
    assert config.find_dirs == True


def test_flag_specified_override():
    cli = build_cli()
    args = cli.parse_args(["-c", fixture_file("no_files.toml"), "--files"])
    config, _ = args_to_config(args)

    assert config.find_files == True
    assert config.find_dirs == True


def fixture_file(fname: str) -> str:
    return os.path.join(FIXTURE_ROOT, fname)
