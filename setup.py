from setuptools import setup

install_requires = [
    "durationpy",
]
try:
    import tomllib  # noqa
except ImportError:
    install_requires.append("tomli")

tests_require = ["pytest"]

setup(
    name="xfind",
    version="0.1",
    description="concurrent find files + exec",
    license="MIT",
    author="Eric Gjertsen",
    email="ericgj72@gmail.com",
    packages=[
        "xfind",
        "xfind.adapter",
        "xfind.model",
        "xfind.util",
    ],
    entry_points={"console_scripts": ["xfind = xfind.__main__:main"]},
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={"test": tests_require},  # to make pip happy
    zip_safe=False,  # to make mypy happy
)
