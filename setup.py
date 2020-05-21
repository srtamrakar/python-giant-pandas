import os
import re
from setuptools import setup, find_packages

ROOT = os.path.dirname(__file__)
MODULE_NAME = "GiantPandas"


def get_author() -> str:
    author_re = re.compile(r"""__author__ = ['"]([A-Za-z .]+)['"]""")
    init = open(os.path.join(ROOT, MODULE_NAME, "__init__.py")).read()
    return author_re.search(init).group(1)


def get_version() -> str:
    version_re = re.compile(r"""__version__ = ['"]([0-9.]+)['"]""")
    init = open(os.path.join(ROOT, MODULE_NAME, "__init__.py")).read()
    return version_re.search(init).group(1)


def get_description() -> str:
    with open(os.path.join(ROOT, "README.md"), encoding="utf-8") as f:
        description = f.read()
    return description


REQUIRED_LIBRARIES = [
    "boto3>=1.12.37",
    "botocore>=1.15.39",
    "numpy>=1.13.3",
    "pandas>=0.25.0",
    "pytest>=5.0.1",
    "psycopg2-binary>=2.8.3",
    "Unidecode>=1.0.22",
    "xlrd>=1.2.0",
    "XlsxWriter>=1.0.2",
    "FreqObjectOps>=0.1.4",
]


setup(
    name=MODULE_NAME,
    packages=find_packages(),
    version=get_version(),
    license="MIT",
    description="Convenient functions and connectors for Pandas.",
    long_description=get_description(),
    long_description_content_type="text/markdown",
    author=get_author(),
    rl="https://github.com/srtamrakar/python-giant-pandas",
    download_url=f"https://github.com/srtamrakar/python-giant-pandas/archive/v_{get_version()}.tar.gz",
    keywords=["pandas", "excel", "postgres", "psql", "postgresql", "redshift", "s3"],
    install_requires=REQUIRED_LIBRARIES,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)
