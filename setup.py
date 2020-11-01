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


dependencies_list = [
    "boto3>=1.16.9",
    "botocore>=1.19.9",
    "numpy>=1.19.3",
    "pandas>=1.1.4",
    "pytest>=6.1.2",
    "psycopg2-binary>=2.8.6",
    "Unidecode>=1.1.1",
    "xlrd>=1.2.0",
    "XlsxWriter>=1.3.7",
    "FreqObjectOps>=0.1.5",
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
    url="https://github.com/srtamrakar/python-giant-pandas",
    download_url=f"https://github.com/srtamrakar/python-giant-pandas/archive/v_{get_version()}.tar.gz",
    keywords=["pandas", "excel", "postgres", "psql", "postgresql", "redshift", "s3"],
    install_requires=dependencies_list,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
