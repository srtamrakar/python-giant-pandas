import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), "README.md"), encoding="utf-8") as f:
    long_description = f.read()

module_version = "0.1.8"

setup(
    name="GiantPandas",
    packages=["GiantPandas"],
    version=module_version,
    license="MIT",
    description="Convenient functions and connectors for Pandas.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Samyak Ratna Tamrakar",
    author_email="samyak.r.tamrakar@gmail.com",
    url="https://github.com/srtamrakar/python-giant-pandas",
    download_url=f"https://github.com/srtamrakar/python-giant-pandas/archive/v_{module_version}.tar.gz",
    keywords=["pandas", "excel", "postgres", "psql", "postgresql", "redshift", "s3"],
    install_requires=[
        "boto3>=1.12.37",
        "numpy>=1.13.3",
        "pandas>=0.25.0",
        "pytest>=5.0.1",
        "psycopg2-binary>=2.8.3",
        "Unidecode>=1.0.22",
        "xlrd>=1.2.0",
        "XlsxWriter>=1.0.2",
        "FreqObjectOps>=0.1.4",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)
