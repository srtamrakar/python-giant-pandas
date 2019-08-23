import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.md'), encoding = 'utf-8') as f:
	long_description = f.read()

setup(
	name = 'GiantPandas',
	packages = ['GiantPandas'],
	version = '0.1',
	license = 'MIT',
	description = 'Some special functions and connectors for Pandas.',
	long_description = long_description,
	long_description_content_type = 'text/markdown',
	author = 'Samyak Ratna Tamrakar',
	author_email = 'samyak.r.tamrakar@gmail.com',
	url = 'https://github.com/srtamrakar/python-giant-pandas',
	download_url = 'https://github.com/srtamrakar/python-giant-pandas/archive/v_0.1.tar.gz',
	keywords = ['pandas', 'excel', 'postgres', 'psql', 'postgresql'],
	install_requires = [
	],
	classifiers = [
		'Development Status :: 3 - Alpha',  # Either"3 - Alpha", "4 - Beta" or "5 - Production/Stable"
		'Intended Audience :: Developers',  # Define that your audience are developers
		'Topic :: Software Development :: Build Tools',
		'License :: OSI Approved :: MIT License',
		'Programming Language :: Python :: 3',
		'Programming Language :: Python :: 3.4',
		'Programming Language :: Python :: 3.5',
		'Programming Language :: Python :: 3.6',
		'Programming Language :: Python :: 3.7'
	],
)
