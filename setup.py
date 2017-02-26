from setuptools import setup, find_packages

# this file is ignored

setup(
    name="EDGAR-research",
    version="0.1",
    packages=find_packages(),
    install_requires=['beautifulsoup4>=4.5', 'six>=1.0', 'pyhaikunator'],
)
