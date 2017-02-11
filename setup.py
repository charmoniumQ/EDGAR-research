from setuptools import setup, find_packages

setup(
    name="EDGAR-research",
    version="0.1",
    packages=find_packages(),
    install_requires=['requests>=2.0', 'beautifulsoup4>=4.5'],
)
