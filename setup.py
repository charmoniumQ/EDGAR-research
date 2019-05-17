import sys
import os
from pathlib import Path
from setuptools import setup, find_packages


packages = find_packages()
print(packages)


setup(
    name='edgar_code',
    version='0.1',
    packages=packages,
)
