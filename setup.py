from setuptools import setup, find_packages

setup(
    name             = 'curia',
    version          = '0.0.0.1',
    packages         = find_packages(),
    license          = 'MIT',
    url              = 'https://github.com/multiparty/curia',
    description      = 'Utilities for handling data from remote object stores.',
    long_description = open('README.md').read()
)