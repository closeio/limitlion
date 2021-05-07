"""LimitLion setup."""

from setuptools import setup

install_requires = ['redis>=2']

tests_require = install_requires + ['pytest', 'pytest-cov']

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='limitlion',
    version='1.0.0',
    url='http://github.com/closeio/limitlion',
    description='Close LimitLion',
    long_description=long_description,
    long_description_content_type="text/markdown",
    platforms='any',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=[
        'limitlion',
    ],
    package_data={'limitlion': ['*.lua']},
    install_requires=install_requires,
    tests_require=tests_require,
)
