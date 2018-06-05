"""LimitLion setup."""

from setuptools import setup

install_requires = [
    'redis'
]

tests_require = install_requires + [
    'pytest',
]

setup(
    name='limitlion',
    version='0.9',
    url='http://github.com/closeio/limitlion',
    description='Close.io LimitLion',
    platforms='any',
    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    packages=[
        'limitlion',
    ],
    package_data={'limitlion': ['*.lua']},
    install_requires=install_requires,
    tests_require=tests_require,
)
