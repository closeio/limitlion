"""LimitLion setup."""

from setuptools import setup

install_requires = [
    'redis==2.10.6'
]

tests_require = install_requires + [
    'pytest', 'pytest-cov'
]

setup(
    name='limitlion',
    version='0.9.1',
    url='http://github.com/closeio/limitlion',
    description='Close LimitLion',
    platforms='any',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
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
