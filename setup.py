import os
from setuptools import setup, find_packages

VERSION = '0.1'


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name='baycat',
    version=VERSION,
    description='A minimalist, efficiency-oriented S3 uploader',
    long_description=get_long_description(),
    long_description_content_type="text/markdown",

    license='AGPL',
    url='http://joshisanerd.com/projects/baycat/',

    author='Josh Myer',
    author_email='josh@joshisanerd.com',


    packages=find_packages(),
    entry_points='''
    [console_scripts]
    baycat=baycat.cli:cli
    ''',

    install_requires=[
        'boto3',
        'click',
    ],
    extras_require={
        "test": [
            "moto",
            "coverage",
            "pycodestyle",
        ],
    },
    python_requres=">=3.7",
)
