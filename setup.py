from setuptools import setup, find_packages

setup(
    name='mylibrary',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'psycopg2-binary',
        'mysql-connector-python',
    ],
)