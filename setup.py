import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='sqlite-s3-query',
    version='0.0.73',
    author='Michal Charemza',
    author_email='michal@charemza.name',
    description='Python context manager to query a SQLite file stored on S3',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/michalc/sqlite-s3-query',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    python_requires='>=3.6.0',
    install_requires=[
        'httpx>=0.18.2',
    ],
    py_modules=[
        'sqlite_s3_query',
    ],
)
