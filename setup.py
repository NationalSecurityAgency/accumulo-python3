from setuptools import find_packages, setup

setup(
    name='accumulo',
    version='0.3.0',
    packages=find_packages(exclude=['tests']),
    url='https://github.com/NationalSecurityAgency/accumulo-python3',
    author='National Security Agency',
    author_email='/dev/null',
    description='Build Python 3 applications that integrate with Apache Accumulo',
    install_requires=[
        'thrift>=0.13.0'
    ],
    extras_require={
        'replication': [
            'pika>=1.1.0',
            'protobuf==3.11.2'
        ]
    },
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        'Operating System :: OS Independent'
    ]
)
