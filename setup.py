from setuptools import find_packages, setup

setup(
    name='accumulo',
    version='2.1.1',
    packages=find_packages(),
    url='https://github.com/NationalSecurityAgency/accumulo-python3',
    author='National Security Agency',
    author_email='/dev/null',
    description='Build Python 3 applications that integrate with Apache Accumulo',
    install_requires=[
        'thrift>=0.16.0' # Update to thrift 0.17.0, pending https://issues.apache.org/jira/browse/THRIFT-5688
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
