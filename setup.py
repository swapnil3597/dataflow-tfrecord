"""
    Setup for apache beam pipeline.
"""
import setuptools


NAME = 'tfrecord_util'
VERSION = '1.0'
REQUIRED_PACKAGES = [
    'apache-beam[gcp]',
    'tensorflow',
    'gcsfs',
    'workflow'
    ]

setuptools.setup(
    name=NAME,
    version=VERSION,
    packages=setuptools.find_packages(),
    install_requires=REQUIRED_PACKAGES
)
