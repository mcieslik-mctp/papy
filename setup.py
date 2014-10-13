#!/usr/bin/env python2
# -*- coding: utf-8 -*-
NAME = 'papy'
VERSION = '1.0.5'

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Operating System :: OS Independent",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Topic :: Scientific/Engineering",
    "Programming Language :: Python :: 2.7",
    "License :: OSI Approved :: BSD License"
    ]

PACKAGES = ['papy', 'numap']
PACKAGE_DIR = {
    'papy': 'src/papy',
    'numap': 'src/numap'
}

setup(
    name=NAME.lower(),
    version=VERSION,
    description='flow-based, parallel and distributed computational pipelines, includes a lazy, parallel, remote, multi-task map function',
    keywords='multiprocessing, parallel, distributed, pool, imap, workflow, pipeline, flow-based',
    author='Marcin Cieslik',
    author_email='mcieslik@med.umich.edu',
    url='http://http://mcieslik-mctp.github.io/papy/',
    license='BSD',
    long_description=open('README.rst', 'r').read(),
    classifiers=CLASSIFIERS,
    packages=PACKAGES,
    package_dir=PACKAGE_DIR,
    #package_data=PACKAGE_DATA,
    #install_requires=REQUIRES,
    # Options
    include_package_data=True,
    zip_safe=False,
    )

