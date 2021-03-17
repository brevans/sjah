#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pathlib

from setuptools import setup

import sjah

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")
with open("requirements-dev.txt") as f:
    dev_requirements = f.read().splitlines()

setup(
    name="sjah",
    version=sjah.__version__,
    description="Slurm Job Array Helper",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ycrc/sjah",
    author="Yale Center for Research Computing",
    author_email="hpc@yale.edu",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: System :: Distributed Computing",
        "Topic :: Scientific/Engineering",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords="slurm, research, cluster, computing, hpc, job, arrays, cluster computing",
    packages=["sjah"],
    python_requires=">=3.6, <4",
    # install_requires=[''],
    extras_require={
        "develop": dev_requirements,
        #     'test': ['coverage'],
    },
    entry_points={  # Optional
        "console_scripts": [
            "sjah=sjah.__main__:main",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/ycrc/sjah/issues",
        "Source": "https://github.com/ycrc/sjah",
    },
    test_suite="tests",
)
