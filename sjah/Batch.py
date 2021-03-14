#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import re
import subprocess


def run_job(args):
    pass


def add_args(parser):
    parser.add_argument("-j", "--jobsfile")
    parser.set_defaults(func=run_job)
