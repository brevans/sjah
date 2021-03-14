#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import subprocess


def get_status(args):
    pass


def add_args(parser):
    parser.add_argument("-j", "--jobid")
    parser.set_defaults(func=get_status)
