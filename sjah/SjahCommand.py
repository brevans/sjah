#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
import argparse
import logging
import sys
import os

from sjah.version import __version__


class SjahCommand:
    def __init__(self):
        self.prog = os.path.basename(sys.argv[0])
        logging.basicConfig(
            level=logging.WARN,
            format="%(asctime)s %(levelname)s {0}.%(module)s.%(funcName)s: %(message)s".format(
                self.prog
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.logger = logging.getLogger(__name__)
        self.version = __version__
        self.description = "Help description for this command."

    def check_output_lines(self, command_str):
        self.logger.info('Running cmd as subprocess: "%s"', command_str)
        completed = subprocess.run(
            command_str,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            universal_newlines=True,
            preexec_fn=lambda: os.setpgrp(),
        )
        for line in completed.stdout.split("\n"):
            yield line

    def mkdir(self, path):
        if not os.path.exists(path):
            self.logger.info("Creating %s", path)
            os.makedirs(path)
        else:
            self.logger.info("Directory %s already exists", path)

    def add_args(self):
        # implement in command
        # self.parser.set_defaults(func=default_function)
        pass

    def run_parser(self, args_str):
        self.parser = argparse.ArgumentParser(
            conflict_handler="resolve", description=self.description
        )
        self.add_args()
        self.args = self.parser.parse_args(args_str)
        self.args.func()
