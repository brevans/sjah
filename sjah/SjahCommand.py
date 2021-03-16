#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import itertools
import logging
import os
import subprocess
import sys
from datetime import datetime

from sjah.version import __version__


class SjahCommand:
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

    def get_slurm_info(self):
        for line in self.check_output_lines("scontrol show conf"):
            if line.startswith("SLURM_VERSION"):
                self.slurm_version = line.split()[-1]
                self.logger.debug("Found Slurm Version %s", self.slurm_version)
            elif line.startswith("MaxArraySize"):
                self.max_array_size = int(line.split()[-1])
                self.logger.debug("MaxArraySize is %s", self.max_array_size)

    def __init__(self):
        self.prog = os.path.basename(sys.argv[0])
        self.today = datetime.now().strftime("%Y-%m-%d")
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
        self.get_slurm_info()

    def mkdir(self, path):
        if not os.path.exists(path):
            self.logger.info("Creating %s", path)
            os.makedirs(path)
        else:
            self.logger.info("Directory %s already exists", path)

    def _collapse_ranges(self, job_idxs):
        # takes a list of numbers, returns tuples of numbers that specify representative ranges
        # inclusive
        for i, t in itertools.groupby(enumerate(job_idxs), lambda tx: tx[1] - tx[0]):
            t = list(t)
            yield t[0][1], t[-1][1]

    # format job ranges
    def format_range(self, job_idxs):
        ranges = list(self._collapse_ranges(job_idxs))
        return ",".join(
            ["{}-{}".format(x[0], x[1]) if x[0] != x[1] else str(x[0]) for x in ranges]
        )

    def add_args(self):
        # implement in command
        # self.parser.set_defaults(func=default_function)
        pass

    def run_parser(self, args_list):
        self.parser = argparse.ArgumentParser(
            conflict_handler="resolve", description=self.description
        )
        self.add_args()
        self.args = self.parser.parse_args(args_list)
        self.args.func()
