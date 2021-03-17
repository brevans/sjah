#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import itertools
import logging
import os
import platform
import subprocess
import sys
import grp
import pwd
from datetime import datetime

from sjah.version import __version__


class ContextFilter(logging.Filter):
    def filter(self, record):
        userinfo = pwd.getpwuid(os.getuid())
        groupinfo = grp.getgrgid(os.getgid())
        record.username = userinfo[0]
        record.uid = userinfo[2]
        record.groupname = groupinfo[0]
        record.gid = groupinfo[2]
        record.hostname = platform.node()
        return True


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
        )
        for line in completed.stdout.split("\n"):
            yield line

    def set_slurm_info(self):
        for line in self.check_output_lines("scontrol show conf"):
            if line.startswith("SLURM_VERSION"):
                self.slurm_version = line.split()[-1]
                self.logger.info("Found Slurm Version %s", self.slurm_version)
            elif line.startswith("MaxArraySize"):
                self.max_array_size = int(line.split()[-1])
                self.logger.info("MaxArraySize is %s", self.max_array_size)

    def __init__(self):
        self.prog = os.path.basename(sys.argv[0])
        self.prog_abs = os.path.abspath(sys.argv[0])
        self.command_name = "base"
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        logging.basicConfig(
            level=logging.WARN,
            format="%(asctime)s %(levelname)s %(hostname)s %(username)s(%(uid)s) %(groupname)s(%(gid)s) {0}.%(module)s.%(funcName)s: %(message)s".format(
                self.prog
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.logger = logging.getLogger(__name__)
        self.log_filter = ContextFilter()
        self.logger.addFilter(self.log_filter)
        self.hostname = platform.node()
        self.version = __version__
        self.description = "Help description for this command."
        self.set_slurm_info()

    def choice_alias(self, choices, choice):
        """
        Function to allow for unique left-anchored substrings matching
        Returns the unique choice if it's unique, otherwise passes the choice through
        """
        options = [c for c in choices if c.startswith(choice)]
        if len(options) == 1:
            self.logger.debug('"%s" matched %s uniquely', choice, options[0])
            return options[0]
        return choice

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
        # implement in Command
        # self.parser.set_defaults(func=default_function)
        pass

    def run_parser(self, args_list):
        self.parser = argparse.ArgumentParser(
            conflict_handler="resolve",
            description=self.description,
            prog="{} {}".format(self.prog, self.command_name),
        )
        self.add_args()
        self.args = self.parser.parse_args(args_list)
        self.args.func()
