#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import platform
import subprocess
import sys
import grp
import pwd
from datetime import datetime
from itertools import groupby

from sjah.version import __version__


class ContextFilter(logging.Filter):
    """
    Logging filter to add host, user, and group context to logging records


    Attributes
    ----------
    record.username : string
        User that started this process.
    record.uid : string
        User ID that started this process.
    record.groupname : string
        Effective group name that started this process.
    record.gid : string
        Effective group ID that started this process.
    record.hostname : string


    Methods
    -------
    filter(record)
        Filter function logging runs to set attributes above, making them available in logging messages.

    Examples
    -------
    >>> import logging
    >>> logging.basicConfig(
    ...     level=logging.WARN,
    ...     format="%(asctime)s %(levelname)s %(hostname)s %(username)s(%(uid)s) %(groupname)s(%(gid)s): %(message)s",
    ... )
    >>> logger = logging.getLogger()
    >>> log_filter = ContextFilter()
    >>> logger.addFilter(self.log_filter)
    >>> logger.warn("The end is nigh!")
    2038-01-19 03:14:00,000 WARNING example.host.com expert(1001) support(1001): The end is nigh!
    """

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
    """
    Base command class for Slum Array Job Helper (sjah). Contains a few convenience functions for sub-classes and some argparse setup.


    Attributes
    ----------
    prog : str
        Base name from sys.argv[0]. Should usually be "sjah".
    prog_abs : str
        Absolute path to sys.argv[0].
    command_name : str
        Name this command should be called as from cli.
    today : str
        Todays date as YYYY-MM-DD
    logger : logging.logger
        Logger object to use for logging.
    version : str
        The __version__ from version.py
    description : str
        Description for this command
    slurm_version : str
        Slurm version, from scontrol show conf
    max_array_size : int
        Max array size/index, from scontrol show conf


    Methods
    -------
    yield_output_lines(command_str)
        Run command_str with subprocess.run, yielding decoded lines from both stdout and stderr.
    choice_alias(choices, choice)
        Return left-anchored unique match of choice in choices, otherwise return choice.
    mkdir(path)
        Create path if it doesn't exist. Equivalent to mkdir -p path.
    collapse_ranges(num_list)
        Convert and return the pre-sorted numbers in num_list to comma-separated ranges. e.g. [1,2,3,5] -> "1-3,5"
    expand_ranges(idx_range)
        Convert a bracketed range string to a list of integers. e.g. "[1-3,5]" -> [1,2,3,5]
    add_args()
        Implement in sub-classes. Add arguments to and setup self.parser. !! Must set default parser behavior with self.parser.set_defaults(func=some_func)
    run_parser()
        Creates a self.argparse.parser, runs add_args, parses args, then runs self.args.func(). Run this method in main.
    """

    def yield_output_lines(self, command_str):
        """
        Run command_str as a subprocess, yield the line-split results as decoded strings.

        Parameters
        ----------
        command_str : str
            String to run with subprocess.run

        Yields
        ------
        line : str
            Lines from STDOUT and STDERR that running command_str generates.

        """
        self.logger.info('Running cmd as subprocess: "%s"', command_str)
        completed = subprocess.run(
            command_str,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            universal_newlines=True,
        )
        self.logger.info(
            'Subprocess "%s" resulted in returncode %s',
            command_str,
            completed.returncode,
        )
        for line in completed.stdout.split("\n"):
            yield line

    def _set_slurm_info(self):
        cmd = "scontrol show conf"
        try:
            for line in self.yield_output_lines(cmd):
                if line.startswith("SLURM_VERSION"):
                    self.slurm_version = line.split()[-1]
                    self.logger.info("Found Slurm Version %s", self.slurm_version)
                elif line.startswith("MaxArraySize"):
                    self.max_array_size = int(line.split()[-1])
                    self.logger.info("MaxArraySize is %s", self.max_array_size)
        except FileNotFoundError as e:
            self.logger.error(
                "Doesn't look like Slurm is installed properly. Couldn't run %s : %s",
                cmd,
                e,
            )
            print("Couldn't find slurm on this system, exiting.", file=sys.stderr)
            sys.exit(1)

    def __init__(self):
        self.prog = os.path.basename(sys.argv[0])
        self.prog_abs = os.path.abspath(sys.argv[0])
        self.command_name = "base"
        self.today = datetime.now().strftime("%Y-%m-%d")
        logging.basicConfig(
            level=logging.WARN,
            format="%(asctime)s %(levelname)s %(hostname)s %(username)s(%(uid)s) %(groupname)s(%(gid)s) {0}.%(module)s.%(funcName)s: %(message)s".format(
                self.prog
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.logger = logging.getLogger(__name__)
        self._log_filter = ContextFilter()
        self.logger.addFilter(self._log_filter)
        self.version = __version__
        self.description = "Help description for this command."
        self._set_slurm_info()

    def _get_log_levels(self):

        return list(logging._nameToLevel.keys())

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

    def collapse_ranges(self, num_list):
        self.logger.debug("Collapsing range %s", num_list)
        for _, values in groupby(enumerate(num_list), lambda pair: pair[1] - pair[0]):
            values = list(values)
            if values[0][1] == values[-1][1]:
                yield "{}".format(values[0][1])
            else:
                yield "{}-{}".format(values[0][1], values[-1][1])

    def expand_ranges(self, idx_range):
        self.logger.debug("Expanding range string %s", idx_range)
        if not (idx_range.startswith("[") and idx_range.endswith("]")):
            yield int(idx_range)
        elif idx_range.startswith("[") and not idx_range.endswith("]"):
            self.logger.error("%s doesn't look like a valid array index", idx_range)
        else:
            start = idx_range.find("[") + 1
            end = (
                idx_range.find("]")
                if idx_range.find("%") == -1
                else idx_range.find("%")
            )
            for sub_idx in idx_range[start:end].split(","):
                if "-" not in sub_idx:
                    yield int(sub_idx)
                else:
                    low, high = sub_idx.split("-", 1)
                    for i in range(int(low), int(high) + 1):
                        yield int(i)

    def add_args(self):
        # implement in sub-classes
        # self.parser.set_defaults(func=some_func)
        raise NotImplementedError

    def run_parser(self, args_list):
        self.parser = argparse.ArgumentParser(
            conflict_handler="resolve",
            description=self.description,
            prog="{} {}".format(self.prog, self.command_name),
            # leave it up to subclasses to add help message
            add_help=False,
        )
        self.add_args()
        self.args = self.parser.parse_args(args_list)
        self.logger.debug("got args: %s", self.args)
        self.args.func()
