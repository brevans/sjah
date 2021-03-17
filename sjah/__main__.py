#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
from functools import partial
from importlib import import_module

import sjah.SjahCommand as SjahCommand


class SjahTopLevelCommand(SjahCommand.SjahCommand):
    def __init__(self):
        SjahCommand.SjahCommand.__init__(self)
        self.description = "Slurm Job Array Helper v {}".format(self.version)
        # these need to match a class provided in this directory
        # e.g. batch -> BatchCommand.BatchCommand
        self.sub_commands = ["batch", "status", "run"]

    def run_subcommand(self):
        subcommand_class_name = "{}Command".format(self.args.command.capitalize())
        subcommand_module = import_module("sjah.{}".format(subcommand_class_name))
        subcommand_class = getattr(subcommand_module, subcommand_class_name)
        self.subcommand = subcommand_class()
        self.logger.debug("running parser for %s", self.args.command)
        self.subcommand.run_parser(self.rest_of_args)

    def add_args(self):
        # top-level args
        self.parser.add_argument(
            "--log-level",
            help=argparse.SUPPRESS,
            choices=self.log_levels,
            type=partial(self.choice_alias, self.log_levels),
        )
        self.parser.add_argument(
            "command",
            help="Sub-command to run.",
            choices=self.sub_commands,
            type=partial(self.choice_alias, self.sub_commands),
        )
        self.parser.set_defaults(func=self.run_subcommand)

    def run_parser(self):
        # create the top-level parser
        self.parser = argparse.ArgumentParser(
            description=self.description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            add_help=False,
            prog=self.prog,
        )
        self.add_args()
        self.args, self.rest_of_args = self.parser.parse_known_args()
        if self.args.log_level is not None:
            self.logger.setLevel(getattr(logging, self.args.log_level))
            self.logger.debug("Setting logging to %s", self.args.log_level)
        self.logger.debug("Got args %s, %s", self.args, self.rest_of_args)
        self.args.func()


def main():
    sjah_comm = SjahTopLevelCommand()
    sjah_comm.run_parser()


if __name__ == "__main__":
    main()
