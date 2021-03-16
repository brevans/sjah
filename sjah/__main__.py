#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
from importlib import import_module

import sjah.SjahCommand as SjahCommand


class SjahTopLevelCommand(SjahCommand.SjahCommand):
    def __init__(self):
        SjahCommand.SjahCommand.__init__(self)
        self.description = "Slurm Job Array Helper v {}".format(self.version)
        # these need to match a class provided in this dir
        # e.g. submit -> SubmitCommand.SubmitCommand
        self.sub_commands = ["batch", "status", "run"]
        self.logger = logging.getLogger(self.prog)

    def choice_alias(self, choice):
        """
        Function to allow for unique left-anchored substrings of the commands
        Returns the right command if it's unique, otherwise passes the choice through
        """
        options = [c for c in self.sub_commands if c.startswith(choice)]
        if len(options) == 1:
            return options[0]
        return choice

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
            "--debug",
            action="store_true",
            help=argparse.SUPPRESS,
        )
        self.parser.add_argument(
            "command",
            help="Sub-command to run.",
            choices=self.sub_commands,
            type=self.choice_alias,
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
        if self.args.debug:
            self.logger.setLevel(logging.DEBUG)
        self.logger.debug("Got args %s, %s", self.args, self.rest_of_args)
        self.args.func()


def main():
    sjah_comm = SjahTopLevelCommand()
    sjah_comm.run_parser()


if __name__ == "__main__":
    main()
