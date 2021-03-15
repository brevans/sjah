#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import re
import subprocess
import sys

import sjah.SjahCommand as SjahCommand


class SubmitCommand(SjahCommand.SjahCommand):
    def __init__(self):
        SjahCommand.SjahCommand.__init__(self)
        self.help = "Submit a job array based on a text file of jobs, one per line."
        # regex to match short & long arguments from "sbatch --help", groups to optionally match whether or not they accept arguments
        self.sbatch_args_regex = r"^\W{1,10}(-[a-zA-Z])?,?\W{1,3}(--[a-zA-Z\-]+)(=\[?[a-zA-Z\-_\[\]\<\>|.:!@{}]*\]?)?(\[=[a-zA-Z ]+\])?"

    def get_sbatch_help_args(self):
        cmd = "sbatch --help"
        for line in self.check_output_lines(cmd):
            self.logger.debug("sbatch help line: %s", line)
            match = re.match(self.sbatch_args_regex, line)
            if match is not None:
                short_arg, long_arg, opt1, opt2 = match.groups()
                self.logger.debug(
                    "sbatch help line group match " + '"%s" ' * 4,
                    short_arg,
                    long_arg,
                    opt1,
                    opt2,
                )
                # some args are short or short and long
                arg_names = [x for x in (short_arg, long_arg) if x is not None]
                # don't flood sjah help by repeating sbatch help
                args_opts = {"help": argparse.SUPPRESS}
                if opt1 is not None:
                    # if specified, required argument
                    args_opts["nargs"] = 1
                    args_opts["metavar"] = opt1.lstrip("=")
                elif opt2 is not None:
                    # if specified, optional argument
                    args_opts["nargs"] = "?"
                    args_opts["metavar"] = opt2.lstrip("=")

                yield arg_names, args_opts

    def add_sbatch_args(self):
        """
        Add the arguments and options from get_sbatch_help_args to the parser passed in
        Return a set of args sbatch recognizes
        """
        self.sbatch_opts = set()
        for arg_names, args_opts in self.get_sbatch_help_args():
            self.logger.debug("Adding args %s with options %s", arg_names, args_opts)
            self.parser.add_argument(*arg_names, **args_opts)
            self.sbatch_opts.add(arg_names[-1])

    def add_sjah_submit_args(self):
        required_sjah_submit = self.parser.add_argument_group("Required Arguments")
        required_sjah_submit.add_argument(
            "job-file",
            metavar="jobs.txt",
            nargs=1,
            type=argparse.FileType("r"),
            help="Job file, one self-contained job per line.",
        )
        # optional arguments
        optional_sjah_submit = self.parser.add_argument_group("Optional Arguments")
        optional_sjah_submit.add_argument(
            "-h",
            "--help",
            action="help",
            default=argparse.SUPPRESS,
            help="Show this help message and exit.",
        )
        optional_sjah_submit.add_argument(
            "--batch-file",
            metavar="sub_script.sh",
            nargs=1,
            help="Name for batch script file. Defaults to sjah_submit-jobfile-YYYY-MM-DD.sh",
        )
        optional_sjah_submit.add_argument(
            "-J",
            "--job-name",
            metavar="jobname",
            nargs=1,
            help="Name of your job array. Defaults to sjah_submit-jobfile",
        )
        optional_sjah_submit.add_argument(
            "--max-jobs",
            metavar="number",
            nargs=1,
            help="Maximum number of simultaneously running jobs from the job array.",
        )
        optional_sjah_submit.add_argument(
            "-o",
            "--output",
            nargs=1,
            metavar="fmt_string",
            help="Slurm output file pattern. There will be one file per line in your job file. To suppress slurm out files, set this to /dev/null. Defaults to sjah_submit-jobfile-%%A_%%a-%%N.out",
        )
        optional_sjah_submit.add_argument(
            "--status-dir",
            metavar="dir",
            nargs=1,
            help="Directory to save the job_jobid_status.tsv file to. Defaults to working directory.",
        )
        optional_sjah_submit.add_argument(
            "--suppress-stats-file",
            action="store_true",
            help="Don't save job stats to job_jobid_status.tsv",
        )
        optional_sjah_submit.add_argument(
            "--stdout", action="store_true", help=argparse.SUPPRESS
        )
        optional_sjah_submit.add_argument(
            "--submit",
            action="store_true",
            help="Submit the job array on the fly instead of creating a submission script.",
        )

    def submit(self):
        for arg_name in vars(self.args):
            arg_val = getattr(self.args, arg_name)
            if arg_val is not None:
                self.logger.debug("got here")
                print(arg_name, arg_val)

    def add_args(self):
        self.add_sbatch_args()
        self.add_sjah_submit_args()
        self.parser.set_defaults(func=self.submit)
