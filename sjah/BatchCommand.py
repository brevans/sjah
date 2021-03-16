#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys
import os
import re

import sjah.SjahCommand as SjahCommand


class BatchCommand(SjahCommand.SjahCommand):
    def __init__(self):
        SjahCommand.SjahCommand.__init__(self)
        self.job_id_list = []
        self.num_jobs = 0
        self.run_args = []
        self.help = "Create a batch submission script for a job array based on a text file of jobs, one per line."
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
                # some args are long or short and long
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
                else:
                    # arg is a boolean
                    args_opts["action"] = "store_true"

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

    def add_sjah_batch_args(self):
        self.sjah_batch_opts = set()
        required_sjah_batch = self.parser.add_argument_group("Required Arguments")
        required_sjah_batch.add_argument(
            "job_file",
            metavar="jobs.txt",
            nargs=1,
            type=argparse.FileType("r"),
            help="Job file, one self-contained job per line.",
        )
        # optional arguments
        optional_sjah_batch = self.parser.add_argument_group("Optional Arguments")
        optional_sjah_batch.add_argument(
            "-h",
            "--help",
            action="help",
            default=argparse.SUPPRESS,
            help="Show this help message and exit.",
        )
        self.sjah_batch_opts.add("--help")
        optional_sjah_batch.add_argument(
            "--batch-file",
            metavar="sub_script.sh",
            nargs=1,
            help="Name for batch script file. Defaults to sjah-jobfile-YYYY-MM-DD.sh",
        )
        self.sjah_batch_opts.add("--batch-file")
        optional_sjah_batch.add_argument(
            "-J",
            "--job-name",
            metavar="jobname",
            nargs=1,
            help="Name of your job array. Defaults to sjah-jobfile",
        )
        self.sjah_batch_opts.add("--job-name")
        optional_sjah_batch.add_argument(
            "--step-array-jobs",
            metavar="number",
            type=int,
            nargs=1,
            help="Only run every n jobs from the job array.",
        )
        self.sjah_batch_opts.add("--step-array-jobs")
        optional_sjah_batch.add_argument(
            "--max-array-jobs",
            metavar="number",
            type=int,
            nargs=1,
            help="Maximum number of simultaneously running jobs from the job array.",
        )
        self.sjah_batch_opts.add("--max-array-jobs")

        optional_sjah_batch.add_argument(
            "-o",
            "--output",
            nargs=1,
            metavar="fmt_string",
            help="Slurm output file pattern. There will be one file per line in your job file. To suppress slurm out files, set this to /dev/null. Defaults to sjah_logs/sjah-jobfile-%%A_%%a-%%N.out",
        )
        self.sjah_batch_opts.add("--output")
        optional_sjah_batch.add_argument(
            "--stdout", action="store_true", help=argparse.SUPPRESS
        )
        self.sjah_batch_opts.add("--stdout")
        optional_sjah_batch.add_argument(
            "--submit",
            action="store_true",
            help="Submit the job array on the fly instead of creating a submission script.",
        )
        self.sjah_batch_opts.add("--submit")
        # silently allow overriding --array, otherwise we calculate that
        optional_sjah_batch.add_argument(
            "-a", "--array", nargs=1, help=argparse.SUPPRESS
        )
        self.sjah_batch_opts.add("--array")

    def check_job_file(self):
        for i, line in enumerate(self.args.job_file[0]):
            if not (line.startswith("#") or line.rstrip() == ""):
                self.job_id_list.append(i)
                self.num_jobs += 1
        self.max_array_idx = self.job_id_list[-1]
        self.array_range = self.format_range(self.job_id_list)

    def make_sbatch_args_out(self):
        self.sbatch_args_out = []

        # set job name
        self.jobfile_no_ext = os.path.splitext(
            os.path.basename(self.args.job_file[0].name)
        )[0]
        if self.args.job_name is not None:
            jobname = self.args.job_name
        else:
            jobname = "sjah-{}".format(self.jobfile_no_ext)
        self.sbatch_args_out.append("--job-name '{0}'".format(jobname))

        # set job output
        if self.args.output is None:
            output = "sjah_logs/sjah-{}-%A_%a-%N.out".format(self.jobfile_no_ext)
        else:
            output = self.args.output
        self.sbatch_args_out.append("--output '{0}'".format(output))
        output_dir = os.path.dirname(output)
        if output_dir != "":
            self.mkdir(output_dir)

        # set job array spec
        if self.args.array is not None:
            arrayspec = self.args.array
        else:
            arrayspec = self.array_range
            if self.args.step_array_jobs is not None:
                arrayspec = "{}:{}".format(arrayspec, self.args.step_array_jobs)
            if self.args.max_array_jobs is not None:
                arrayspec = "{}%{}".format(arrayspec, self.args.max_array_jobs)

        self.sbatch_args_out.append("--array '{0}'".format(arrayspec))

        just_sbatch = self.sbatch_opts.difference(self.sjah_batch_opts)
        for arg_name in vars(self.args):
            arg_val = getattr(self.args, arg_name)
            arg = "--" + arg_name
            if arg in just_sbatch and arg_val is not None and arg_val is not False:
                if arg_val is True:
                    self.sbatch_args_out.append(arg)
                else:
                    self.sbatch_args_out.append(
                        "--{0} '{1}'".format(arg_name, arg_val[0])
                    )

    def make_run_call(self):
        self.run_call = "sjah run {0} {1}".format(
            self.args.job_file[0].name, self.run_args
        )

    def submit_cli(self):
        pass

    def write_batch_file(self):
        if self.args.stdout:
            out_fh = sys.stdout
        elif self.args.batch_file is not None:
            out_fh = open(self.args.batch_file[0], "w")
        else:
            out_fh = open("sjah-{}-{}.sh".format(self.jobfile_no_ext, self.today), "w")
        print("#!/bin/bash", file=out_fh)
        for sbatch_arg in self.sbatch_args_out:
            print("#SBATCH {}".format(sbatch_arg), file=out_fh)
        print("\n# DO NOT EDIT LINE BELOW", file=out_fh)
        print(self.run_call, file=out_fh)
        out_fh.close()
        print(
            "Batch script generated. To submit your job array, run:\n sbatch {}".format(
                out_fh.name
            )
        )

    def batch(self):
        self.check_job_file()
        self.make_sbatch_args_out()
        self.make_run_call()
        if self.args.submit:
            self.submit_cli()
        else:
            self.write_batch_file()

    def add_args(self):
        self.add_sbatch_args()
        self.add_sjah_batch_args()
        self.parser.set_defaults(func=self.batch)
