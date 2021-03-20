#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import re
import subprocess
import sys

import sjah.SjahCommand as SjahCommand


class BatchCommand(SjahCommand.SjahCommand):
    def __init__(self):
        SjahCommand.SjahCommand.__init__(self)
        self.command_name = "batch"
        self.job_id_list = []
        self.num_jobs = 0
        self.run_args = []
        self.description = "Create a batch submission script for a job array based on a text file of jobs, one per line."
        # regex to match short & long arguments from "sbatch --help", groups to optionally match whether or not they accept arguments
        self.sbatch_args_regex = r"^\W{1,10}(-[a-zA-Z])?,?\W{1,3}(--[a-zA-Z\-]+)(=\[?[a-zA-Z\-_\[\]\<\>|.:!@{}]*\]?)?(\[=[a-zA-Z ]+\])?"

    def get_sbatch_help_args(self):
        cmd = "sbatch --help"
        for line in self.yield_output_lines(cmd):
            match = re.match(self.sbatch_args_regex, line)
            if match is not None:
                short_arg, long_arg, opt1, opt2 = match.groups()
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
            # help="Only run every n jobs from the job array (the :number to sbatch --array).",
            help=argparse.SUPPRESS,
        )
        self.sjah_batch_opts.add("--step-array-jobs")
        optional_sjah_batch.add_argument(
            "--max-array-jobs",
            metavar="number",
            type=int,
            nargs=1,
            help="Maximum number of simultaneously running jobs from the job array (the %%number to sbatch --array).",
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
            "--submit",
            action="store_true",
            help="Submit the job script after creating it.",
        )
        self.sjah_batch_opts.add("--submit")
        optional_sjah_batch.add_argument(
            "--stats-file",
            metavar="stats.tsv",
            nargs=1,
            help="Save tab-separated job stats and original job string to specified file. Add %%A to the filename to add the array jobid to the output file.",
        )
        self.sjah_batch_opts.add("--stats-file")
        optional_sjah_batch.add_argument(
            "--stats-file",
            metavar="stats.tsv",
            nargs=1,
            help="Save tab-separated job stats and original job string to specified file. Add %%A to the filename to add the array jobid to the output file.",
        )
        self.sjah_batch_opts.add("--stats-file")
        # silently allow overriding --array, otherwise we calculate that
        optional_sjah_batch.add_argument(
            "-a", "--array", nargs=1, help=argparse.SUPPRESS
        )
        self.sjah_batch_opts.add("--array")
        # catch sbatch --parsable so we can submit job properly
        optional_sjah_batch.add_argument(
            "--parsable", action="store_true", help=argparse.SUPPRESS
        )
        optional_sjah_batch.add_argument(
            "--stdout", action="store_true", help=argparse.SUPPRESS
        )
        self.sjah_batch_opts.add("--stdout")

    def check_job_file(self):
        for i, line in enumerate(self.args.job_file[0]):
            if not (line.startswith("#") or line.rstrip() == ""):
                self.job_id_list.append(i)
                self.num_jobs += 1
        if self.num_jobs == 0:
            self.logger.error(
                "Couldn't find any jobs in file %s", self.args.job_file[0].name
            )
            print(
                "Couldn't find any jobs in file {}".format(self.args.job_file[0].name),
                file=sys.stderr,
            )
            sys.exit(1)
        self.max_array_idx = self.job_id_list[-1]
        self.logger.info(
            "Found %s jobs in file %s with %s total lines.",
            self.num_jobs,
            self.args.job_file[0].name,
            i + 1,
        )
        self.array_range_str = ",".join(self.collapse_ranges(self.job_id_list))

    def set_sbatch_args_out(self):
        self.sbatch_args_out = []

        # set job name
        self.jobfile_no_ext = os.path.splitext(
            os.path.basename(self.args.job_file[0].name)
        )[0]
        if self.args.job_name is not None:
            jobname = self.args.job_name
        else:
            jobname = "sjah-{}".format(self.jobfile_no_ext)
        self.sbatch_args_out.append("--job-name='{0}'".format(jobname))

        # set job output
        if self.args.output is None:
            output = "sjah_logs/sjah-{}-%A_%a-%N.out".format(self.jobfile_no_ext)
        else:
            output = self.args.output
        self.sbatch_args_out.append("--output='{0}'".format(output))
        output_dir = os.path.dirname(output)
        if output_dir != "":
            self.mkdir(output_dir)

        # set job array spec
        if self.args.array is not None:
            arrayspec = self.args.array
        else:
            arrayspec = self.array_range_str
            if self.args.step_array_jobs is not None:
                arrayspec = "{}:{}".format(arrayspec, self.args.step_array_jobs[0])
            if self.args.max_array_jobs is not None:
                arrayspec = "{}%{}".format(arrayspec, self.args.max_array_jobs[0])

        self.sbatch_args_out.append("--array='{0}'".format(arrayspec))

        just_sbatch = self.sbatch_opts.difference(self.sjah_batch_opts)
        for argparse_name in vars(self.args):
            arg_val = getattr(self.args, argparse_name)
            # switch back to '-' separated arg words
            arg = "--" + argparse_name.replace("_", "-")
            if arg in just_sbatch and arg_val is not None and arg_val is not False:
                if arg_val is True:
                    self.logger.debug("Setting sbatch boolean argument %s ", arg)
                    self.sbatch_args_out.append(arg)
                else:
                    long_arg = "{0}='{1}'".format(arg, arg_val[0])
                    self.sbatch_args_out.append(long_arg)
                    self.logger.debug("Setting sbatch argument %s ", long_arg)

    def make_run_call(self):
        if self.args.stats_file is not None:
            self.run_args.append('--stats-file="{}"'.format(self.args.stats_file[0]))
        str_log_level = logging.getLevelName(self.logger.getEffectiveLevel())
        self.run_call = "{0} {1} --log-level={2} run {3} {4}".format(
            sys.executable,
            self.prog_abs,
            str_log_level,
            self.args.job_file[0].name,
            " ".join(self.run_args),
        )

    def batch_submit(self):
        if self.args.submit:
            if self.args.stdout:
                self.logger.info("Tried to run --submit --stdout.")
                print(
                    "Can't run sbatch without a file. Try again without specifying --stdout.",
                    file=sys.stderr,
                )
                sys.exit(1)
            self.logger.debug("Submitting script: %s", self.out_file_name)
            if self.args.parsable:
                sbatch_com = "sbatch --parsable"
            else:
                sbatch_com = "sbatch"
            completed = subprocess.run(
                "{} {}".format(sbatch_com, self.out_file_name),
                shell=True,
            )
            self.logger.debug("Job submitted with exit code %s", completed.returncode)
            sys.exit(completed.returncode)
        else:
            if not self.args.stdout:
                print(
                    "Batch script generated. To submit your job array, run:\n sbatch {}".format(
                        self.out_file_name
                    )
                )

    def write_batch_file(self):

        if not self.args.stdout:
            self.out_file_name = "sjah-{}-{}.sh".format(self.jobfile_no_ext, self.today)
            if self.args.batch_file is not None:
                # user choice
                self.logger.debug(
                    "Printing job file to user choice: %s", self.args.batch_file[0]
                )
                self.out_file_name = self.args.batch_file[0]
            self.out_file_handle = open(self.out_file_name, "w")
        elif self.args.stdout:
            self.out_file_handle = sys.stdout
        print("#!/bin/bash", file=self.out_file_handle)
        for sbatch_arg in self.sbatch_args_out:
            print("#SBATCH {}".format(sbatch_arg), file=self.out_file_handle)
        print(
            "\n# DO NOT EDIT BELOW HERE, instead run {} {} again".format(
                self.prog, self.command_name
            ),
            file=self.out_file_handle,
        )
        print(self.run_call, file=self.out_file_handle)
        self.out_file_handle.close()
        self.logger.info("Finished writing batch file %s", self.out_file_handle.name)

    def batch(self):
        self.check_job_file()
        self.set_sbatch_args_out()
        self.make_run_call()
        self.write_batch_file()
        self.batch_submit()

    def add_args(self):
        self.add_sbatch_args()
        self.add_sjah_batch_args()
        self.parser.set_defaults(func=self.batch)
