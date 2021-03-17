#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import re
import subprocess
import sys
from datetime import datetime

import sjah.SjahCommand as SjahCommand


class RunCommand(SjahCommand.SjahCommand):
    def __init__(self):
        SjahCommand.SjahCommand.__init__(self)
        self.command_name = "run"
        self.job_str = None
        self.array_jid = int(os.environ.get("SLURM_ARRAY_JOB_ID"))
        self.array_tid = int(os.environ.get("SLURM_ARRAY_TASK_ID"))
        self.time_fmt = "%Y-%m-%d %H:%M:%S"
        self.out_cols = [
            "Array_Task_ID",
            "Exit_Code",
            "Hostname",
            "Time_Start",
            "Time_End",
            "Time_Elapsed",
            "Job_String",
        ]

    def set_job_cmd(self):
        for idx, line in enumerate(self.args.job_file[0]):
            if idx == self.array_tid:
                self.job_str = line.strip()
                break
        self.args.job_file[0].close()

    def exec_job(self):
        if self.job_str is None:
            self.logger.error(
                "Could not find zero-indexed line %s in job file %s",
                self.array_tid,
                self.job_file,
            )
            sys.exit(1)
        process = subprocess.Popen(self.job_str, shell=True)
        self.return_code = process.wait()

    def save_stats(self):
        if self.args.stats_file is not None:
            with open(
                re.sub("%A", self.array_jid, self.args.stats_file), "w"
            ) as stats_file_fh:
                self.logger.info("Writing job stats to %s", stats_file_fh.name)
                time_start_str = self.start_time.strftime(self.time_fmt)
                time_end_str = self.end_time.strftime(self.time_fmt)
                time_elapsed_str = str(
                    (self.end_time - self.start_time).total_seconds()
                )
                print(
                    "\t".join(
                        [
                            self.array_tid,
                            self.return_code,
                            self.hostname,
                            time_start_str,
                            time_end_str,
                            time_elapsed_str,
                            self.job_str,
                        ]
                    ),
                    file=stats_file_fh,
                )

    def run_job(self):
        self.set_job_cmd()
        # run job and track its execution time
        self.start_time = datetime.now()
        ret = self.exec_job()
        self.end_time = datetime.now()
        self.save_stats()
        sys.exit(ret)

    def add_args(self):
        required_sjah_run = self.parser.add_argument_group("Required Arguments")
        required_sjah_run.add_argument(
            "job_file",
            metavar="jobs.txt",
            nargs=1,
            type=argparse.FileType("r"),
            help="Job file, one self-contained job per line.",
        )
        optional_sjah_run = self.parser.add_argument_group("Optional Arguments")
        optional_sjah_run.add_argument(
            "-h",
            "--help",
            action="help",
            default=argparse.SUPPRESS,
            help="Show this help message and exit.",
        )
        optional_sjah_run.add_argument(
            "--stats-file",
            metavar="stats.tsv",
            nargs=1,
            help="Save tab-separated job stats and original job string to specified file. Add %%A to the filename to add the array jobid to the output file.",
        )
        self.parser.set_defaults(func=self.run_job)
