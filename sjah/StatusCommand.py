#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys
from collections import defaultdict

import sjah.SjahCommand as SjahCommand


class StatusCommand(SjahCommand.SjahCommand):
    def __init__(self):
        SjahCommand.SjahCommand.__init__(self)
        self.command_name = "status"
        self.sacct_input_cols = ["JobName", "JobID", "State"]
        self.all_job_states = {
            (
                "BF",
                "BOOT_FAIL",
            ): "Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the job can not be requeued).",
            (
                "CA",
                "CANCELLED",
            ): "Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.",
            (
                "CD",
                "COMPLETED",
            ): "Job has terminated all processes on all nodes with an exit code of zero.",
            ("DL", "DEADLINE"): "Job terminated on deadline.",
            (
                "F",
                "FAILED",
            ): "Job terminated with non-zero exit code or other failure condition.",
            (
                "NF",
                "NODE_FAIL",
            ): "Job terminated due to failure of one or more allocated nodes.",
            ("OOM", "OUT_OF_MEMORY"): "Job experienced out of memory error.",
            ("PD", "PENDING"): "Job is awaiting resource allocation.",
            ("PR", "PREEMPTED"): "Job terminated due to preemption.",
            ("R", "RUNNING"): "Job currently has an allocation.",
            ("RQ", "REQUEUED"): "Job was requeued.",
            ("RS", "RESIZING"): "Job is about to change size.",
            (
                "RV",
                "REVOKED",
            ): "Sibling was removed from cluster due to other cluster starting the job.",
            (
                "S",
                "SUSPENDED",
            ): "Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.",
            ("TO", "TIMEOUT"): "Job terminated upon reaching its time limit.",
        }
        self.job_states_filter = set()
        self.filtered_array_idxs = set()
        self.all_array_idx = set()
        self.job_name = None
        self.idxs_by_state = defaultdict(lambda: set())
        self.summary_output_cols = ["Job_State", "Count", "Indices"]

    def set_sacct_info(self):
        sacct_cmd = "sacct -o {} -nXPj {}".format(
            ",".join(self.sacct_input_cols), self.args.jobid
        )
        self.logger.debug("Running %s", sacct_cmd)
        for i, line in enumerate(self.check_output_lines(sacct_cmd)):
            split_line = line.split("|")
            if len(split_line) == len(self.sacct_input_cols):
                name, jobid_and_idx, state = split_line
                state = state.split()[0]
                jobid_and_idx = jobid_and_idx.split("_")

                if len(jobid_and_idx) == 2:
                    idxs = list(self.expand_ranges(jobid_and_idx[1]))
                    if self.job_name is None:
                        self.job_name = name
                    if state.split()[0] in self.job_states_filter:
                        self.filtered_array_idxs.update(idxs)
                    self.idxs_by_state[state].update(idxs)
                    self.all_array_idx.update(idxs)
                else:
                    self.logger.warn(
                        "Job %s doesn't look like an array.", self.args.jobid
                    )

        if i == 0:
            self.logger.info("No response from sacct for jobid %s", self.args.jobid)
            print(
                "sacct returned no results for JobID {}".format(self.args.jobid),
                file=sys.stderr,
            )
            sys.exit(1)

    def format_sacct_str(self):
        col_titles = ["Job_State", "Count"]
        output_column_lengths = dict(zip(col_titles, [len(x) for x in col_titles]))
        line_template_str = "{{:<{}}} {{:>{}}}"
        output_rows = []
        for state in self.idxs_by_state:
            count = len(self.idxs_by_state[state])
            idxs = ",".join(self.collapse_ranges(sorted(self.idxs_by_state[state])))
            output_rows.append(
                dict(zip(self.summary_output_cols, [state, count, idxs]))
            )
            self.logger.debug("Raw output line: %s", output_rows[-1])

            for col_name in output_column_lengths:
                col_len = len(str(output_rows[-1][col_name]))
                if col_len > output_column_lengths[col_name]:
                    output_column_lengths[col_name] = col_len

        line_template = line_template_str.format(
            *[output_column_lengths[col] for col in output_column_lengths]
        )
        if self.args.long:
            col_titles = self.summary_output_cols
            line_template = line_template + " {}"
        self.formatted_output_lines = [line_template.format(*col_titles)]
        self.formatted_output_lines.append(
            line_template.format(*["-" * len(col) for col in col_titles])
        )
        for line in output_rows:
            self.formatted_output_lines.append(
                line_template.format(*[line[col] for col in col_titles])
            )

    def print_array_info(self):
        print(
            "JobName: {}\nJobID:   {}[{}]\nArray Summary:".format(
                self.job_name,
                self.args.jobid,
                ",".join(self.collapse_ranges(sorted(self.all_array_idx))),
            ),
            file=sys.stderr,
        )
        for line in self.formatted_output_lines:
            print(line, file=sys.stderr)

    def set_state_filters(self):
        for _state in self.args.states[0].split(","):
            state_valid = False
            state = _state.upper()
            for valid_tup in self.all_job_states:
                if state in valid_tup:
                    state_valid = True
                    self.logger.debug("Adding state %s to filters", valid_tup[1])
                    self.job_states_filter.add(valid_tup[1])
            if not state_valid:
                self.logger.error("Got bad state %s as filter.", _state)
                self.parser.print_usage()
                print(
                    "Unrecognized State: {}. For a list of all valid states, run:\n{} {} --list-slurm-states".format(
                        _state,
                        self.prog,
                        self.command_name,
                    ),
                    file=sys.stderr,
                )

    def filter_jobs_file(self):
        line_count = sum(1 for i in self.args.job_file)
        if max(self.all_array_idx) != line_count:
            self.logger.warn(
                "Job file %s line count: %s, Max array idx: %s",
                line_count,
                self.args.job_file,
                max(self.all_array_idx),
            )
            print(
                "Warning! Max array index and number of lines in {} don't match. Are you sure this file is for job {}?".format(
                    self.args.job_file.name, self.args.jobid
                ),
                file=sys.stderr,
            )
        self.args.job_file.seek(0)
        filtered = 0
        for i, line in enumerate(self.args.job_file):
            if i in self.filtered_array_idxs:
                filtered += 1
                print(line.rstrip(), file=self.args.output)
        print("{} jobs matched filter.".format(filtered), file=sys.stderr)

    def print_info(self):
        if self.args.list_slurm_states:
            self.logger.debug("Printing job states as requested.")
            for short_state, long_state in self.all_job_states:
                print("\n{} or {}:".format(short_state, long_state))
                print(self.all_job_states[(short_state, long_state)])
                sys.exit(0)
        elif self.args.jobid is not None:
            self.set_state_filters()
            self.set_sacct_info()
            if not self.args.quiet:
                self.format_sacct_str()
                self.print_array_info()
            if self.args.job_file is not None:
                self.filter_jobs_file()
        else:
            self.parser.print_usage()
            print("Please specify a JobID.")

    def add_args(self):
        args_sjah_status = self.parser.add_argument_group("Arguments")
        args_sjah_status.add_argument(
            "jobid",
            metavar="JobID",
            type=int,
            nargs="?",
            help="The JobID for a job array (e.g. 1234 not 1234_5)",
        )
        args_sjah_status.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show this help message and exit.",
        )
        args_sjah_status.add_argument(
            "-f",
            "--job-file",
            metavar="jobs.txt",
            type=argparse.FileType("r"),
            help="Job file, one job per line (not your job submission script).",
        )
        args_sjah_status.add_argument(
            "-l",
            "--long",
            action="store_true",
            help="Print array indices in state summary.",
        )
        args_sjah_status.add_argument(
            "--list-slurm-states",
            action="store_true",
            help="Print a list of the job states {} {} knows about, then exit.".format(
                self.prog, self.command_name
            ),
        )
        args_sjah_status.add_argument(
            "-o",
            "--output",
            metavar="new-jobs.txt",
            type=argparse.FileType("w"),
            default=sys.stdout,
            help="Output file for filtered jobs. Default is to print to STDOUT.",
        )
        args_sjah_status.add_argument(
            "-q",
            "--quiet",
            action="store_true",
            help="Suppress printing state summary.",
        )
        args_sjah_status.add_argument(
            "-s",
            "--states",
            nargs=1,
            metavar="states",
            default=["CANCELLED,NODE_FAIL,PREEMPTED"],
            help="Comma separated list of states to filter job file with. FAILED and TIMEOUT are not defaults because we generally expect unchanged jobs to reproduce those results. Default: CANCELLED,NODE_FAIL,PREEMPTED",
        )

        self.parser.set_defaults(func=self.print_info)
