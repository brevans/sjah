#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import argparse
from . import Submit
from . import Status
from . import Batch

logging.basicConfig(level=logging.WARN)
prog = "sjah"


def main():
    # create the top-level parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug", action="store_true", help="Turn on verbose {} logging".format(prog)
    )
    subparsers = parser.add_subparsers(dest="subparser_name")

    parser_submit = subparsers.add_parser(
        "submit", aliases=["sub"], conflict_handler="resolve"
    )
    Submit.add_args(parser_submit)
    parser_status = subparsers.add_parser("status", aliases=["stat"])
    Status.add_args(parser_status)
    parser_batch = subparsers.add_parser("batch")
    Batch.add_args(parser_batch)

    # TODO
    # parser_jobfile = subparsers.add_parser('jobfile', aliases=['jf'])
    # JobFile.add_args(parser_jobfile)

    args = parser.parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    logging.debug("Got args %s", args)
    args.func(args)


if __name__ == "__main__":
    main()
