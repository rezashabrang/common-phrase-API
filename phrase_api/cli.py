#!/usr/bin/env python
"""Module for updating thesaurus."""
import argparse

import sys

from phrase_api.lib.cli_helper import ingest_site
from phrase_api.scripts.progress_viewer import view_progress
from phrase_api.scripts.NER_extractor import ner_handler
from phrase_api.scripts.chunk_aggregate import aggregation_handler
from phrase_api.scripts.NE_search import tag_handler


def cli_wrapper(args):
    """CLI configurations

    Args:
        args: Command Line input for parsing.
    """
    # CLI Confs
    common_phrase_api_parser = argparse.ArgumentParser()
    subparsers = common_phrase_api_parser.add_subparsers(dest="command")

    # ------------------------- Progress Viewer -------------------------
    prog_handler = subparsers.add_parser(
        "view-progress", help="View progress for ingestion"
    )

    # ------------------------- NER Search Handler -------------------------
    ner_search_handler_parser = subparsers.add_parser(
        "search-NE", help="Search for NE and tag them with suggested highlight."
    )

    # Max Records
    ner_search_handler_parser.add_argument(
        "--max_records", action="store",
        help="Total Number of records in aggregated collection",
        required=True, type=int
    )

    # Chunk Size
    ner_search_handler_parser.add_argument(
        "--chunk_size", action="store", help="N records to fetch in each job",
        required=False, default=1000, type=int
    )

    # NER Search Jobs
    ner_search_handler_parser.add_argument(
        "--n_jobs", action="store", help="Number of processes to run on.",
        required=False, default=1
    )

    # ------------------------- NER Process Handler -------------------------
    ner_handler_parser = subparsers.add_parser(
        "process-NER", help="Process a file or directory containing NER files."
    )
    # NER path
    ner_handler_parser.add_argument(
        "--ner_path", action="store", help="Path to the NER files", required=True
    )

    # NER Jobs
    ner_handler_parser.add_argument(
        "--n_jobs", action="store", help="Number of processes to run on.",
        required=False
    )

    # ------------------------- Chunk Aggregator -------------------------
    agg_handler = subparsers.add_parser(
        "chunk-agg", help="Aggregating phrases in database."
    )

    # sitename
    agg_handler.add_argument(
        "--sitename", action="store", help="Name of the ingested site", required=True
    )

    # max ID
    agg_handler.add_argument(
        "--maxID", action="store",
        help="Maximum ID of the ingested site", required=False
    )

    # ID List
    agg_handler.add_argument(
        "--IDList",
        nargs="+",
        action="store", help="List of IDs", required=False
    )

    # ----------------------- doc-process Enpoint Handler -----------------------
    ingest_parser = subparsers.add_parser(
        "ingest", help="CLI wrapper for doc-process API"
    )

    # Host
    ingest_parser.add_argument(
        "--host", action="store", help="Host address of the databse", required=True
    )

    # Port
    ingest_parser.add_argument(
        "--port", action="store", help="Port of the databse", default=3306
    )

    # Sitename
    ingest_parser.add_argument("-s", "--sitename", action="store", help="The sitename")

    # Username
    ingest_parser.add_argument(
        "-u", "--username", action="store", help="Username for access", required=True
    )

    # Password
    ingest_parser.add_argument(
        "-p", "--password", action="store", help="Password for access", required=True
    )

    # Database
    ingest_parser.add_argument(
        "-d", "--db", action="store", help="Name of the database", required=True
    )

    # Tag-stop
    ingest_parser.add_argument(
        "--tag-stop",
        action="store_true",
        help="Whether to use suggested-stop status. Default is False.",
    )
    # Tag-Highlight
    ingest_parser.add_argument(
        "--tag-highlight",
        action="store_true",
        help="Whether to use suggested-highlight status. Default is False.",
    )

    # Replace-stop
    ingest_parser.add_argument(
        "--replace-stop",
        action="store_true",
        help="Whether to replace stop words. Default is False.",
    )

    # Min ID
    ingest_parser.add_argument(
        "--min_id",
        action="store",
        help="Optional arg for starting article id.",
        type=int,
    )

    # Max ID
    ingest_parser.add_argument(
        "--max_id",
        action="store",
        help="Optional arg for maximum article id.",
        type=int,
    )
    # Ngram range
    ingest_parser.add_argument(
        "--ngram_range",
        action="store",
        help="Range for ngrams",
        type=str,
    )

    # n-jobs
    ingest_parser.add_argument(
        "--n-jobs",
        action="store",
        help="Number of processes to use.",
        type=int,
    )
    # ------------------------- Processing Args ------------------------
    args = vars(common_phrase_api_parser.parse_args(args))

    return args


def run_commands(args: dict):
    """Running command based on parsed cli arguments.

    Args:
        args: Parsed CLI arguments.
    """
    if args["command"] == "ingest":
        ingest_site(args)
    elif args["command"] == "view-progress":
        view_progress()
    elif args["command"] == "process-NER":
        ner_handler(args["ner_path"])
    elif args["command"] == "chunk-agg":
        aggregation_handler(args)
    elif args["command"] == "search-NE":
        tag_handler(
            max_records=args["max_records"],
            chunk_size=args["chunk_size"],
            n_jobs=args["n_jobs"]
        )


args = cli_wrapper(sys.argv[1:])
run_commands(args)
