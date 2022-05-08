#!/usr/bin/env python
"""Module for updating thesaurus."""
import argparse

from phrase_api.lib.cli_helper import ingest_site, initialize_sqlite
from phrase_api.scripts.progress_viewer import view_progress
from phrase_api.scripts.NER_extractor import ner_handler
from phrase_api.scripts.chunk_aggregate import aggregation_handler

if __name__ == "__main__":
    # CLI Confs
    common_phrase_api_parser = argparse.ArgumentParser()
    subparsers = common_phrase_api_parser.add_subparsers(dest="command")

    # ----------------------- SQLITE Handler -----------------------
    sqlite_handler = subparsers.add_parser("ini-sqlite", help="Create sqlite db")

    # ------------------------- Progress Viewer -------------------------
    prog_handler = subparsers.add_parser(
        "view-progress", help="View progress for ingestion"
    )

    # ------------------------- NER Process Handler -------------------------
    ner_handler_parser = subparsers.add_parser(
        "process-NER", help="Process a file or directory containing NER files."
    )
    # NER path
    ner_handler_parser.add_argument(
        "--ner_path", action="store", help="Path to the NER files", required=True
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

    # Replace-stop
    ingest_parser.add_argument(
        "--replace-stop",
        action="store_true",
        help="Whether to replace stop words. Default is False.",
    )

    # ID
    ingest_parser.add_argument(
        "-i",
        "--id",
        action="store",
        help="Optional arg for starting article id.",
        type=int,
    )
    # ------------------------- Processing Args ------------------------
    args = vars(common_phrase_api_parser.parse_args())
    if args["command"] == "ini-sqlite":
        initialize_sqlite()
    elif args["command"] == "ingest":
        ingest_site(args)
    elif args["command"] == "view-progress":
        view_progress()
    elif args["command"] == "process-NER":
        ner_handler(args["ner_path"])
    elif args["command"] == "chunk-agg":
        aggregation_handler(args)
