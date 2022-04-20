#!/usr/bin/env python
"""Module for updating thesaurus."""
import argparse

from phrase_api.lib.cli_helper import ingest_site, initialize_sqlite

if __name__ == "__main__":
    # CLI Confs
    common_phrase_api_parser = argparse.ArgumentParser()
    subparsers = common_phrase_api_parser.add_subparsers()

    # ----------------------- SQLITE Handler -----------------------
    sqlite_handler = subparsers.add_parser("ini-sqlite", help="Create sqlite db")
    sqlite_handler.add_argument(
        "--init", action="store_true", required=True, help="Create sqlite database"
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
    if "init" in args:
        initialize_sqlite()
    else:
        ingest_site(args)
