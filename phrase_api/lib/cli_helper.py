"""Helper functions for CLI."""
from locale import currency
from pydoc import cli
import sqlite3

from phrase_api.logger import get_logger
from sqlalchemy import create_engine
import requests
import os

logger = get_logger(__name__)


def initialize_sqlite() -> None:
    """Creating database and related table."""
    conn = sqlite3.connect('tracker.db')

    logger.info("Created sqlite database.")

    conn.execute('''CREATE TABLE tracker
         (ID INT PRIMARY KEY AUTOINCREMENT     NOT NULL,
         sitename           CHAR(100)    NOT NULL,
         host            CHAR(100)     NOT NULL,
         article_id         INT);
         ''')

    logger.info("Created tracker table.")

    conn.close()


def update_tracker(
    sitename: str,
    host: str,
    article_id: int
):
    """Updating tracker for current database."""
    conn = sqlite3.connect('tracker.db')

    cur = conn.cursor()

    select_query = "SELECT * FROM tracker where host = :host AND sitename = :sitename"

    check_res = cur.execute(select_query, {"host": host, "sitename": sitename})

    # If there is not any record beforehand then insert new one.
    if not list(check_res):
        insert_q = "INSERT INTO tracker (sitename,host,article_id) \
            VALUES (:sitename, :host, :article_id )"
        cur.execute(
            insert_q, {"sitename": sitename, "host": host, "article_id": article_id}
        )

    # If there is already a record then update the article_id
    else:
        update_q = "UPDATE tracker SET article_id = :article_id WHERE\
            host = :host AND sitename = :sitename"

        cur.execute(
            update_q, {"sitename": sitename, "host": host, "article_id": article_id}
        )

    conn.close()
    return


def ingest_site(cli_args):
    """Ingesting site articles."""
    db_engine = create_engine(
        "mysql://{username}:{password}@{host}:{port}/{db}?charset={ch}".format(
            username=cli_args["username"],
            password=cli_args["password"],
            host=cli_args["host"],
            port=cli_args["port"],
            db=cli_args["db"],
            ch="utf8",
        )
    )
    conn = db_engine.connect()

    # TODO Selecting sorted article ids
    sorted_article_ids = []
    for id in sorted_article_ids:
        # TODO fetching article text
        query = ""
        article = ""

        # --------------- Creating request to doc-process endpoint ---------------
        payload = {
            "document": article
        }
        headers = {
            "x-token": os.getenv("API_KEY")
        }
        # TODO maybe setting rootpath in env file?
        request_url = f"http://127.0.0.1:8000/api/doc-process/?doc_type=TEXT&\
replace_stop={cli_args['replace-stop']}&tag_stop={cli_args['tag-stop']}"

        req = requests.post(
            request_url,
            json=payload,
            headers=headers
        )

        if req.status_code == 201:
            update_tracker(
                sitename=cli_args["sitename"],
                host=cli_args["host"],
                article_id=id
            )
        else:
            logger.error("Failed ingesting %s - article: %d", cli_args['sitename'], id)
