"""Helper functions for CLI."""
from cmath import log
import sqlite3
from phrase_api.logger import get_logger
from sqlalchemy import create_engine, select, Column, Integer, Text, BLOB
import requests
import os
from sqlalchemy.orm import declarative_base
import multiprocessing as mp


Base = declarative_base()

logger = get_logger(__name__)


class News(Base):
    """News metadata"""
    __tablename__ = "newsstudio_contents"
    id = Column(Integer)
    newsstudio_id = Column(Integer, primary_key=True)
    content = Column(Text)
    newsstudio_content_data = Column(BLOB)


def initialize_sqlite() -> None:
    """Creating database and related table."""
    conn = sqlite3.connect('tracker.db')

    logger.info("Created sqlite database.")

    conn.execute('''CREATE TABLE tracker
         (ID INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL,
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

    result = cur.execute(select_query, {"host": host, "sitename": sitename}).fetchone()

    check_res = None if not result else list(result)

    # If there is not any record beforehand then insert new one.
    if not check_res:
        insert_q = "INSERT INTO tracker (sitename,host,article_id) \
            VALUES (:sitename, :host, :article_id )"
        cur.execute(
            insert_q, {"sitename": sitename, "host": host, "article_id": article_id}
        )

    # If there is already a record then update the article_id
    else:
        if article_id > check_res[0]:
            update_q = "UPDATE tracker SET article_id = :article_id WHERE\
                host = :host AND sitename = :sitename"

            cur.execute(
                update_q, {"sitename": sitename, "host": host, "article_id": article_id}
            )

    conn.commit()
    conn.close()
    return


def check_tracker(
    sitename: str,
    host: str,
):
    """Getting last news id from tracker."""
    conn = sqlite3.connect('tracker.db')

    cur = conn.cursor()
    # conn.execute("DELETE FROM tracker")
    # conn.commit()
    select_query = "SELECT article_id FROM tracker where host = :host AND sitename = :sitename"

    result = cur.execute(select_query, {"sitename": sitename, "host": host}).fetchone()

    last_news_id = None if not result else list(result)
    conn.close()

    # If there is not any record return 0
    if not last_news_id:
        return 0
    return last_news_id[0]


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

    logger.info("Connected to database.")

    max_id_query = "SELECT MAX(newsstudio_id) FROM newsstudio_contents"
    max_id = (conn.execute(max_id_query).fetchone())[0]

    logger.info("Checking tracker.")

    last_news_id = check_tracker(
        sitename=cli_args["sitename"],
        host=cli_args["host"],
    )

    if last_news_id == 0:
        logger.info("First time ingesting %s.", cli_args["sitename"])
    else:
        logger.info(
            "Starting from news id %d for site %s", last_news_id, cli_args["sitename"]
        )

    logger.info("Starting fetching news.")

    # for news_id in range(last_news_id, max_id + 1):
    num_threads = mp.cpu_count()

    pool = mp.Pool(num_threads)
    pool.starmap(
        ingest_news,
        zip((news_id for news_id in range(last_news_id, max_id + 1)),
            (cli_args for _ in range(last_news_id, max_id + 1)),
            (max_id for _ in range(last_news_id, max_id + 1))
            ))


def ingest_news(
    news_id, cli_args, max_id
):
    """Ingesting each news"""
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
    # Fetching article text
    query = select(News.content).where(News.newsstudio_id == news_id)
    news_content = conn.execute(query).fetchone()

    # If there is no result continue
    if news_content is None:
        logger.info("No news for id %d", news_id)
        return

    news_content = news_content[0]
    conn.close()
    # --------------- Creating request to doc-process endpoint ---------------
    payload = {
        "document": news_content
    }
    headers = {
        "x-token": os.getenv("API_KEY")
    }
    # TODO maybe setting rootpath in env file?
    request_url = f"http://127.0.0.1:80/api/doc-process/?doc_type=TEXT&\
replace_stop={cli_args['replace_stop']}&tag_stop={cli_args['tag_stop']}\
&doc_id={news_id}&sitename={cli_args['sitename']}"

    req = requests.post(
        request_url,
        json=payload,
        headers=headers
    )

    if req.status_code == 201:
        update_tracker(
            sitename=cli_args["sitename"],
            host=cli_args["host"],
            article_id=news_id
        )
    else:
        logger.error(
            "Failed ingesting %s - news_id: %d", cli_args['sitename'], news_id
        )
        return

    logger.info(
        "Finished processing news %d / %d of site %s.",
        news_id,
        max_id,
        cli_args['sitename']
    )
