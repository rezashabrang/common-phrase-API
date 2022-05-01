"""Helper functions for CLI."""
import multiprocessing as mp
import os
import sqlite3

import requests
from sqlalchemy import BLOB, Column, Integer, Text, create_engine, select, update, and_
from sqlalchemy.exc import OperationalError, TimeoutError
from sqlalchemy.orm import declarative_base

from phrase_api.logger import get_logger

Base = declarative_base()

logger = get_logger(__name__)


class News(Base):
    """News metadata"""

    __tablename__ = "newsstudio_contents"
    id = Column(Integer)
    newsstudio_id = Column(Integer, primary_key=True)
    content = Column(Text)
    newsstudio_content_data = Column(BLOB)
    proc_status = Column(Integer)


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

    conn.close()
    db_engine.dispose()

    logger.info("Starting fetching news.")

    num_threads = mp.cpu_count()

    # Subgrouping news
    i = list(range(0, max_id + 101, 100))
    j = [i - 1 for i in i]
    news_groups = list(zip(i, j[1:]))

    pool = mp.Pool(num_threads)
    pool.starmap(
        ingest_news,
        zip(
            (news_ids for news_ids in news_groups),
            (cli_args for _ in range(len(news_groups))),
            (max_id for _ in range(len(news_groups))),
        ),
    )


def ingest_news(news_ids, cli_args, max_id):
    """Ingesting each news"""
    try:
        # database engine
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
        query = select([News.content, News.newsstudio_id]).where(
            and_(
                News.newsstudio_id.between(news_ids[0], news_ids[1]),
                News.proc_status != 1
            )
        )
        news_content = conn.execute(query).fetchall()

        # If there is no result continue
        if news_content is None:
            logger.info("No news for ids between %d and %d", news_ids[0], news_ids[1])
            return

        news_content = list(news_content)

        for content in news_content:
            news = content[0]
            news_id = content[1]

            # --------------- Creating request to doc-process endpoint ---------------
            payload = {"document": news}
            headers = {"x-token": os.getenv("API_KEY")}
            request_url = f"http://127.0.0.1:80/api/doc-process/?doc_type=TEXT&\
replace_stop={cli_args['replace_stop']}&tag_stop={cli_args['tag_stop']}\
&doc_id={news_id}&sitename={cli_args['sitename']}"

            req = requests.post(request_url, json=payload, headers=headers)

            # --------------- Updating process status in news DB ---------------
            if req.status_code == 201:

                update_query = (update(News).where(
                    News.newsstudio_id == news_id).values(proc_status=1))

                conn.execute(update_query)
                logger.info(
                    "Finished processing news %d of site %s.",
                    news_id,
                    cli_args["sitename"]
                )

            else:
                logger.error(
                    "Failed ingesting %s - news_id: %d",
                    cli_args["sitename"],
                    news_id,
                )
                update_query = (update(News).where(
                    News.newsstudio_id == news_id).values(proc_status=2))

                conn.execute(update_query)

    except (OperationalError, TimeoutError) as err:
        logger.error("Failed connecting to news database.", exc_info=err)
        conn.close()
        db_engine.dispose()
        return

    except Exception as err:
        logger.error(
            "ERROR ingesting news batch: %d - %d. Unexpected exception.",
            news_ids[0],
            news_ids[1],
            exc_info=err
        )
        conn.close()
        db_engine.dispose()
        return

    conn.close()
    db_engine.dispose()


# ------------------------ TRACKER ------------------------------
def initialize_sqlite() -> None:
    """Creating database and related table."""
    conn = sqlite3.connect("tracker.db")

    logger.info("Created sqlite database.")

    conn.execute(
        """CREATE TABLE tracker
         (ID INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL,
         sitename           CHAR(100)    NOT NULL,
         host            CHAR(100)     NOT NULL,
         article_id         INT);
         """
    )

    logger.info("Created tracker table.")

    conn.close()


def update_tracker(sitename: str, host: str, article_id: int) -> None:
    """Updating tracker for current database."""
    conn = sqlite3.connect("tracker.db")

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
) -> int:
    """Getting last news id from tracker."""
    conn = sqlite3.connect("tracker.db")

    cur = conn.cursor()
    select_query = "SELECT article_id FROM tracker where host = :host AND sitename = \
:sitename"

    result = cur.execute(select_query, {"sitename": sitename, "host": host}).fetchone()

    last_news_id = None if not result else list(result)
    conn.close()

    # If there is not any record return 0
    if not last_news_id:
        return 0
    return int(last_news_id[0])
