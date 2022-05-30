"""Helper functions for CLI."""
import multiprocessing as mp
import os
from pydoc import cli

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
    if not cli_args["max_id"]:
        max_id_query = "SELECT MAX(newsstudio_id) FROM newsstudio_contents"
        max_id = (conn.execute(max_id_query).fetchone())[0]
    else:
        max_id = cli_args["max_id"]

    min_id = 0 if not cli_args["min_id"] else cli_args["min_id"]

    conn.close()
    db_engine.dispose()

    logger.info("Starting fetching news.")

    num_threads = cli_args["n-jobs"] if "n-jobs" in cli_args else max(
        mp.cpu_count() - 2, 1)

    # Subgrouping news
    i = list(range(min_id, max_id + 101, 100))
    j = [i - 1 for i in i]
    news_groups = list(zip(i, j[1:]))

    # Ngram Range
    ngram_range = cli_args["ngram_range"]

    pool = mp.Pool(num_threads)
    pool.starmap(
        ingest_news,
        zip(
            (news_ids for news_ids in news_groups),
            (cli_args for _ in range(len(news_groups))),
            (ngram_range for _ in range(len(news_groups))),
        ),
    )


def ingest_news(news_ids, cli_args, ngram_range):
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
&tag_highlight={cli_args['tag_highlight']}&ngram_range={ngram_range}\
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
