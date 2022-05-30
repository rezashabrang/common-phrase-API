"""Suggested stop cli endpoint."""
from math import ceil
import multiprocessing as mp
from phrase_api.lib.db import arango_connection
from hashlib import sha256
import os
from phrase_api.logger import get_logger


LOGGER = get_logger("NER-Finder")


def tag_handler(
    max_records: int,
    chunk_size: int = 1000,
    n_jobs: int = mp.cpu_count()-2
):
    """Main handler for tagging suggested-highlight status

    Args:
        max_record: Maximum number of records present in aggregated collection.
        chunk_size: Number of record to fetch in each process.
        n_jobs: N processes to run on.
    """
    LOGGER.info("Starting NE Search process.")

    offset_times = ceil(max_records / chunk_size)
    offset_list = [offset * chunk_size for offset in range(offset_times)]

    LOGGER.info("A total number of %d jobs needs to be completed.", len(offset_list))

    pool = mp.Pool(n_jobs)

    LOGGER.info("Running on %d processes.", n_jobs)

    pool.starmap(
        tag_suggested_highlight,
        zip(
            (offset for offset in offset_list),
            (chunk_size for _ in range(len(offset_list)))
        ),
    )
    LOGGER.info("Suggested highlight tagging(NE Search) process finished.")


def tag_suggested_highlight(
    offset: int,
    chunk_size: int
):
    """Tagging records that are Named Entity."""
    agg_collection = os.getenv("AGG_PHRASE_COL")
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    try:
        client = arango_connection()
        phrase_db = client.db(database, username=username, password=password)

        doc_fetch_query = """
        for doc in @@agg_collection
            limit @offset, @chunk_size
            filter doc.status == null
            return doc
        """
        bind_parameters = {
            "@agg_collection": agg_collection,
            "offset": offset,
            "chunk_size": chunk_size
        }

        # Fetching Records
        records = phrase_db.aql.execute(
            doc_fetch_query,
            bind_vars=bind_parameters
        )
        records = list(records)

        # For every record if it is NE update status if not do nothing
        update_status_query = """
            UPDATE {_key: @phrase_hash} with {status: "suggested-highlight"}\
                 IN @@agg_collection
        """
        for record in records:
            if check_ner(record["bag"], phrase_db):
                bind_pars = {
                    "@agg_collection": agg_collection,
                    "phrase_hash": record["_key"]
                }
                phrase_db.aql.execute(
                    update_status_query,
                    bind_vars=bind_pars
                )

    except Exception as err:
        LOGGER.error("Failed Processing for offset %d", offset, exc_info=err)
        return

    LOGGER.info("Finished processing offset %d", offset)


def check_ner(
    phrase: str,
    phrase_db
):
    """Breaking phrase and checking each word for if it is NE or not.

    Args:
        phrase: Phrase fetched from aggregated collection. Can be more than 1 word.
        phrase_db: Arango client.

    Returns:
        True if all words of phrase are NE else False.
    """
    ner_collection = os.getenv("NER_COLLECTION")
    words = [sha256(word.encode()).hexdigest() for word in phrase.split(" ")]
    ner_query = """
        for doc in @@ner_collection
            filter doc._key == @word_hash
            return doc
    """
    for word in words:
        bind_pars = {
            "@ner_collection": ner_collection,
            "word_hash": word
        }
        res = phrase_db.aql.execute(
            ner_query,
            bind_vars=bind_pars
        )
        res = list(res)
        if not res:
            return False

    return True
