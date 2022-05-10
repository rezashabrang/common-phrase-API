import os

from phrase_api.lib.db import arango_connection
import multiprocessing as mp
from phrase_api.logger import get_logger
from arango.exceptions import AQLQueryExecuteError
from time import sleep

LOGGER = get_logger("Chunk-AGG")


def aggregation_handler(
    cli_args,
):
    """Aggregating phrases in database.

    Args:
        cli_args: Arguments through CLI wrapper.
    """
    if cli_args["IDList"] is None and cli_args["maxID"] is None:
        raise Exception("No max id or id list is given.")
    elif cli_args["IDList"] is None:
        max_records = int(cli_args["maxID"])
        doc_id_list = (doc_id for doc_id in range(int(cli_args["maxID"]) + 1))
    elif cli_args["maxID"] is None:
        max_records = len(cli_args["IDList"])
        doc_id_list = (doc_id for doc_id in cli_args["IDList"])

    sitename = cli_args["sitename"]
    num_threads = max(mp.cpu_count() - 4, 2)
    LOGGER.info(
        "Starting chunk aggregator process on %d threads(processes).", num_threads
    )

    pool = mp.Pool(num_threads)
    pool.starmap(
        chunk_aggregate,
        zip(
            (sitename for _ in range(max_records + 1)),
            doc_id_list
        ),
    )
    LOGGER.info("Chunk aggregator process finished.")


def chunk_aggregate(
    sitename: str,
    doc_id: int
):
    """Aggregating phrases in chunks.

    Chunk here referes to (sitename, doc_id) pairs that contains phrase data.

    Args:
        sitename: name of the site
        doc_id: ID of the document

    """
    phrase_collection = os.getenv("ARANGO_VERTEX_COLLECTION")
    username = os.getenv("AGG_PHRASE_USER")
    password = os.getenv("AGG_PHRASE_PASS")
    database = os.getenv("AGG_PHARSE_DB")
    try:
        client = arango_connection()
        phrase_db = client.db(database, username=username, password=password)

        doc_fetch_query = """
        for doc in @@phrase_collection
            filter doc.sitename == @sitename AND doc.doc_id == @doc_id
            return doc
        """
        bind_parameters = {
            "@phrase_collection": phrase_collection,
            "sitename": sitename,
            "doc_id": str(doc_id)
        }
        records = phrase_db.aql.execute(
            doc_fetch_query,
            bind_vars=bind_parameters
        )
        records = list(records)
    except Exception as err:
        LOGGER.error(
            "Failed fetching records for doc ID %s, sitename %s.",
            doc_id,
            sitename,
            exc_info=err
        )
        return

    if not records:
        LOGGER.info("No record for doc ID %s, sitename %s", doc_id, sitename)
        return
    for record in records:
        try:
            if "agg_status" in record:
                if record["agg_status"] == 1:
                    continue

            aggregate_record(record, phrase_db)
        except Exception as err:
            LOGGER.error(
                "Failed aggregating record with _key %s in doc ID %s, sitename %s.",
                record["_key"],
                doc_id,
                sitename,
                exc_info=err
            )

    LOGGER.info(
        "Finished aggregating records in (sitename = %s, docID = %s)", sitename, doc_id
    )

    client.close()


def aggregate_record(record: dict, phrase_client):
    """Processing aggregation for a single record.

    Args:
        record: Dictionary record in database.
    """
    phrase_key = record["_key"]
    phrase_hash = record["phrase_hash"]
    phrase_bag = record["bag"]
    phrase_count = record["count"]
    phrase_status = record["status"]
    phrase_length = record["length"]
    agg_collection = os.getenv("AGG_PHRASE_COL")
    phrase_collection = os.getenv("ARANGO_VERTEX_COLLECTION")

    upsert_query = """
    UPSERT {"_key": @phrase_hash}
        INSERT {"_key": @phrase_hash, "bag": @phrase_bag,"count": @phrase_count,\
        "status": @phrase_status, "length": @phrase_length}
        UPDATE {"count": OLD.count + @phrase_count} 
    IN @@agg_collection
    UPDATE { _key: @phrase_key } with {"agg_status": 1} in @@phrase_collection
    """
    bind_parameters = {
        "@phrase_collection": phrase_collection,
        "@agg_collection": agg_collection,
        "phrase_hash": phrase_hash,
        "phrase_bag": phrase_bag,
        "phrase_count": phrase_count,
        "phrase_status": phrase_status,
        "phrase_length": phrase_length,
        "phrase_key": phrase_key
    }
    try_counter = 1
    while True:
        try:
            phrase_client.aql.execute(
                upsert_query,
                bind_vars=bind_parameters
            )
            break
        except AQLQueryExecuteError:
            LOGGER.warning(
                "AQL exception for _key %s. Retrying (%d).",
                phrase_key,
                try_counter
            )
            try_counter += 1
            sleep(0.1)

